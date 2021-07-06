/*************************************************************************************************
 * Python binding of Tkrzw
 *
 * Copyright 2020 Google LLC
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *     https://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.  See the License for the specific language governing permissions
 * and limitations under the License.
 *************************************************************************************************/

#include <string>
#include <string_view>
#include <map>
#include <memory>
#include <vector>

#include <cstddef>
#include <cstdint>

#include "tkrzw_cmd_util.h"
#include "tkrzw_dbm.h"
#include "tkrzw_dbm_common_impl.h"
#include "tkrzw_dbm_mmap.h"
#include "tkrzw_dbm_poly.h"
#include "tkrzw_dbm_shard.h"
#include "tkrzw_file.h"
#include "tkrzw_file_poly.h"
#include "tkrzw_file_util.h"
#include "tkrzw_key_comparators.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"

extern "C" {

#undef _POSIX_C_SOURCE
#undef _XOPEN_SOURCE
#include "Python.h"
#include "structmember.h"

// Global variables.
PyObject* mod_tkrzw;
PyObject* cls_utility;
PyObject* cls_status;
PyObject* cls_expt;
PyObject* cls_dbm;
PyObject* cls_iter;
PyObject* cls_file;
PyObject* obj_proc_noop;
PyObject* obj_proc_remove;

// Python object of Utility.
struct PyUtility {
  PyObject_HEAD
};

// Python object of Status.
struct PyTkStatus {
  PyObject_HEAD
  tkrzw::Status* status;
};

// Python ofject of StatusException.
struct PyException {
  PyException_HEAD
  PyObject* pystatus;
};

// Python object of DBM.
struct PyDBM {
  PyObject_HEAD
  tkrzw::ParamDBM* dbm;
  bool concurrent;
};

// Python object of Iterator.
struct PyIterator {
  PyObject_HEAD
  tkrzw::DBM::Iterator* iter;
  bool concurrent;
};

// Python object of File.
struct PyFile {
  PyObject_HEAD
  tkrzw::PolyFile* file;
  bool concurrent;
};

// Creates a new string of Python.
static PyObject* CreatePyString(std::string_view str) {
  return PyUnicode_DecodeUTF8(str.data(), str.size(), "ignore");
}

// Creatse a new byte array of Python.
static PyObject* CreatePyBytes(std::string_view str) {
  return PyBytes_FromStringAndSize(str.data(), str.size());
}

// Creates a status object of Python.
static PyObject* CreatePyTkStatus(const tkrzw::Status& status) {
  PyTypeObject* pytype = (PyTypeObject*)cls_status;
  PyTkStatus* obj = (PyTkStatus*)pytype->tp_alloc(pytype, 0);
  if (!obj) return nullptr;
  obj->status = new tkrzw::Status(status);
  return (PyObject*)obj;
}

// Throws an invalid argument error.
static void ThrowInvalidArguments(std::string_view message) {
  PyErr_SetString(PyExc_TypeError, tkrzw::StrCat("invalid arguments: ", message).c_str());
}

// Throws a status error.
static void ThrowStatusException(const tkrzw::Status& status) {
  PyObject* pystatus = CreatePyTkStatus(status);
  PyErr_SetObject(cls_expt, pystatus);
  Py_DECREF(pystatus);
}

// Locking device to call a nagive function.
class NativeLock final {
 public:
  NativeLock(bool concurrent) : thstate_(nullptr) {
    if (concurrent) {
      thstate_ = PyEval_SaveThread();
    }
  }

  ~NativeLock() {
    if (thstate_) {
      PyEval_RestoreThread(thstate_);
    }
  }

 private:
  PyThreadState* thstate_;
};

// Wrapper to treat a Python string as a C++ string_view.
class SoftString final {
 public:
  explicit SoftString(PyObject* pyobj) :
    pyobj_(pyobj), pystr_(nullptr), pybytes_(nullptr), ptr_(nullptr), size_(0) {
    Py_INCREF(pyobj_);
    if (PyUnicode_Check(pyobj_)) {
      pybytes_ = PyUnicode_AsUTF8String(pyobj_);
      if (pybytes_) {
        ptr_ = PyBytes_AS_STRING(pybytes_);
        size_ = PyBytes_GET_SIZE(pybytes_);
      } else {
        PyErr_Clear();
        ptr_ = "";
        size_ = 0;
      }
    } else if (PyBytes_Check(pyobj_)) {
      ptr_ = PyBytes_AS_STRING(pyobj_);
      size_ = PyBytes_GET_SIZE(pyobj_);
    } else if (PyByteArray_Check(pyobj_)) {
      ptr_ = PyByteArray_AS_STRING(pyobj_);
      size_ = PyByteArray_GET_SIZE(pyobj_);
    } else if (pyobj_ == Py_None) {
      ptr_ = "";
      size_ = 0;
    } else {
      pystr_ = PyObject_Str(pyobj_);
      if (pystr_) {
        pybytes_ = PyUnicode_AsUTF8String(pystr_);
        if (pybytes_) {
          ptr_ = PyBytes_AS_STRING(pybytes_);
          size_ = PyBytes_GET_SIZE(pybytes_);
        } else {
          PyErr_Clear();
          ptr_ = "";
          size_ = 0;
        }
      } else {
        ptr_ = "(unknown)";
        size_ = std::strlen(ptr_);
      }
    }
  }

  ~SoftString() {
    if (pybytes_) Py_DECREF(pybytes_);
    if (pystr_) Py_DECREF(pystr_);
    Py_DECREF(pyobj_);
  }
  
  std::string_view Get() const {
    return std::string_view(ptr_, size_);
  }

 private:
  PyObject* pyobj_;
  PyObject* pystr_;
  PyObject* pybytes_;
  const char* ptr_;
  size_t size_;
};

// Converts a numeric parameter to an integer.
static int64_t PyObjToInt(PyObject* pyobj) {
  if (PyLong_Check(pyobj)) {
    return PyLong_AsLong(pyobj);
  } else if (PyFloat_Check(pyobj)) {
    return (int64_t)PyFloat_AsDouble(pyobj);
  } else if (PyUnicode_Check(pyobj) || PyBytes_Check(pyobj)) {
    SoftString str(pyobj);
    return tkrzw::StrToInt(str.Get());
  } else if (pyobj != Py_None) {
    int64_t num = 0;
    PyObject* pylong = PyNumber_Long(pyobj);
    if (pylong) {
      num = PyLong_AsLong(pyobj);
      Py_DECREF(pylong);
    }
    return num;
  }
  return 0;
}

// Converts a Python Unicode object into a UCS-4 vector.
std::vector<uint32_t> PyUnicodeToUCS4(PyObject* pyuni) {
  const int32_t kind = PyUnicode_KIND(pyuni);
  void* data = PyUnicode_DATA(pyuni);
  const int32_t len = PyUnicode_GET_LENGTH(pyuni);
  std::vector<uint32_t> ucs;
  ucs.reserve(len);
  for (int32_t i = 0; i < len; i++) {
    ucs.emplace_back(PyUnicode_READ(kind, data, i));
  }
  return ucs;
}

// Sets a constant of long integer.
static bool SetConstLong(PyObject* pyobj, const char* name, int64_t value) {
  PyObject* pyname = PyUnicode_FromString(name);
  PyObject* pyvalue = PyLong_FromLongLong(value);
  return PyObject_GenericSetAttr(pyobj, pyname, pyvalue) == 0;
}

// Sets a constant of unsigned long integer.
static bool SetConstUnsignedLong(PyObject* pyobj, const char* name, uint64_t value) {
  PyObject* pyname = PyUnicode_FromString(name);
  PyObject* pyvalue = PyLong_FromUnsignedLongLong(value);
  return PyObject_GenericSetAttr(pyobj, pyname, pyvalue) == 0;
}

// Sets a constant of string.
static bool SetConstStr(PyObject* pyobj, const char* name, const char* value) {
  PyObject* pyname = PyUnicode_FromString(name);
  PyObject* pyvalue = PyUnicode_FromString(value);
  return PyObject_GenericSetAttr(pyobj, pyname, pyvalue) == 0;
}

// Maps keyword arguments into C++ map.
static std::map<std::string, std::string> MapKeywords(PyObject* pykwds) {
  std::map<std::string, std::string> map;
  PyObject* pykwlist = PyMapping_Items(pykwds);
  const int32_t kwnum = PyList_GET_SIZE(pykwlist);
  for (int32_t i = 0; i < kwnum; i++) {
    PyObject* pyrec = PyList_GET_ITEM(pykwlist, i);
    if (PyTuple_GET_SIZE(pyrec) == 2) {
      PyObject* pykey = PyTuple_GET_ITEM(pyrec, 0);
      PyObject* pyvalue = PyTuple_GET_ITEM(pyrec, 1);
      SoftString key(pykey);
      SoftString value(pyvalue);
      map.emplace(std::string(key.Get()), std::string(value.Get()));
    }
  }
  Py_DECREF(pykwlist);
  return map;
}

// Extracts a list of pairs of string views from a sequence object.
static std::vector<std::pair<std::string_view, std::string_view>> ExtractSVPairs(
    PyObject* pyseq, std::vector<std::string>* placeholder) {
  std::vector<std::pair<std::string_view, std::string_view>> result;
  const size_t size = PySequence_Size(pyseq);
  result.reserve(size);
  placeholder->reserve(size * 2);
  for (size_t i = 0; i < size; i++) {
    PyObject* pypair = PySequence_GetItem(pyseq, i);
    if (PySequence_Check(pypair) && PySequence_Size(pypair) >= 2) {
      PyObject* pykey = PySequence_GetItem(pypair, 0);
      PyObject* pyvalue = PySequence_GetItem(pypair, 1);
      {
        SoftString key(pykey);
        placeholder->emplace_back(std::string(key.Get()));
        std::string_view key_view = placeholder->back();
        std::string_view value_view;
        if (pyvalue != Py_None) {
          SoftString value(pyvalue);
          placeholder->emplace_back(std::string(value.Get()));
          value_view = placeholder->back();
        }
        result.emplace_back(std::make_pair(key_view, value_view));
      }
      Py_DECREF(pykey);
      Py_DECREF(pyvalue);
    }
    Py_DECREF(pypair);
  }
  return result;
}

// Defines the module.
static bool DefineModule() {
  static PyModuleDef module_def = { PyModuleDef_HEAD_INIT };
  const size_t zoff = offsetof(PyModuleDef, m_name);
  std::memset((char*)&module_def + zoff, 0, sizeof(module_def) - zoff);
  module_def.m_name = "tkrzw";
  module_def.m_doc = "a set of implementations of DBM";
  module_def.m_size = -1;
  static PyMethodDef methods[] = {
    { nullptr, nullptr, 0, nullptr },
  };
  module_def.m_methods = methods;
  mod_tkrzw = PyModule_Create(&module_def);
  return true;
}

// Implementation of Utility.GetMemoryCapacity.
static PyObject* utility_GetMemoryCapacity(PyObject* self) {
  return PyLong_FromLongLong(tkrzw::GetMemoryCapacity());
}

// Implementation of Utility.GetMemoryUsage.
static PyObject* utility_GetMemoryUsage(PyObject* self) {
  return PyLong_FromLongLong(tkrzw::GetMemoryUsage());
}

// Implementation of Utility.PrimaryHash.
static PyObject* utility_PrimaryHash(PyObject* self, PyObject* pyargs) {
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc < 1 || argc > 2) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pydata = PyTuple_GET_ITEM(pyargs, 0);
  SoftString data(pydata);
  uint64_t num_buckets = 0;
  if (argc > 1) {
    PyObject* pynum = PyTuple_GET_ITEM(pyargs, 1);
    num_buckets = PyObjToInt(pynum);
  }
  if (num_buckets == 0) {
    num_buckets = tkrzw::UINT64MAX;
  }
  return PyLong_FromUnsignedLongLong(tkrzw::PrimaryHash(data.Get(), num_buckets));
}

// Implementation of Utility.SecondaryHash.
static PyObject* utility_SecondaryHash(PyObject* self, PyObject* pyargs) {
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc < 1 || argc > 2) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pydata = PyTuple_GET_ITEM(pyargs, 0);
  SoftString data(pydata);
  uint64_t num_shards = tkrzw::UINT64MAX;
  if (argc > 1) {
    PyObject* pynum = PyTuple_GET_ITEM(pyargs, 1);
    num_shards = PyObjToInt(pynum);
  }
  if (num_shards == 0) {
    num_shards = tkrzw::UINT64MAX;
  }
  return PyLong_FromUnsignedLongLong(tkrzw::SecondaryHash(data.Get(), num_shards));
}

// Implementation of Utility.EditDistanceLev.
static PyObject* utility_EditDistanceLev(PyObject* self, PyObject* pyargs) {
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc != 2) {
    ThrowInvalidArguments(argc < 2 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pyucsa = PyTuple_GET_ITEM(pyargs, 0);
  PyObject* pyucsb = PyTuple_GET_ITEM(pyargs, 1);
  if (!PyUnicode_Check(pyucsa) || PyUnicode_READY(pyucsa) != 0 || 
      !PyUnicode_Check(pyucsb) || PyUnicode_READY(pyucsb) != 0) {
    ThrowInvalidArguments("not Unicode arguments");
    return nullptr;
  }
  const std::vector<uint32_t>& ucsa = PyUnicodeToUCS4(pyucsa);
  const std::vector<uint32_t>& ucsb = PyUnicodeToUCS4(pyucsb);
  return PyLong_FromLong(tkrzw::EditDistanceLev<std::vector<uint32_t>>(ucsa, ucsb));
}

// Defines the Utility class.
static bool DefineUtility() {
  static PyTypeObject type_utility = {PyVarObject_HEAD_INIT(nullptr, 0)};
  const size_t zoff = offsetof(PyTypeObject, tp_name);
  std::memset((char*)&type_utility + zoff, 0, sizeof(type_utility) - zoff);
  type_utility.tp_name = "tkrzw.Utility";
  type_utility.tp_basicsize = sizeof(PyUtility);
  type_utility.tp_itemsize = 0;
  type_utility.tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
  type_utility.tp_doc = "Library utilities.";
  static PyMethodDef methods[] = {
    {"GetMemoryCapacity", (PyCFunction)utility_GetMemoryCapacity, METH_CLASS | METH_NOARGS,
     "Gets the memory capacity of the platform."},
    {"GetMemoryUsage", (PyCFunction)utility_GetMemoryUsage, METH_CLASS | METH_NOARGS,
     "Gets the current memory usage of the process."},
    {"PrimaryHash", (PyCFunction)utility_PrimaryHash, METH_CLASS | METH_VARARGS,
     "Primary hash function for the hash database."},
    {"SecondaryHash", (PyCFunction)utility_SecondaryHash, METH_CLASS | METH_VARARGS,
     "Secondary hash function for sharding."},
    {"EditDistanceLev", (PyCFunction)utility_EditDistanceLev, METH_CLASS | METH_VARARGS,
     "Gets the Levenshtein edit distance of two Unicode strings."},
    { nullptr, nullptr, 0, nullptr },
  };
  type_utility.tp_methods = methods;
  if (PyType_Ready(&type_utility) != 0) return false;
  cls_utility = (PyObject*)&type_utility;
  Py_INCREF(cls_utility);
  if (!SetConstStr(cls_utility, "VERSION", tkrzw::PACKAGE_VERSION)) return false;
  if (!SetConstStr(cls_utility, "OS_NAME", tkrzw::OS_NAME)) return false;
  if (!SetConstLong(cls_utility, "INT32MIN", (int64_t)tkrzw::INT32MIN)) return false;
  if (!SetConstLong(cls_utility, "INT32MAX", (int64_t)tkrzw::INT32MAX)) return false;
  if (!SetConstUnsignedLong(cls_utility, "UINT32MAX", (uint64_t)tkrzw::UINT32MAX)) return false;
  if (!SetConstLong(cls_utility, "INT64MIN", (int64_t)tkrzw::INT64MIN)) return false;
  if (!SetConstLong(cls_utility, "INT64MAX", (int64_t)tkrzw::INT64MAX)) return false;
  if (!SetConstUnsignedLong(cls_utility, "UINT64MAX", (uint64_t)tkrzw::UINT64MAX)) return false;
  if (PyModule_AddObject(mod_tkrzw, "Utility", cls_utility) != 0) return false;
  return true;
}

// Implementation of Status.new.
static PyObject* status_new(PyTypeObject* pytype, PyObject* pyargs, PyObject* pykwds) {
  PyTkStatus* self = (PyTkStatus*)pytype->tp_alloc(pytype, 0);
  if (!self) return nullptr;
  self->status = new tkrzw::Status();
  return (PyObject*)self;
}

// Implementation of Status#dealloc.
static void status_dealloc(PyTkStatus* self) {
  delete self->status;
  Py_TYPE(self)->tp_free((PyObject*)self);
}

// Implementation of Status#__init__.
static int status_init(PyTkStatus* self, PyObject* pyargs, PyObject* pykwds) {
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc > 2) {
    ThrowInvalidArguments("too many arguments");
    return -1;
  }
  tkrzw::Status::Code code = tkrzw::Status::SUCCESS;
  if (argc > 0) {
    PyObject* pycode = PyTuple_GET_ITEM(pyargs, 0);
    code = (tkrzw::Status::Code)PyLong_AsLong(pycode);
  }
  if (argc > 1) {
    PyObject* pymessage = PyTuple_GET_ITEM(pyargs, 1);
    SoftString str(pymessage);
    self->status->Set(code, str.Get());
  } else {
    self->status->Set(code);
  }
  return 0;
}

// Implementation of Status#__repr__.
static PyObject* status_repr(PyTkStatus* self) {
  return CreatePyString(tkrzw::StrCat("<tkrzw.Status: ", *self->status, ">"));
}

// Implementation of Status#__str__.
static PyObject* status_str(PyTkStatus* self) {
  return CreatePyString(tkrzw::ToString(*self->status));
}

// Implementation of Status#__richcmp__.
static PyObject* status_richcmp(PyTkStatus* self, PyObject* pyrhs, int op) {
  bool rv = false;
  int32_t code = (int32_t)self->status->GetCode();
  int32_t rcode = 0;
  if (PyObject_IsInstance(pyrhs, cls_status)) {
    PyTkStatus* pyrhs_status = (PyTkStatus*)pyrhs;
    rcode = (int32_t)pyrhs_status->status->GetCode();
  } else if (PyLong_Check(pyrhs)) {
    rcode = (int32_t)PyLong_AsLong(pyrhs);
  } else {
    rcode = tkrzw::INT32MAX;
  }
  switch (op) {
    case Py_LT: rv = code < rcode; break;
    case Py_LE: rv = code <= rcode; break;
    case Py_EQ: rv = code == rcode; break;
    case Py_NE: rv = code != rcode; break;
    case Py_GT: rv = code > rcode; break;
    case Py_GE: rv = code >= rcode; break;
    default: rv = false; break;
  }
  if (rv) {
    Py_RETURN_TRUE;
  }
  Py_RETURN_FALSE;
}

// Implementation of Status#Set.
static PyObject* status_Set(PyTkStatus* self, PyObject* pyargs) {
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc > 2) {
    ThrowInvalidArguments("too many arguments");
    Py_RETURN_NONE;
  }
  tkrzw::Status::Code code = tkrzw::Status::SUCCESS;
  if (argc > 0) {
    PyObject* pycode = PyTuple_GET_ITEM(pyargs, 0);
    code = (tkrzw::Status::Code)PyLong_AsLong(pycode);
  }
  if (argc > 1) {
    PyObject* pymessage = PyTuple_GET_ITEM(pyargs, 1);
    SoftString str(pymessage);
    self->status->Set(code, str.Get());
  } else {
    self->status->Set(code);
  }
  Py_RETURN_NONE;
}

// Implementation of Status#GetCode.
static PyObject* status_GetCode(PyTkStatus* self) {
  return PyLong_FromLongLong(self->status->GetCode());
}

// Implementation of Status#GetMessage.
static PyObject* status_GetMessage(PyTkStatus* self) {
  return PyUnicode_FromString(self->status->GetMessage().c_str());
}

// Implementation of Status#IsOK.
static PyObject* status_IsOK(PyTkStatus* self) {
  if (*self->status == tkrzw::Status::SUCCESS) {
    Py_RETURN_TRUE;
  }
  Py_RETURN_FALSE;
}

// Implementation of Status#OrDie.
static PyObject* status_OrDie(PyTkStatus* self) {
  if (*self->status != tkrzw::Status::SUCCESS) {
    ThrowStatusException(*self->status);    
    return nullptr;
  }
  Py_RETURN_NONE;
}

// Defines the Status class.
static bool DefineStatus() {
  static PyTypeObject type_status = {PyVarObject_HEAD_INIT(nullptr, 0)};
  const size_t zoff = offsetof(PyTypeObject, tp_name);
  std::memset((char*)&type_status + zoff, 0, sizeof(type_status) - zoff);
  type_status.tp_name = "tkrzw.Status";
  type_status.tp_basicsize = sizeof(PyTkStatus);
  type_status.tp_itemsize = 0;
  type_status.tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
  type_status.tp_doc = "Status of operations.";
  type_status.tp_new = status_new;
  type_status.tp_dealloc = (destructor)status_dealloc;
  type_status.tp_init = (initproc)status_init;
  type_status.tp_repr = (unaryfunc)status_repr;
  type_status.tp_str = (unaryfunc)status_str;
  type_status.tp_richcompare = (richcmpfunc)status_richcmp;
  static PyMethodDef methods[] = {
    {"Set", (PyCFunction)status_Set, METH_VARARGS,
     "Set the code and the message."},
    {"GetCode", (PyCFunction)status_GetCode, METH_NOARGS,
     "Gets the status code.."},
    {"GetMessage", (PyCFunction)status_GetMessage, METH_NOARGS,
     "Gets the status message."},
    {"IsOK", (PyCFunction)status_IsOK, METH_NOARGS,
     "Returns true if the status is success."},
    {"OrDie", (PyCFunction)status_OrDie, METH_NOARGS,
     "Raises a runtime error if the status is not success."},
    { nullptr, nullptr, 0, nullptr },
  };
  type_status.tp_methods = methods;
  if (PyType_Ready(&type_status) != 0) return false;
  cls_status = (PyObject*)&type_status;
  Py_INCREF(cls_status);
  if (!SetConstLong(cls_status, "SUCCESS",
                    (int64_t)tkrzw::Status::SUCCESS)) return false;
  if (!SetConstLong(cls_status, "UNKNOWN_ERROR",
                    (int64_t)tkrzw::Status::UNKNOWN_ERROR)) return false;
  if (!SetConstLong(cls_status, "SYSTEM_ERROR",
                    (int64_t)tkrzw::Status::SYSTEM_ERROR)) return false;
  if (!SetConstLong(cls_status, "NOT_IMPLEMENTED_ERROR",
                    (int64_t)tkrzw::Status::NOT_IMPLEMENTED_ERROR)) return false;
  if (!SetConstLong(cls_status, "PRECONDITION_ERROR",
                    (int64_t)tkrzw::Status::PRECONDITION_ERROR)) return false;
  if (!SetConstLong(cls_status, "INVALID_ARGUMENT_ERROR",
                    (int64_t)tkrzw::Status::INVALID_ARGUMENT_ERROR)) return false;
  if (!SetConstLong(cls_status, "CANCELED_ERROR",
                    (int64_t)tkrzw::Status::CANCELED_ERROR)) return false;
  if (!SetConstLong(cls_status, "NOT_FOUND_ERROR",
                    (int64_t)tkrzw::Status::NOT_FOUND_ERROR)) return false;
  if (!SetConstLong(cls_status, "PERMISSION_ERROR",
                    (int64_t)tkrzw::Status::PERMISSION_ERROR)) return false;
  if (!SetConstLong(cls_status, "INFEASIBLE_ERROR",
                    (int64_t)tkrzw::Status::INFEASIBLE_ERROR)) return false;
  if (!SetConstLong(cls_status, "DUPLICATION_ERROR",
                    (int64_t)tkrzw::Status::DUPLICATION_ERROR)) return false;
  if (!SetConstLong(cls_status, "BROKEN_DATA_ERROR",
                    (int64_t)tkrzw::Status::BROKEN_DATA_ERROR)) return false;
  if (!SetConstLong(cls_status, "APPLICATION_ERROR",
                    (int64_t)tkrzw::Status::APPLICATION_ERROR)) return false;
  if (PyModule_AddObject(mod_tkrzw, "Status", cls_status) != 0) return false;
  return true;
}

// Implementation of StatusException.new.
static PyObject* expt_new(PyTypeObject* pytype, PyObject* pyargs, PyObject* pykwds) {
  PyException* self = (PyException*)pytype->tp_alloc(pytype, 0);
  if (!self) return nullptr;
  self->pystatus = nullptr;
  return (PyObject*)self;
}

// Implementation of StatusException#dealloc.
static void expt_dealloc(PyException* self) {
  if (self->pystatus) Py_DECREF(self->pystatus);
  Py_TYPE(self)->tp_free((PyObject*)self);
}

// Implementation of StatusException#__init__.
static int expt_init(PyException* self, PyObject* pyargs, PyObject* pykwds) {
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc != 1) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return -1;
  }
  PyObject* pystatus = PyTuple_GET_ITEM(pyargs, 0);
  if (!PyObject_IsInstance(pystatus, cls_status)) {
    ThrowInvalidArguments("the argument is not a status");
    return -1;
  }
  Py_INCREF(pystatus);
  self->pystatus = pystatus;
  return 0;
}

// Implementation of StatusException#__repr__.
static PyObject* expt_repr(PyException* self) {
  const tkrzw::Status* status = ((PyTkStatus*)self->pystatus)->status;
  return CreatePyString(tkrzw::StrCat("<tkrzw.StatusException: ", *status, ">"));
}

// Implementation of StatusException#__str__.
static PyObject* expt_str(PyException* self) {
  const tkrzw::Status* status = ((PyTkStatus*)self->pystatus)->status;
  return CreatePyString(tkrzw::ToString(*status));
}

// Implementation of StatusException#GetStatus.
static PyObject* expt_GetStatus(PyException* self) {
  Py_INCREF(self->pystatus);
  return self->pystatus;
}

// Defines the StatusException class.
static bool DefineStatusException() {
  static PyTypeObject type_expt = {PyVarObject_HEAD_INIT(nullptr, 0)};
  const size_t zoff = offsetof(PyTypeObject, tp_name);
  std::memset((char*)&type_expt + zoff, 0, sizeof(type_expt) - zoff);
  type_expt.tp_name = "tkrzw.StatusException";
  type_expt.tp_basicsize = sizeof(PyException);
  type_expt.tp_itemsize = 0;
  type_expt.tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
  type_expt.tp_doc = "Exception to convey the status of operations.";
  type_expt.tp_new = expt_new;
  type_expt.tp_dealloc = (destructor)expt_dealloc;
  type_expt.tp_init = (initproc)expt_init;
  type_expt.tp_repr = (unaryfunc)expt_repr;
  type_expt.tp_str = (unaryfunc)expt_str;
  static PyMethodDef expt_methods[] = {
    { "GetStatus", (PyCFunction)expt_GetStatus, METH_NOARGS,
      "Get the status object." },
    { nullptr, nullptr, 0, nullptr }
  };
  type_expt.tp_methods = expt_methods;
  type_expt.tp_base = (PyTypeObject*)PyExc_RuntimeError;
  if (PyType_Ready(&type_expt) != 0) return false;
  cls_expt = (PyObject*)&type_expt;
  Py_INCREF(cls_expt);
  if (PyModule_AddObject(mod_tkrzw, "StatusException", cls_expt) != 0) return false;
  return true;
}

// Implementation of DBM.new.
static PyObject* dbm_new(PyTypeObject* pytype, PyObject* pyargs, PyObject* pykwds) {
  PyDBM* self = (PyDBM*)pytype->tp_alloc(pytype, 0);
  if (!self) return nullptr;
  self->dbm = nullptr;
  self->concurrent = false;
  return (PyObject*)self;
}

// Implementation of DBM#dealloc.
static void dbm_dealloc(PyDBM* self) {
  delete self->dbm;
  Py_TYPE(self)->tp_free((PyObject*)self);
}

// Implementation of DBM#__init__.
static int dbm_init(PyDBM* self, PyObject* pyargs, PyObject* pykwds) {
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc != 0) {
    ThrowInvalidArguments("too many arguments");
    return -1;
  }
  return 0;
}

// Implementation of DBM#__repr__.
static PyObject* dbm_repr(PyDBM* self) {
  std::string class_name = "unknown";
  std::string path = "-";
  int64_t num_records = -1;
  if (self->dbm != nullptr) {
    NativeLock lock(self->concurrent);
    for (const auto& rec : self->dbm->Inspect()) {
      if (rec.first == "class") {
        class_name = rec.second;
      } else if (rec.first == "path") {
        path = rec.second;
      }
    }   
    num_records = self->dbm->CountSimple();
  }
  const std::string& str = tkrzw::StrCat(
      "<tkrzw.DBM: class=", class_name,
      " path=", tkrzw::StrEscapeC(path, true), " num_records=", num_records, ">");
  return CreatePyString(str);
}

// Implementation of DBM#__str__.
static PyObject* dbm_str(PyDBM* self) {
  std::string class_name = "unknown";
  std::string path = "-";
  int64_t num_records = -1;
  if (self->dbm != nullptr) {
    NativeLock lock(self->concurrent);
    for (const auto& rec : self->dbm->Inspect()) {
      if (rec.first == "class") {
        class_name = rec.second;
      } else if (rec.first == "path") {
        path = rec.second;
      }
    }
    num_records = self->dbm->CountSimple();
  }
  const std::string& str = tkrzw::StrCat(
      class_name, ":", tkrzw::StrEscapeC(path, true), ":", num_records);
  return CreatePyString(str);
}

// Implementation of DBM#Open.
static PyObject* dbm_Open(PyDBM* self, PyObject* pyargs, PyObject* pykwds) {
  if (self->dbm != nullptr) {
    ThrowInvalidArguments("opened database");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc != 2) {
    ThrowInvalidArguments(argc < 2 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pypath = PyTuple_GET_ITEM(pyargs, 0);
  PyObject* pywritable = PyTuple_GET_ITEM(pyargs, 1);
  SoftString path(pypath);
  const bool writable = PyObject_IsTrue(pywritable);
  int32_t num_shards = -1;
  bool concurrent = false;
  int32_t open_options = 0;
  std::map<std::string, std::string> params;
  if (pykwds != nullptr) {
    params = MapKeywords(pykwds);
    num_shards = tkrzw::StrToInt(tkrzw::SearchMap(params, "num_shards", "-1"));
    if (tkrzw::StrToBool(tkrzw::SearchMap(params, "concurrent", "false"))) {
      concurrent = true;
    }
    if (tkrzw::StrToBool(tkrzw::SearchMap(params, "truncate", "false"))) {
      open_options |= tkrzw::File::OPEN_TRUNCATE;
    }
    if (tkrzw::StrToBool(tkrzw::SearchMap(params, "no_create", "false"))) {
      open_options |= tkrzw::File::OPEN_NO_CREATE;
    }
    if (tkrzw::StrToBool(tkrzw::SearchMap(params, "no_wait", "false"))) {
      open_options |= tkrzw::File::OPEN_NO_WAIT;
    }
    if (tkrzw::StrToBool(tkrzw::SearchMap(params, "no_lock", "false"))) {
      open_options |= tkrzw::File::OPEN_NO_LOCK;
    }
    params.erase("concurrent");
    params.erase("truncate");
    params.erase("no_create");
    params.erase("no_wait");
    params.erase("no_lock");
  }
  if (num_shards >= 0) {
    self->dbm = new tkrzw::ShardDBM();
  } else {
    self->dbm = new tkrzw::PolyDBM();
  }
  self->concurrent = concurrent;
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->dbm->OpenAdvanced(std::string(path.Get()), writable, open_options, params);
  }
  if (status != tkrzw::Status::SUCCESS) {
    delete self->dbm;
    self->dbm = nullptr;
  }
  return CreatePyTkStatus(status);
}

// Implementation of DBM#Close.
static PyObject* dbm_Close(PyDBM* self) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->dbm->Close();
  }
  return CreatePyTkStatus(status);
}

// Implementation of DBM#Get.
static PyObject* dbm_Get(PyDBM* self, PyObject* pyargs) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc < 1 || argc > 2) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pykey = PyTuple_GET_ITEM(pyargs, 0);
  SoftString key(pykey);
  PyObject* pystatus = nullptr;
  if (argc > 1) {
    pystatus = PyTuple_GET_ITEM(pyargs, 1);
    if (pystatus == Py_None) {
      pystatus = nullptr;
    } else if (!PyObject_IsInstance(pystatus, cls_status)) {
      ThrowInvalidArguments("not a status object");
      return nullptr;
    }
  }
  std::string value;
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->dbm->Get(key.Get(), &value);
  }
  if (pystatus != nullptr) {
    *((PyTkStatus*)pystatus)->status = status;
  }
  if (status != tkrzw::Status::SUCCESS) {
    Py_RETURN_NONE;
  }
  return CreatePyBytes(value);
}

// Implementation of DBM#GetStr.
static PyObject* dbm_GetStr(PyDBM* self, PyObject* pyargs) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc < 1 || argc > 2) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pykey = PyTuple_GET_ITEM(pyargs, 0);
  SoftString key(pykey);
  PyObject* pystatus = nullptr;
  if (argc > 1) {
    pystatus = PyTuple_GET_ITEM(pyargs, 1);
    if (pystatus == Py_None) {
      pystatus = nullptr;
    } else if (!PyObject_IsInstance(pystatus, cls_status)) {
      ThrowInvalidArguments("not a status object");
      return nullptr;
    }
  }
  std::string value;
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->dbm->Get(key.Get(), &value);
  }
  if (pystatus != nullptr) {
    *((PyTkStatus*)pystatus)->status = status;
  }
  if (status != tkrzw::Status::SUCCESS) {
    Py_RETURN_NONE;
  }
  return CreatePyString(value);
}

// Implementation of DBM#GetMulti.
static PyObject* dbm_GetMulti(PyDBM* self, PyObject* pyargs) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  std::vector<std::string> keys;
  for (int32_t i = 0; i < argc; i++) {
    PyObject* pykey = PyTuple_GET_ITEM(pyargs, i);
    SoftString key(pykey);
    keys.emplace_back(std::string(key.Get()));
  }
  std::vector<std::string_view> key_views(keys.begin(), keys.end());
  std::map<std::string, std::string> records;
  {
    NativeLock lock(self->concurrent);
    records = self->dbm->GetMulti(key_views);
  }
  PyObject* pyrv = PyDict_New();
  for (const auto& rec : records) {
    PyObject* pyname = CreatePyBytes(rec.first);
    PyObject* pyvalue = CreatePyBytes(rec.second);
    PyDict_SetItem(pyrv, pyname, pyvalue);
    Py_DECREF(pyvalue);
    Py_DECREF(pyname);
  }
  return pyrv;
}

// Implementation of DBM#GetMultiStr.
static PyObject* dbm_GetMultiStr(PyDBM* self, PyObject* pyargs) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  std::vector<std::string> keys;
  for (int32_t i = 0; i < argc; i++) {
    PyObject* pykey = PyTuple_GET_ITEM(pyargs, i);
    SoftString key(pykey);
    keys.emplace_back(std::string(key.Get()));
  }
  std::vector<std::pair<std::string, std::string>> records;
  {
    NativeLock lock(self->concurrent);
    for (const auto& key : keys) {
      std::string value;
      if (self->dbm->Get(key, &value) == tkrzw::Status::SUCCESS) {
        records.emplace_back(std::make_pair(key, value));
      }
    }
  }
  PyObject* pyrv = PyDict_New();
  for (const auto& rec : records) {
    PyObject* pyname = CreatePyString(rec.first);
    PyObject* pyvalue = CreatePyString(rec.second);
    PyDict_SetItem(pyrv, pyname, pyvalue);
    Py_DECREF(pyvalue);
    Py_DECREF(pyname);
  }
  return pyrv;
}

// Implementation of DBM#Set.
static PyObject* dbm_Set(PyDBM* self, PyObject* pyargs) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc < 2 || argc > 3) {
    ThrowInvalidArguments(argc < 2 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pykey = PyTuple_GET_ITEM(pyargs, 0);
  PyObject* pyvalue = PyTuple_GET_ITEM(pyargs, 1);
  const bool overwrite = argc > 2 ? PyObject_IsTrue(PyTuple_GET_ITEM(pyargs, 2)) : true;
  SoftString key(pykey);
  SoftString value(pyvalue);
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->dbm->Set(key.Get(), value.Get(), overwrite);
  }
  return CreatePyTkStatus(status);
}

// Implementation of DBM#SetMulti.
static PyObject* dbm_SetMulti(PyDBM* self, PyObject* pyargs, PyObject* pykwds) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc != 0) {
    ThrowInvalidArguments("too many arguments");
    return nullptr;
  }
  std::map<std::string, std::string> records;
  if (pykwds != nullptr) {
    records = MapKeywords(pykwds);
  }
  std::map<std::string_view, std::string_view> record_views;
  for (const auto& record : records) {
    record_views.emplace(std::make_pair(
        std::string_view(record.first), std::string_view(record.second)));
  }
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->dbm->SetMulti(record_views);
  }
  return CreatePyTkStatus(status);
}

// Implementation of DBM#SetAndGet.
static PyObject* dbm_SetAndGet(PyDBM* self, PyObject* pyargs) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc < 2 || argc > 3) {
    ThrowInvalidArguments(argc < 2 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pykey = PyTuple_GET_ITEM(pyargs, 0);
  PyObject* pyvalue = PyTuple_GET_ITEM(pyargs, 1);
  const bool overwrite = argc > 2 ? PyObject_IsTrue(PyTuple_GET_ITEM(pyargs, 2)) : true;
  SoftString key(pykey);
  SoftString value(pyvalue);
  tkrzw::Status impl_status(tkrzw::Status::SUCCESS);
  std::string old_value;
  bool hit = false;
  class Processor final : public tkrzw::DBM::RecordProcessor {
   public:
    Processor(tkrzw::Status* status, std::string_view value, bool overwrite,
              std::string* old_value, bool* hit)
        : status_(status), value_(value), overwrite_(overwrite),
          old_value_(old_value), hit_(hit) {}
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      *old_value_ = value;
      *hit_ = true;
      if (overwrite_) {
        return value_;
      }
      status_->Set(tkrzw::Status::DUPLICATION_ERROR);
      return NOOP;
    }
    std::string_view ProcessEmpty(std::string_view key) override {
      return value_;
    }
   private:
    tkrzw::Status* status_;
    std::string_view value_;
    bool overwrite_;
    std::string* old_value_;
    bool* hit_;
  };
  Processor proc(&impl_status, value.Get(), overwrite, &old_value, &hit);
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->dbm->Process(key.Get(), &proc, true);
  }
  status |= impl_status;
  PyObject* pytuple = PyTuple_New(2);
  PyTuple_SET_ITEM(pytuple, 0, CreatePyTkStatus(status));
  if (hit) {
    PyObject* pyold_value = PyUnicode_Check(pyvalue) ?
        CreatePyString(old_value) : CreatePyBytes(old_value);
    PyTuple_SET_ITEM(pytuple, 1, pyold_value);
  } else {
    Py_INCREF(Py_None);
    PyTuple_SET_ITEM(pytuple, 1, Py_None);
  }
  return pytuple;
}

// Implementation of DBM#Remove.
static PyObject* dbm_Remove(PyDBM* self, PyObject* pyargs) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc != 1) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pykey = PyTuple_GET_ITEM(pyargs, 0);
  SoftString key(pykey);
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->dbm->Remove(key.Get());
  }
  return CreatePyTkStatus(status);
}

// Implementation of DBM#RemoveMulti.
static PyObject* dbm_RemoveMulti(PyDBM* self, PyObject* pyargs) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  std::vector<std::string> keys;
  for (int32_t i = 0; i < argc; i++) {
    PyObject* pykey = PyTuple_GET_ITEM(pyargs, i);
    SoftString key(pykey);
    keys.emplace_back(std::string(key.Get()));
  }
  std::vector<std::string_view> key_views(keys.begin(), keys.end());
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->dbm->RemoveMulti(key_views);
  }
  return CreatePyTkStatus(status);
}

// Implementation of DBM#RemoveAndGet.
static PyObject* dbm_RemoveAndGet(PyDBM* self, PyObject* pyargs) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc != 1) {
    ThrowInvalidArguments(argc < 2 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pykey = PyTuple_GET_ITEM(pyargs, 0);
  SoftString key(pykey);
  tkrzw::Status impl_status(tkrzw::Status::SUCCESS);
  std::string old_value;
  class Processor final : public tkrzw::DBM::RecordProcessor {
   public:
    Processor(tkrzw::Status* status, std::string* old_value)
        : status_(status), old_value_(old_value) {}
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      *old_value_ = value;
      return REMOVE;
    }
    std::string_view ProcessEmpty(std::string_view key) override {
      status_->Set(tkrzw::Status::NOT_FOUND_ERROR);
      return NOOP;
    }
   private:
    tkrzw::Status* status_;
    std::string* old_value_;
  };
  Processor proc(&impl_status, &old_value);
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->dbm->Process(key.Get(), &proc, true);
  }
  status |= impl_status;
  PyObject* pytuple = PyTuple_New(2);
  PyTuple_SET_ITEM(pytuple, 0, CreatePyTkStatus(status));
  if (status == tkrzw::Status::SUCCESS) {
    PyObject* pyold_value = PyUnicode_Check(pykey) ?
        CreatePyString(old_value) : CreatePyBytes(old_value);
    PyTuple_SET_ITEM(pytuple, 1, pyold_value);
  } else {
    Py_INCREF(Py_None);
    PyTuple_SET_ITEM(pytuple, 1, Py_None);
  }
  return pytuple;
}

// Implementation of DBM#Append.
static PyObject* dbm_Append(PyDBM* self, PyObject* pyargs) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc < 2 || argc > 3) {
    ThrowInvalidArguments(argc < 2 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pykey = PyTuple_GET_ITEM(pyargs, 0);
  PyObject* pyvalue = PyTuple_GET_ITEM(pyargs, 1);
  PyObject* pydelim = argc > 2 ? PyTuple_GET_ITEM(pyargs, 2) : nullptr;
  SoftString key(pykey);
  SoftString value(pyvalue);
  SoftString delim(pydelim == nullptr ? Py_None : pydelim);
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->dbm->Append(key.Get(), value.Get(), delim.Get());
  }
  return CreatePyTkStatus(status);
}

// Implementation of DBM#CompareExchange.
static PyObject* dbm_CompareExchange(PyDBM* self, PyObject* pyargs) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc != 3) {
    ThrowInvalidArguments(argc < 3 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pykey = PyTuple_GET_ITEM(pyargs, 0);
  PyObject* pyexpected = PyTuple_GET_ITEM(pyargs, 1);
  PyObject* pydesired = PyTuple_GET_ITEM(pyargs, 2);
  SoftString key(pykey);
  std::unique_ptr<SoftString> expected;
  std::string_view expected_view;
  if (pyexpected != Py_None) {
    expected = std::make_unique<SoftString>(pyexpected);
    expected_view = expected->Get();
  }
  std::unique_ptr<SoftString> desired;
  std::string_view desired_view;
  if (pydesired != Py_None) {
    desired = std::make_unique<SoftString>(pydesired);
    desired_view = desired->Get();
  }
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->dbm->CompareExchange(key.Get(), expected_view, desired_view);
  }
  return CreatePyTkStatus(status);
}

// Implementation of DBM#Increment.
static PyObject* dbm_Increment(PyDBM* self, PyObject* pyargs) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc < 1 || argc > 4) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pykey = PyTuple_GET_ITEM(pyargs, 0);
  SoftString key(pykey);
  int64_t inc = 1;
  if (argc > 1) {
    PyObject* pyinc = PyTuple_GET_ITEM(pyargs, 1);
    inc = PyObjToInt(pyinc);
  }
  int64_t init = 0;
  if (argc > 2) {
    PyObject* pyinit = PyTuple_GET_ITEM(pyargs, 2);
    init = PyObjToInt(pyinit);
  }
  PyObject* pystatus = nullptr;
  if (argc > 3) {
    pystatus = PyTuple_GET_ITEM(pyargs, 3);
    if (pystatus == Py_None) {
      pystatus = nullptr;
    } else if (!PyObject_IsInstance(pystatus, cls_status)) {
      ThrowInvalidArguments("not a status object");
      return nullptr;
    }
  }
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  int64_t current = 0;
  {
    NativeLock lock(self->concurrent);
    status = self->dbm->Increment(key.Get(), inc, &current, init);
  }
  if (pystatus != nullptr) {
    *((PyTkStatus*)pystatus)->status = status;
  }
  if (status == tkrzw::Status::SUCCESS) {
    return PyLong_FromLongLong(current);
  }
  Py_RETURN_NONE;
}

// Implementation of DBM#CompareExchangeMulti.
static PyObject* dbm_CompareExchangeMulti(PyDBM* self, PyObject* pyargs) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc != 2) {
    ThrowInvalidArguments(argc < 2 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pyexpected = PyTuple_GET_ITEM(pyargs, 0);
  PyObject* pydesired = PyTuple_GET_ITEM(pyargs, 1);
  if (!PySequence_Check(pyexpected) || !PySequence_Check(pydesired)) {
    ThrowInvalidArguments("parameters must be sequences of strings");
    return nullptr;
  }
  std::vector<std::string> expected_ph;
  const auto& expected = ExtractSVPairs(pyexpected, &expected_ph);
  std::vector<std::string> desired_ph;
  const auto& desired = ExtractSVPairs(pydesired, &desired_ph);
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->dbm->CompareExchangeMulti(expected, desired);
  }
  return CreatePyTkStatus(status);
}

// Implementation of DBM#Count.
static PyObject* dbm_Count(PyDBM* self) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
  int64_t count = -1;
  {
    NativeLock lock(self->concurrent);
    count = self->dbm->CountSimple();
  }
  if (count >= 0) {
    return PyLong_FromLongLong(count);
  }
  Py_RETURN_NONE;
}

// Implementation of DBM#GetFileSize.
static PyObject* dbm_GetFileSize(PyDBM* self) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
  int64_t file_size = -1;
  {
    NativeLock lock(self->concurrent);
    file_size = self->dbm->GetFileSizeSimple();
  }
  if (file_size >= 0) {
    return PyLong_FromLongLong(file_size);
  }
  Py_RETURN_NONE;
}

// Implementation of DBM#GetFilePath.
static PyObject* dbm_GetFilePath(PyDBM* self) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
  std::string path;
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->dbm->GetFilePath(&path);
  }
  if (status == tkrzw::Status::SUCCESS) {
    return CreatePyString(path);
  }
  Py_RETURN_NONE;
}

// Implementation of DBM#Clear.
static PyObject* dbm_Clear(PyDBM* self) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->dbm->Clear();
  }
  return CreatePyTkStatus(status);
}

// Implementation of DBM#Rebuild.
static PyObject* dbm_Rebuild(PyDBM* self, PyObject* pyargs, PyObject* pykwds) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc != 0) {
    ThrowInvalidArguments("too many arguments");
    return nullptr;
  }
  std::map<std::string, std::string> params;
  if (pykwds != nullptr) {
    params = MapKeywords(pykwds);
  }
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->dbm->RebuildAdvanced(params);
  }
  return CreatePyTkStatus(status);
}

// Implementation of DBM#ShouldBeRebuilt.
static PyObject* dbm_ShouldBeRebuilt(PyDBM* self) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
  bool tobe = false;
  {
    NativeLock lock(self->concurrent);
    tobe = self->dbm->ShouldBeRebuiltSimple();
  }
  return PyBool_FromLong(tobe);
}

// Implementation of DBM#Synchronize.
static PyObject* dbm_Synchronize(PyDBM* self, PyObject* pyargs, PyObject* pykwds) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc != 1) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pyhard = PyTuple_GET_ITEM(pyargs, 0);
  const bool hard = PyObject_IsTrue(pyhard);
  std::map<std::string, std::string> params;
  if (pykwds != nullptr) {
    params = MapKeywords(pykwds);
  }
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->dbm->SynchronizeAdvanced(hard, nullptr, params);
  }
  return CreatePyTkStatus(status);
}

// Implementation of DBM#CopyFileData.
static PyObject* dbm_CopyFileData(PyDBM* self, PyObject* pyargs) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc != 1) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pydest = PyTuple_GET_ITEM(pyargs, 0);
  SoftString dest(pydest);
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->dbm->CopyFileData(std::string(dest.Get()));
  }
  return CreatePyTkStatus(status);
}

// Implementation of DBM#Export.
static PyObject* dbm_Export(PyDBM* self, PyObject* pyargs) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc != 1) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pydest = PyTuple_GET_ITEM(pyargs, 0);
  if (!PyObject_IsInstance(pydest, cls_dbm)) {
    ThrowInvalidArguments("the argument is not a DBM");
    return nullptr;
  }
  PyDBM* dest = (PyDBM*)pydest;
  if (dest->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->dbm->Export(dest->dbm);
  }
  return CreatePyTkStatus(status);
}

// Implementation of DBM#ExportKeysAsLines.
static PyObject* dbm_ExportKeysAsLines(PyDBM* self, PyObject* pyargs) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc != 1) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pydest = PyTuple_GET_ITEM(pyargs, 0);
  SoftString dest(pydest);
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    tkrzw::MemoryMapParallelFile file;
    status = file.Open(std::string(dest.Get()), true, tkrzw::File::OPEN_TRUNCATE);
    if (status == tkrzw::Status::SUCCESS) {
      status |= tkrzw::ExportDBMKeysAsLines(self->dbm, &file);
      status |= file.Close();
    }
  }
  return CreatePyTkStatus(status);
}

// Implementation of DBM#Inspect.
static PyObject* dbm_Inspect(PyDBM* self) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
  std::vector<std::pair<std::string, std::string>> records;
  {
    NativeLock lock(self->concurrent);
    records = self->dbm->Inspect();
  }
  PyObject* pyrv = PyDict_New();
  for (const auto& rec : records) {
    PyObject* pyname = CreatePyString(rec.first);
    PyObject* pyvalue = CreatePyString(rec.second);
    PyDict_SetItem(pyrv, pyname, pyvalue);
    Py_DECREF(pyvalue);
    Py_DECREF(pyname);
  }
  return pyrv;
}

// Implementation of DBM#IsOpen.
static PyObject* dbm_IsOpen(PyDBM* self) {
  if (self->dbm == nullptr) {
    Py_RETURN_FALSE;
  }
  Py_RETURN_TRUE;  
}

// Implementation of DBM#IsHealthy.
static PyObject* dbm_IsHealthy(PyDBM* self) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
  bool healthy = false;
  {
    NativeLock lock(self->concurrent);
    healthy = self->dbm->IsHealthy();
  }
  if (healthy) {
    Py_RETURN_TRUE;
  }
  Py_RETURN_FALSE;
}

// Implementation of DBM#IsOrdered.
static PyObject* dbm_IsOrdered(PyDBM* self) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
  bool ordered = false;
  {
    NativeLock lock(self->concurrent);
    ordered = self->dbm->IsOrdered();
  }
  if (ordered) {
    Py_RETURN_TRUE;
  }
  Py_RETURN_FALSE;
}

// Implementation of DBM#Search.
static PyObject* dbm_Search(PyDBM* self, PyObject* pyargs) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc < 2 || argc > 4) {
    ThrowInvalidArguments(argc < 2 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pymode = PyTuple_GET_ITEM(pyargs, 0);
  PyObject* pypattern = PyTuple_GET_ITEM(pyargs, 1);
  int32_t capacity = 0;
  if (argc > 2) {
    capacity = PyObjToInt(PyTuple_GET_ITEM(pyargs, 2));
  }
  bool utf = false;
  if (argc > 3) {
    utf = PyObject_IsTrue(PyTuple_GET_ITEM(pyargs, 3));
  }
  SoftString pattern(pypattern);
  SoftString mode(pymode);
  std::vector<std::string> keys;
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = tkrzw::SearchDBMModal(
        self->dbm, mode.Get(), pattern.Get(), &keys, capacity, utf);
  }
  if (status != tkrzw::Status::SUCCESS) {
    ThrowStatusException(status);    
    return nullptr;
  }
  PyObject* pyrv = PyList_New(keys.size());
  for (size_t i = 0; i < keys.size(); i++) {
    PyList_SET_ITEM(pyrv, i, CreatePyString(keys[i]));
  }
  return pyrv;
}

// Implementation of DBM#MakeIterator.
static PyObject* dbm_MakeIterator(PyDBM* self) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
  PyTypeObject* pyitertype = (PyTypeObject*)cls_iter;
  PyIterator* pyiter = (PyIterator*)pyitertype->tp_alloc(pyitertype, 0);
  if (!pyiter) return nullptr;
  {
    NativeLock lock(self->concurrent);
    pyiter->iter = self->dbm->MakeIterator().release();
  }
  pyiter->concurrent = self->concurrent;
  return (PyObject*)pyiter;
}

// Implementation of DBM.RestoreDatabase.
static PyObject* dbm_RestoreDatabase(PyObject* self, PyObject* pyargs) {
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc < 2 || argc > 4) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  SoftString old_file_path(PyTuple_GET_ITEM(pyargs, 0));
  SoftString new_file_path(PyTuple_GET_ITEM(pyargs, 1));
  SoftString class_name(argc > 2 ? PyTuple_GET_ITEM(pyargs, 2) : Py_None);
  const int64_t end_offset = argc > 3 ? PyObjToInt(PyTuple_GET_ITEM(pyargs, 3)) : -1;
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  int32_t num_shards = 0;
  if (tkrzw::ShardDBM::GetNumberOfShards(std::string(old_file_path.Get()), &num_shards) ==
      tkrzw::Status::SUCCESS) {
    NativeLock lock(true);
    status = tkrzw::ShardDBM::RestoreDatabase(
        std::string(old_file_path.Get()), std::string(new_file_path.Get()),
        std::string(class_name.Get()), end_offset);
  } else {
    NativeLock lock(true);
    status = tkrzw::PolyDBM::RestoreDatabase(
        std::string(old_file_path.Get()), std::string(new_file_path.Get()),
        std::string(class_name.Get()), end_offset);
  }
  return CreatePyTkStatus(status);
}

// Implementation of DBM#__len__.
static Py_ssize_t dbm_len(PyDBM* self) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return -1;
  }
  int64_t count = -1;
  {
    NativeLock lock(self->concurrent);
    count = self->dbm->CountSimple();
  }
  return count;
}

// Implementation of DBM#__getitem__.
static PyObject* dbm_getitem(PyDBM* self, PyObject* pykey) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
  const bool is_unicode = PyUnicode_Check(pykey);
  SoftString key(pykey);
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  std::string value;
  {
    NativeLock lock(self->concurrent);
    status = self->dbm->Get(key.Get(), &value);
  }
  if (status != tkrzw::Status::SUCCESS) {
    ThrowStatusException(status);
    return nullptr;
  }
  if (is_unicode) {
    return CreatePyString(value);
  }
  return CreatePyBytes(value);
}

// Implementation of DBM#__setitem__.
static int dbm_setitem(PyDBM* self, PyObject* pykey, PyObject* pyvalue) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return -1;
  }
  if (pyvalue) {
    SoftString key(pykey);
    SoftString value(pyvalue);
    tkrzw::Status status(tkrzw::Status::SUCCESS);
    {
      NativeLock lock(self->concurrent);
      status = self->dbm->Set(key.Get(), value.Get());
    }
    if (status != tkrzw::Status::SUCCESS) {
      ThrowStatusException(status);
      return -1;
    }
  } else {
    SoftString key(pykey);
    tkrzw::Status status(tkrzw::Status::SUCCESS);
    {
      NativeLock lock(self->concurrent);
      status = self->dbm->Remove(key.Get());
    }
    if (status != tkrzw::Status::SUCCESS) {
      ThrowStatusException(status);
      return -1;
    }
  }
  return 0;
}

// Implementation of DBM#__iter__.
static PyObject* dbm_iter(PyDBM* self) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
  PyTypeObject* pyitertype = (PyTypeObject*)cls_iter;
  PyIterator* pyiter = (PyIterator*)pyitertype->tp_alloc(pyitertype, 0);
  if (!pyiter) return nullptr;
  {
    NativeLock lock(self->concurrent);
    pyiter->iter = self->dbm->MakeIterator().release();
    pyiter->concurrent = self->concurrent;
    pyiter->iter->First();
  }
  return (PyObject*)pyiter;
}

// Defines the DBM class.
static bool DefineDBM() {
  static PyTypeObject type_dbm = {PyVarObject_HEAD_INIT(nullptr, 0)};
  const size_t zoff = offsetof(PyTypeObject, tp_name);
  std::memset((char*)&type_dbm + zoff, 0, sizeof(type_dbm) - zoff);
  type_dbm.tp_name = "tkrzw.DBM";
  type_dbm.tp_basicsize = sizeof(PyDBM);
  type_dbm.tp_itemsize = 0;
  type_dbm.tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
  type_dbm.tp_doc = "Polymorphic database manager.";
  type_dbm.tp_new = dbm_new;
  type_dbm.tp_dealloc = (destructor)dbm_dealloc;
  type_dbm.tp_init = (initproc)dbm_init;
  type_dbm.tp_repr = (unaryfunc)dbm_repr;
  type_dbm.tp_str = (unaryfunc)dbm_str;
  static PyMethodDef methods[] = {
    {"Open", (PyCFunction)dbm_Open, METH_VARARGS | METH_KEYWORDS,
     "Opens a database file."},
    {"Close", (PyCFunction)dbm_Close, METH_NOARGS,
     "Closes the database file."},
    {"Get", (PyCFunction)dbm_Get, METH_VARARGS,
     "Gets the value of a record of a key."},
    {"GetStr", (PyCFunction)dbm_GetStr, METH_VARARGS,
     "Gets the value of a record of a key, as a string."},
    {"GetMulti", (PyCFunction)dbm_GetMulti, METH_VARARGS,
     "Gets the values of multiple records of keys."},
    {"GetMultiStr", (PyCFunction)dbm_GetMultiStr, METH_VARARGS,
     "Gets the values of multiple records of keys, as strings."},
    {"Set", (PyCFunction)dbm_Set, METH_VARARGS,
     "Sets a record of a key and a value."},
    {"SetMulti", (PyCFunction)dbm_SetMulti, METH_VARARGS | METH_KEYWORDS,
     "Sets multiple records specified by an initializer list of pairs of strings."},
    {"SetAndGet", (PyCFunction)dbm_SetAndGet, METH_VARARGS,
     "Sets a record and get the old value."},
    {"Remove", (PyCFunction)dbm_Remove, METH_VARARGS,
     "Removes a record of a key."},
    {"RemoveMulti", (PyCFunction)dbm_RemoveMulti, METH_VARARGS,
     "Removes records of keys."},
    {"RemoveAndGet", (PyCFunction)dbm_RemoveAndGet, METH_VARARGS,
     "Removes a record and get the value."},
    {"Append", (PyCFunction)dbm_Append, METH_VARARGS,
     "Appends data at the end of a record of a key."},
    {"CompareExchange", (PyCFunction)dbm_CompareExchange, METH_VARARGS,
     "Compares the value of a record and exchanges if the condition meets."},
    {"Increment", (PyCFunction)dbm_Increment, METH_VARARGS,
     "Increments the numeric value of a record."},
    {"CompareExchangeMulti", (PyCFunction)dbm_CompareExchangeMulti, METH_VARARGS,
     "Compares the values of records and exchanges if the condition meets."},
    {"Count", (PyCFunction)dbm_Count, METH_NOARGS,
     "Gets the number of records."},
    {"GetFileSize", (PyCFunction)dbm_GetFileSize, METH_NOARGS,
     "Gets the current file size of the database."},
    {"GetFilePath", (PyCFunction)dbm_GetFilePath, METH_NOARGS,
     "Gets the path of the database file."},
    {"Clear", (PyCFunction)dbm_Clear, METH_NOARGS,
     "Removes all records."},
    {"Rebuild", (PyCFunction)dbm_Rebuild, METH_VARARGS | METH_KEYWORDS,
     "Rebuilds the entire database."},
    {"ShouldBeRebuilt", (PyCFunction)dbm_ShouldBeRebuilt, METH_NOARGS,
     "Checks whether the database should be rebuilt."},
    {"Synchronize", (PyCFunction)dbm_Synchronize, METH_VARARGS | METH_KEYWORDS,
     "Synchronizes the content of the database to the file system."},
    {"CopyFileData", (PyCFunction)dbm_CopyFileData, METH_VARARGS,
     "Copies the content of the database file to another file."},
    {"Export", (PyCFunction)dbm_Export, METH_VARARGS,
     "Exports all records to another database."},
    {"ExportKeysAsLines", (PyCFunction)dbm_ExportKeysAsLines, METH_VARARGS,
     "Exports the keys of all records as lines to a text file."},
    {"Inspect", (PyCFunction)dbm_Inspect, METH_NOARGS,
     "Inspects the database."},
    {"IsOpen", (PyCFunction)dbm_IsOpen, METH_NOARGS,
     "Checks whether the database is open."},
    {"IsHealthy", (PyCFunction)dbm_IsHealthy, METH_NOARGS,
     "Checks whether the database condition is healthy."},
    {"IsOrdered", (PyCFunction)dbm_IsOrdered, METH_NOARGS,
     "Checks whether ordered operations are supported."},
    {"Search", (PyCFunction)dbm_Search, METH_VARARGS,
     "Searches the database and get keys which match a pattern."},
    {"MakeIterator", (PyCFunction)dbm_MakeIterator, METH_NOARGS,
     "Makes an iterator for each record."},   
    {"RestoreDatabase", (PyCFunction)dbm_RestoreDatabase, METH_CLASS | METH_VARARGS,
     "Makes an iterator for each record."},   
    {nullptr, nullptr, 0, nullptr},
  };
  type_dbm.tp_methods = methods;
  static PyMappingMethods type_dbm_map;
  std::memset(&type_dbm_map, 0, sizeof(type_dbm_map));
  type_dbm_map.mp_length = (lenfunc)dbm_len;
  type_dbm_map.mp_subscript = (binaryfunc)dbm_getitem;
  type_dbm_map.mp_ass_subscript = (objobjargproc)dbm_setitem;
  type_dbm.tp_as_mapping = &type_dbm_map;
  type_dbm.tp_iter = (getiterfunc)dbm_iter;
  if (PyType_Ready(&type_dbm) != 0) return false;
  cls_dbm = (PyObject*)&type_dbm;
  Py_INCREF(cls_dbm);
  if (PyModule_AddObject(mod_tkrzw, "DBM", cls_dbm) != 0) return false;
  return true;
}

// Implementation of Iterator.new.
static PyObject* iter_new(PyTypeObject* pytype, PyObject* pyargs, PyObject* pykwds) {
  PyIterator* self = (PyIterator*)pytype->tp_alloc(pytype, 0);
  if (!self) return nullptr;
  self->iter = nullptr;
  self->concurrent = false;
  return (PyObject*)self;
}

// Implementation of Iterator#dealloc.
static void iter_dealloc(PyIterator* self) {
  delete self->iter;
  Py_TYPE(self)->tp_free((PyObject*)self);
}

// Implementation of Iterator#__init__.
static int iter_init(PyIterator* self, PyObject* pyargs, PyObject* pykwds) {
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc != 1) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return -1;
  }
  PyObject* pydbm_obj = PyTuple_GET_ITEM(pyargs, 0);
  if (!PyObject_IsInstance(pydbm_obj, cls_dbm)) {
    ThrowInvalidArguments("the argument is not a DBM");
    return -1;
  }
  PyDBM* pydbm = (PyDBM*)pydbm_obj;
  {
    NativeLock lock(pydbm->concurrent);
    self->iter = pydbm->dbm->MakeIterator().release();
  }
  self->concurrent = pydbm->concurrent;
  return 0;
}

// Implementation of Iterator#__repr__.
static PyObject* iter_repr(PyIterator* self) {
  std::string key;
  {
    NativeLock lock(self->concurrent);
    const tkrzw::Status status = self->iter->Get(&key);
    if (status != tkrzw::Status::SUCCESS) {
      key = "(unlocated)";
    }
  }
  return CreatePyString(tkrzw::StrCat("<tkrzw.Iterator: ", tkrzw::StrEscapeC(key, true), ">"));
}

// Implementation of Iterator#__str__.
static PyObject* iter_str(PyIterator* self) {
  std::string key;
  {
    NativeLock lock(self->concurrent);
    const tkrzw::Status status = self->iter->Get(&key);
    if (status != tkrzw::Status::SUCCESS) {
      key = "(unlocated)";
    }
  }
  return CreatePyString(tkrzw::StrEscapeC(key, true));
}

// Implementation of Iterator#First.
static PyObject* iter_First(PyIterator* self) {
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->iter->First();
  }
  return CreatePyTkStatus(status);
}

// Implementation of Iterator#Last.
static PyObject* iter_Last(PyIterator* self) {
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->iter->Last();
  }
  return CreatePyTkStatus(status);
}

// Implementation of Iterator#Jump.
static PyObject* iter_Jump(PyIterator* self, PyObject* pyargs) {
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc != 1) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pykey = PyTuple_GET_ITEM(pyargs, 0);
  SoftString key(pykey);
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->iter->Jump(key.Get());
  }
  return CreatePyTkStatus(status);
}

// Implementation of Iterator#JumpLower.
static PyObject* iter_JumpLower(PyIterator* self, PyObject* pyargs) {
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc < 1 || argc > 2) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pykey = PyTuple_GET_ITEM(pyargs, 0);
  const bool inclusive = argc > 1 ? PyObject_IsTrue(PyTuple_GET_ITEM(pyargs, 1)) : false;
  SoftString key(pykey);
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->iter->JumpLower(key.Get(), inclusive);
  }
  return CreatePyTkStatus(status);
}

// Implementation of Iterator#JumpUpper.
static PyObject* iter_JumpUpper(PyIterator* self, PyObject* pyargs) {
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc < 1 || argc > 2) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pykey = PyTuple_GET_ITEM(pyargs, 0);
  const bool inclusive = argc > 1 ? PyObject_IsTrue(PyTuple_GET_ITEM(pyargs, 1)) : false;
  SoftString key(pykey);
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->iter->JumpUpper(key.Get(), inclusive);
  }
  return CreatePyTkStatus(status);
}

// Implementation of Iterator#Next.
static PyObject* iter_Next(PyIterator* self) {
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->iter->Next();
  }
  return CreatePyTkStatus(status);
}

// Implementation of Iterator#Previous.
static PyObject* iter_Previous(PyIterator* self) {
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->iter->Previous();
  }
  return CreatePyTkStatus(status);
}

// Implementation of Iterator#Get.
static PyObject* iter_Get(PyIterator* self, PyObject* pyargs) {
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc > 1) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pystatus = nullptr;
  if (argc > 0) {
    pystatus = PyTuple_GET_ITEM(pyargs, 0);
    if (pystatus == Py_None) {
      pystatus = nullptr;
    } else if (!PyObject_IsInstance(pystatus, cls_status)) {
      ThrowInvalidArguments("not a status object");
      return nullptr;
    }
  }  
  std::string key, value;
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->iter->Get(&key, &value);
  }
  if (pystatus != nullptr) {
    *((PyTkStatus*)pystatus)->status = status;
  }
  if (status == tkrzw::Status::SUCCESS) {
    PyObject* pykey = CreatePyBytes(key);
    PyObject* pyvalue = CreatePyBytes(value);
    PyObject * pyrv = PyTuple_Pack(2, pykey, pyvalue);
    Py_DECREF(pyvalue);
    Py_DECREF(pykey);
    return pyrv;
  }
  Py_RETURN_NONE;
}

// Implementation of Iterator#GetStr.
static PyObject* iter_GetStr(PyIterator* self, PyObject* pyargs) {
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc > 1) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pystatus = nullptr;
  if (argc > 0) {
    pystatus = PyTuple_GET_ITEM(pyargs, 0);
    if (pystatus == Py_None) {
      pystatus = nullptr;
    } else if (!PyObject_IsInstance(pystatus, cls_status)) {
      ThrowInvalidArguments("not a status object");
      return nullptr;
    }
  }  
  std::string key, value;
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->iter->Get(&key, &value);
  }
  if (pystatus != nullptr) {
    *((PyTkStatus*)pystatus)->status = status;
  }
  if (status == tkrzw::Status::SUCCESS) {
    PyObject* pykey = CreatePyString(key);
    PyObject* pyvalue = CreatePyString(value);
    PyObject * pyrv = PyTuple_Pack(2, pykey, pyvalue);
    Py_DECREF(pyvalue);
    Py_DECREF(pykey);
    return pyrv;
  }
  Py_RETURN_NONE;
}

// Implementation of Iterator#GetKey.
static PyObject* iter_GetKey(PyIterator* self, PyObject* pyargs) {
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc > 1) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pystatus = nullptr;
  if (argc > 0) {
    pystatus = PyTuple_GET_ITEM(pyargs, 0);
    if (pystatus == Py_None) {
      pystatus = nullptr;
    } else if (!PyObject_IsInstance(pystatus, cls_status)) {
      ThrowInvalidArguments("not a status object");
      return nullptr;
    }
  }
  std::string key;
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->iter->Get(&key);
  }
  if (pystatus != nullptr) {
    *((PyTkStatus*)pystatus)->status = status;
  }
  if (status != tkrzw::Status::SUCCESS) {
    Py_RETURN_NONE;
  }
  return CreatePyBytes(key);
}

// Implementation of Iterator#GetKeyStr.
static PyObject* iter_GetKeyStr(PyIterator* self, PyObject* pyargs) {
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc > 1) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pystatus = nullptr;
  if (argc > 0) {
    pystatus = PyTuple_GET_ITEM(pyargs, 0);
    if (pystatus == Py_None) {
      pystatus = nullptr;
    } else if (!PyObject_IsInstance(pystatus, cls_status)) {
      ThrowInvalidArguments("not a status object");
      return nullptr;
    }
  }
  std::string key;
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->iter->Get(&key);
  }
  if (pystatus != nullptr) {
    *((PyTkStatus*)pystatus)->status = status;
  }
  if (status != tkrzw::Status::SUCCESS) {
    Py_RETURN_NONE;
  }
  return CreatePyString(key);
}

// Implementation of Iterator#GetValue.
static PyObject* iter_GetValue(PyIterator* self, PyObject* pyargs) {
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc > 1) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pystatus = nullptr;
  if (argc > 0) {
    pystatus = PyTuple_GET_ITEM(pyargs, 0);
    if (pystatus == Py_None) {
      pystatus = nullptr;
    } else if (!PyObject_IsInstance(pystatus, cls_status)) {
      ThrowInvalidArguments("not a status object");
      return nullptr;
    }
  }
  std::string value;
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->iter->Get(nullptr, &value);
  }
  if (pystatus != nullptr) {
    *((PyTkStatus*)pystatus)->status = status;
  }
  if (status != tkrzw::Status::SUCCESS) {
    Py_RETURN_NONE;
  }
  return CreatePyBytes(value);
}

// Implementation of Iterator#GetValueStr.
static PyObject* iter_GetValueStr(PyIterator* self, PyObject* pyargs) {
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc > 1) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pystatus = nullptr;
  if (argc > 0) {
    pystatus = PyTuple_GET_ITEM(pyargs, 0);
    if (pystatus == Py_None) {
      pystatus = nullptr;
    } else if (!PyObject_IsInstance(pystatus, cls_status)) {
      ThrowInvalidArguments("not a status object");
      return nullptr;
    }
  }
  std::string value;
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->iter->Get(nullptr, &value);
  }
  if (pystatus != nullptr) {
    *((PyTkStatus*)pystatus)->status = status;
  }
  if (status != tkrzw::Status::SUCCESS) {
    Py_RETURN_NONE;
  }
  return CreatePyString(value);
}

// Implementation of Iterator#Set.
static PyObject* iter_Set(PyIterator* self, PyObject* pyargs) {
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc != 1) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pyvalue = PyTuple_GET_ITEM(pyargs, 0);
  SoftString value(pyvalue);
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->iter->Set(value.Get());
  }
  return CreatePyTkStatus(status);
}

// Implementation of Iterator#Remove.
static PyObject* iter_Remove(PyIterator* self) {
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->iter->Remove();
  }
  return CreatePyTkStatus(status);
}

// Implementation of Iterator#__next__.
static PyObject* iter_iternext(PyIterator* self) {
  std::string key, value;
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->iter->Get(&key, &value);
  }
  PyObject* pyrv = nullptr;
  if (status == tkrzw::Status::SUCCESS) {
    PyObject* pykey = CreatePyBytes(key);
    PyObject* pyvalue = CreatePyBytes(value);
    pyrv = PyTuple_Pack(2, pykey, pyvalue);
    Py_DECREF(pykey);
    Py_DECREF(pyvalue);
    self->iter->Next();
  } else {
    PyErr_SetString(PyExc_StopIteration, "end of iteration");
    pyrv = nullptr;
  }
  return pyrv;
}

// Defines the Iterator class.
static bool DefineIterator() {
  static PyTypeObject type_iter = {PyVarObject_HEAD_INIT(nullptr, 0)};
  const size_t zoff = offsetof(PyTypeObject, tp_name);
  std::memset((char*)&type_iter + zoff, 0, sizeof(type_iter) - zoff);
  type_iter.tp_name = "tkrzw.Iterator";
  type_iter.tp_basicsize = sizeof(PyIterator);
  type_iter.tp_itemsize = 0;
  type_iter.tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
  type_iter.tp_doc = "Iterator for each record.";
  type_iter.tp_new = iter_new;
  type_iter.tp_dealloc = (destructor)iter_dealloc;
  type_iter.tp_init = (initproc)iter_init;
  type_iter.tp_repr = (unaryfunc)iter_repr;
  type_iter.tp_str = (unaryfunc)iter_str;
  static PyMethodDef methods[] = {
    {"First", (PyCFunction)iter_First, METH_NOARGS,
     "Initializes the iterator to indicate the first record."},
    {"Last", (PyCFunction)iter_Last, METH_NOARGS,
     "Initializes the iterator to indicate the last record."},
    {"Jump", (PyCFunction)iter_Jump, METH_VARARGS,
     "Initializes the iterator to indicate a specific record."},
    {"JumpLower", (PyCFunction)iter_JumpLower, METH_VARARGS,
     "Initializes the iterator to indicate the last record whose key is lower."},
    {"JumpUpper", (PyCFunction)iter_JumpUpper, METH_VARARGS,
     "Initializes the iterator to indicate the first record whose key is upper."},
    {"Next", (PyCFunction)iter_Next, METH_NOARGS,
     "Moves the iterator to the next record."},
    {"Previous", (PyCFunction)iter_Previous, METH_NOARGS,
     "Moves the iterator to the previous record."},
    {"Get", (PyCFunction)iter_Get, METH_VARARGS,
     "Gets the key and the value of the current record of the iterator."},
    {"GetStr", (PyCFunction)iter_GetStr, METH_VARARGS,
     "Gets the key and the value of the current record of the iterator, as strings."},
    {"GetKey", (PyCFunction)iter_GetKey, METH_VARARGS,
     "Gets the key of the current record."},
    {"GetKeyStr", (PyCFunction)iter_GetKeyStr, METH_VARARGS,
     "Gets the key of the current record, as a string."},
    {"GetValue", (PyCFunction)iter_GetValue, METH_VARARGS,
     "Gets the value of the current record."},
    {"GetValueStr", (PyCFunction)iter_GetValueStr, METH_VARARGS,
     "Gets the value of the current record, as a string."},
    {"Set", (PyCFunction)iter_Set, METH_VARARGS,
     "Sets the value of the current record."},
    {"Remove", (PyCFunction)iter_Remove, METH_NOARGS,
     "Removes the current record."},
    {nullptr, nullptr, 0, nullptr}
  };
  type_iter.tp_methods = methods;
  type_iter.tp_iternext = (iternextfunc)iter_iternext;
  if (PyType_Ready(&type_iter) != 0) return false;
  cls_iter = (PyObject*)&type_iter;
  Py_INCREF(cls_iter);
  if (PyModule_AddObject(mod_tkrzw, "Iterator", cls_iter) != 0) return false;
  return true;
}

// Implementation of File.new.
static PyObject* file_new(PyTypeObject* pytype, PyObject* pyargs, PyObject* pykwds) {
  PyFile* self = (PyFile*)pytype->tp_alloc(pytype, 0);
  if (!self) return nullptr;
  self->file = new tkrzw::PolyFile();
  self->concurrent = false;
  return (PyObject*)self;
}

// Implementation of File#dealloc.
static void file_dealloc(PyFile* self) {
  delete self->file;
  Py_TYPE(self)->tp_free((PyObject*)self);
}

// Implementation of File#__init__.
static int file_init(PyFile* self, PyObject* pyargs, PyObject* pykwds) {
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc != 0) {
    ThrowInvalidArguments("too many arguments");
    return -1;
  }
  return 0;
}

// Implementation of File#__repr__.
static PyObject* file_repr(PyFile* self) {
  return CreatePyString("<tkrzw.File>");
}

// Implementation of File#__str__.
static PyObject* file_str(PyFile* self) {
  return CreatePyString("(File)");
}

// Implementation of File#Open.
static PyObject* file_Open(PyFile* self, PyObject* pyargs, PyObject* pykwds) {
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc != 2) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pypath = PyTuple_GET_ITEM(pyargs, 0);
  PyObject* pywritable = PyTuple_GET_ITEM(pyargs, 1);
  SoftString path(pypath);
  const bool writable = PyObject_IsTrue(pywritable);
  bool concurrent = false;
  int32_t open_options = 0;
  std::map<std::string, std::string> params;
  if (pykwds != nullptr) {
    params = MapKeywords(pykwds);
    if (tkrzw::StrToBool(tkrzw::SearchMap(params, "concurrent", "false"))) {
      concurrent = true;
    }
    if (tkrzw::StrToBool(tkrzw::SearchMap(params, "truncate", "false"))) {
      open_options |= tkrzw::File::OPEN_TRUNCATE;
    }
    if (tkrzw::StrToBool(tkrzw::SearchMap(params, "no_create", "false"))) {
      open_options |= tkrzw::File::OPEN_NO_CREATE;
    }
    if (tkrzw::StrToBool(tkrzw::SearchMap(params, "no_wait", "false"))) {
      open_options |= tkrzw::File::OPEN_NO_WAIT;
    }
    if (tkrzw::StrToBool(tkrzw::SearchMap(params, "no_lock", "false"))) {
      open_options |= tkrzw::File::OPEN_NO_LOCK;
    }
  }
  self->concurrent = concurrent;
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->file->OpenAdvanced(std::string(path.Get()), writable, open_options, params);
  }
  return CreatePyTkStatus(status);
}

// Implementation of File#Close
static PyObject* file_Close(PyFile* self) {
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->file->Close();
  }
  return CreatePyTkStatus(status);
}

// Implementation of File#Search.
static PyObject* file_Search(PyFile* self, PyObject* pyargs) {
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc < 2 || argc > 4) {
    ThrowInvalidArguments(argc < 2 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pymode = PyTuple_GET_ITEM(pyargs, 0);
  PyObject* pypattern = PyTuple_GET_ITEM(pyargs, 1);
  int32_t capacity = 0;
  if (argc > 2) {
    capacity = PyObjToInt(PyTuple_GET_ITEM(pyargs, 2));
  }
  bool utf = false;
  if (argc > 3) {
    utf = PyObject_IsTrue(PyTuple_GET_ITEM(pyargs, 3));
  }
  SoftString pattern(pypattern);
  SoftString mode(pymode);
  std::vector<std::string> lines;
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = tkrzw::SearchTextFileModal(
        self->file, mode.Get(), pattern.Get(), &lines, capacity, utf);
  }
  if (status != tkrzw::Status::SUCCESS) {
    ThrowStatusException(status);    
    return nullptr;
  }
  PyObject* pyrv = PyList_New(lines.size());
  for (size_t i = 0; i < lines.size(); i++) {
    PyList_SET_ITEM(pyrv, i, CreatePyString(lines[i]));
  }
  return pyrv;
}

// Defines the File class.
static bool DefineFile() {
  static PyTypeObject type_file = {PyVarObject_HEAD_INIT(nullptr, 0)};
  const size_t zoff = offsetof(PyTypeObject, tp_name);
  std::memset((char*)&type_file + zoff, 0, sizeof(type_file) - zoff);
  type_file.tp_name = "tkrzw.File";
  type_file.tp_basicsize = sizeof(PyFile);
  type_file.tp_itemsize = 0;
  type_file.tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
  type_file.tp_doc = "Text file of line data.";
  type_file.tp_new = file_new;
  type_file.tp_dealloc = (destructor)file_dealloc;
  type_file.tp_init = (initproc)file_init;
  type_file.tp_repr = (unaryfunc)file_repr;
  type_file.tp_str = (unaryfunc)file_str;
  static PyMethodDef methods[] = {
    {"Open", (PyCFunction)file_Open, METH_VARARGS | METH_KEYWORDS,
     "Opens a text file."},
    {"Close", (PyCFunction)file_Close, METH_NOARGS,
     "Closes the text file."},
    {"Search", (PyCFunction)file_Search, METH_VARARGS,
     "Searches the text file and get lines which match a pattern."},
    {nullptr, nullptr, 0, nullptr}
  };
  type_file.tp_methods = methods;
  if (PyType_Ready(&type_file) != 0) return false;
  cls_file = (PyObject*)&type_file;
  Py_INCREF(cls_file);
  if (PyModule_AddObject(mod_tkrzw, "File", cls_file) != 0) return false;
  return true;
}

// Entry point of the library.
PyMODINIT_FUNC PyInit_tkrzw() {
  if (!DefineModule()) return nullptr;
  if (!DefineUtility()) return nullptr;
  if (!DefineStatus()) return nullptr;
  if (!DefineStatusException()) return nullptr;
  if (!DefineDBM()) return nullptr;
  if (!DefineIterator()) return nullptr;
  if (!DefineFile()) return nullptr;
  return mod_tkrzw;
}

}  // extern "C"

// END OF FILE
