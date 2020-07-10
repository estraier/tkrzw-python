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
#include "tkrzw_dbm_poly.h"
#include "tkrzw_dbm_shard.h"
#include "tkrzw_file.h"
#include "tkrzw_file_mmap.h"
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
PyObject* cls_textfile;
PyObject* obj_proc_noop;
PyObject* obj_proc_remove;

// Python object of Utility.
struct PyUtility {
  PyObject_HEAD
};

// Python object of Status.
struct PyStatus {
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

// Python object of TextFile.
struct PyTextFile {
  PyObject_HEAD
  tkrzw::File* file;
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
static PyObject* CreatePyStatus(const tkrzw::Status& status) {
  PyTypeObject* pytype = (PyTypeObject*)cls_status;
  PyStatus* obj = (PyStatus*)pytype->tp_alloc(pytype, 0);
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
  PyObject* pystatus = CreatePyStatus(status);
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

// Implementation of Utility.GetMemoryUsage.
static PyObject* utility_GetMemoryUsage(PyObject* self) {
  const std::map<std::string, std::string> records = tkrzw::GetSystemInfo();
  return PyLong_FromLongLong(tkrzw::StrToInt(tkrzw::SearchMap(records, "mem_rss", "-1")));
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
  if (PyModule_AddObject(mod_tkrzw, "Utility", cls_utility) != 0) return false;
  return true;
}

// Implementation of Status.new.
static PyObject* status_new(PyTypeObject* pytype, PyObject* pyargs, PyObject* pykwds) {
  PyStatus* self = (PyStatus*)pytype->tp_alloc(pytype, 0);
  if (!self) return nullptr;
  self->status = new tkrzw::Status();
  return (PyObject*)self;
}

// Implementation of Status#dealloc.
static void status_dealloc(PyStatus* self) {
  delete self->status;
  Py_TYPE(self)->tp_free((PyObject*)self);
}

// Implementation of Status#__init__.
static int status_init(PyStatus* self, PyObject* pyargs, PyObject* pykwds) {
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
static PyObject* status_repr(PyStatus* self) {
  return CreatePyString(tkrzw::StrCat("<tkrzw.Status: ", *self->status, ">"));
}

// Implementation of Status#__str__.
static PyObject* status_str(PyStatus* self) {
  return CreatePyString(tkrzw::ToString(*self->status));
}

// Implementation of Status#__richcmp__.
static PyObject* status_richcmp(PyStatus* self, PyObject* pyrhs, int op) {
  bool rv = false;
  int32_t code = (int32_t)self->status->GetCode();
  int32_t rcode = 0;
  if (PyObject_IsInstance(pyrhs, cls_status)) {
    PyStatus* pyrhs_status = (PyStatus*)pyrhs;
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
static PyObject* status_Set(PyStatus* self, PyObject* pyargs) {
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
static PyObject* status_GetCode(PyStatus* self) {
  return PyLong_FromLongLong(self->status->GetCode());
}

// Implementation of Status#GetMessage.
static PyObject* status_GetMessage(PyStatus* self) {
  return PyUnicode_FromString(self->status->GetMessage().c_str());
}

// Implementation of Status#IsOK.
static PyObject* status_IsOK(PyStatus* self) {
  if (*self->status == tkrzw::Status::SUCCESS) {
    Py_RETURN_TRUE;
  }
  Py_RETURN_FALSE;
}

// Implementation of Status#OrDie.
static PyObject* status_OrDie(PyStatus* self) {
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
  type_status.tp_basicsize = sizeof(PyStatus);
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
  const tkrzw::Status* status = ((PyStatus*)self->pystatus)->status;
  return CreatePyString(tkrzw::StrCat("<tkrzw.StatusException: ", *status, ">"));
}

// Implementation of StatusException#__str__.
static PyObject* expt_str(PyException* self) {
  const tkrzw::Status* status = ((PyStatus*)self->pystatus)->status;
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
    params.erase("num_shards");
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
  return CreatePyStatus(status);
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
  return CreatePyStatus(status);
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
    *((PyStatus*)pystatus)->status = status;
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
    *((PyStatus*)pystatus)->status = status;
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
  return CreatePyStatus(status);
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
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    for (const auto& record : records) {
      status |= self->dbm->Set(record.first, record.second);
    }
  }
  return CreatePyStatus(status);
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
  return CreatePyStatus(status);
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
  return CreatePyStatus(status);
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
  SoftString expected(pyexpected);
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  if (pydesired == Py_None) {
    std::string_view desired;
    NativeLock lock(self->concurrent);
    status = self->dbm->CompareExchange(key.Get(), expected.Get(), desired);
  } else {
    SoftString desired(pydesired);
    NativeLock lock(self->concurrent);
    status = self->dbm->CompareExchange(key.Get(), expected.Get(), desired.Get());
  }
  return CreatePyStatus(status);
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
    *((PyStatus*)pystatus)->status = status;
  }
  if (status == tkrzw::Status::SUCCESS) {
    return PyLong_FromLongLong(current);
  }
  Py_RETURN_NONE;
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
  return CreatePyStatus(status);
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
  return CreatePyStatus(status);
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
  return CreatePyStatus(status);
}

// Implementation of DBM#CopyFile.
static PyObject* dbm_CopyFile(PyDBM* self, PyObject* pyargs) {
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
    status = self->dbm->CopyFile(std::string(dest.Get()));
  }
  return CreatePyStatus(status);
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
  return CreatePyStatus(status);
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
  return CreatePyStatus(status);
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
  if (mode.Get() == "contain") {
    NativeLock lock(self->concurrent);
    status = tkrzw::SearchDBM(self->dbm, pattern.Get(), &keys, capacity, tkrzw::StrContains);
  } else if (mode.Get() == "begin") {
    NativeLock lock(self->concurrent);
    status = tkrzw::SearchDBMForwardMatch(self->dbm, pattern.Get(), &keys, capacity);
  } else if (mode.Get() == "end") {
    NativeLock lock(self->concurrent);
    status = tkrzw::SearchDBM(self->dbm, pattern.Get(), &keys, capacity, tkrzw::StrEndsWith);
  } else if (mode.Get() == "regex") {
    NativeLock lock(self->concurrent);
    status = tkrzw::SearchDBMRegex(self->dbm, pattern.Get(), &keys, capacity, utf);
  } else if (mode.Get() == "edit") {
    NativeLock lock(self->concurrent);
    status = tkrzw::SearchDBMEditDistance(self->dbm, pattern.Get(), &keys, capacity, utf);
  } else {
    ThrowInvalidArguments("unknown mode");
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
    {"Remove", (PyCFunction)dbm_Remove, METH_VARARGS,
     "Removes a record of a key."},
    {"Append", (PyCFunction)dbm_Append, METH_VARARGS,
     "Appends data at the end of a record of a key."},
    {"CompareExchange", (PyCFunction)dbm_CompareExchange, METH_VARARGS,
     "Compares the value of a record and exchanges if the condition meets."},
    {"Increment", (PyCFunction)dbm_Increment, METH_VARARGS,
     "Increments the numeric value of a record."},
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
    {"CopyFile", (PyCFunction)dbm_CopyFile, METH_VARARGS,
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
  return CreatePyStatus(status);
}

// Implementation of Iterator#Last.
static PyObject* iter_Last(PyIterator* self) {
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->iter->Last();
  }
  return CreatePyStatus(status);
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
  return CreatePyStatus(status);
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
  return CreatePyStatus(status);
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
  return CreatePyStatus(status);
}

// Implementation of Iterator#Next.
static PyObject* iter_Next(PyIterator* self) {
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->iter->Next();
  }
  return CreatePyStatus(status);
}

// Implementation of Iterator#Previous.
static PyObject* iter_Previous(PyIterator* self) {
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->iter->Previous();
  }
  return CreatePyStatus(status);
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
    *((PyStatus*)pystatus)->status = status;
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
    *((PyStatus*)pystatus)->status = status;
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
    *((PyStatus*)pystatus)->status = status;
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
    *((PyStatus*)pystatus)->status = status;
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
    *((PyStatus*)pystatus)->status = status;
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
    *((PyStatus*)pystatus)->status = status;
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
  return CreatePyStatus(status);
}

// Implementation of Iterator#Remove.
static PyObject* iter_Remove(PyIterator* self) {
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->iter->Remove();
  }
  return CreatePyStatus(status);
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

// Implementation of Textfile.new.
static PyObject* textfile_new(PyTypeObject* pytype, PyObject* pyargs, PyObject* pykwds) {
  PyTextFile* self = (PyTextFile*)pytype->tp_alloc(pytype, 0);
  if (!self) return nullptr;
  self->file = new tkrzw::MemoryMapParallelFile();
  return (PyObject*)self;
}

// Implementation of TextFile#dealloc.
static void textfile_dealloc(PyTextFile* self) {
  delete self->file;
  Py_TYPE(self)->tp_free((PyObject*)self);
}

// Implementation of TextFile#__init__.
static int textfile_init(PyTextFile* self, PyObject* pyargs, PyObject* pykwds) {
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc != 0) {
    ThrowInvalidArguments("too many arguments");
    return -1;
  }
  return 0;
}

// Implementation of TextFile#__repr__.
static PyObject* textfile_repr(PyTextFile* self) {
  return CreatePyString("<tkrzw.TextFile>");
}

// Implementation of TextFile#__str__.
static PyObject* textfile_str(PyTextFile* self) {
  return CreatePyString("(TextFile)");
}

// Implementation of TextFile#Open.
static PyObject* textfile_Open(PyTextFile* self, PyObject* pyargs) {
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc != 1) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pypath = PyTuple_GET_ITEM(pyargs, 0);
  SoftString path(pypath);
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(true);
    status = self->file->Open(std::string(path.Get()), false);
  }
  return CreatePyStatus(status);
}

// Implementation of TextFile#Close
static PyObject* textfile_Close(PyTextFile* self) {
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(true);
    status = self->file->Close();
  }
  return CreatePyStatus(status);
}

// Implementation of TextFile#Search.
static PyObject* textfile_Search(PyTextFile* self, PyObject* pyargs) {
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
  if (mode.Get() == "contain") {
    NativeLock lock(true);
    status = tkrzw::SearchTextFile(
        self->file, pattern.Get(), &keys, capacity, tkrzw::StrContains);
  } else if (mode.Get() == "begin") {
    NativeLock lock(true);
    status = tkrzw::SearchTextFile(
        self->file, pattern.Get(), &keys, capacity, tkrzw::StrBeginsWith);
  } else if (mode.Get() == "end") {
    NativeLock lock(true);
    status = tkrzw::SearchTextFile(
        self->file, pattern.Get(), &keys, capacity, tkrzw::StrEndsWith);
  } else if (mode.Get() == "regex") {
    NativeLock lock(true);
    status = tkrzw::SearchTextFileRegex(
        self->file, pattern.Get(), &keys, capacity, utf);
  } else if (mode.Get() == "edit") {
    NativeLock lock(true);
    status = tkrzw::SearchTextFileEditDistance(
        self->file, pattern.Get(), &keys, capacity, utf);
  } else {
    ThrowInvalidArguments("unknown mode");
    return nullptr;
  }
  PyObject* pyrv = PyList_New(keys.size());
  for (size_t i = 0; i < keys.size(); i++) {
    PyList_SET_ITEM(pyrv, i, CreatePyString(keys[i]));
  }
  return pyrv;
}

// Defines the TextFile class.
static bool DefineTextFile() {
  static PyTypeObject type_textfile = {PyVarObject_HEAD_INIT(nullptr, 0)};
  const size_t zoff = offsetof(PyTypeObject, tp_name);
  std::memset((char*)&type_textfile + zoff, 0, sizeof(type_textfile) - zoff);
  type_textfile.tp_name = "tkrzw.TextFile";
  type_textfile.tp_basicsize = sizeof(PyTextFile);
  type_textfile.tp_itemsize = 0;
  type_textfile.tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
  type_textfile.tp_doc = "Text file of line data.";
  type_textfile.tp_new = textfile_new;
  type_textfile.tp_dealloc = (destructor)textfile_dealloc;
  type_textfile.tp_init = (initproc)textfile_init;
  type_textfile.tp_repr = (unaryfunc)textfile_repr;
  type_textfile.tp_str = (unaryfunc)textfile_str;
  static PyMethodDef methods[] = {
    {"Open", (PyCFunction)textfile_Open, METH_VARARGS,
     "Opens a text file."},
    {"Close", (PyCFunction)textfile_Close, METH_NOARGS,
     "Closes the text file."},
    {"Search", (PyCFunction)textfile_Search, METH_VARARGS,
     "Searches the text file and get lines which match a pattern."},
    {nullptr, nullptr, 0, nullptr}
  };
  type_textfile.tp_methods = methods;

  if (PyType_Ready(&type_textfile) != 0) return false;
  cls_textfile = (PyObject*)&type_textfile;
  Py_INCREF(cls_textfile);
  if (PyModule_AddObject(mod_tkrzw, "TextFile", cls_textfile) != 0) return false;
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
  if (!DefineTextFile()) return nullptr;
  return mod_tkrzw;
}

}  // extern "C"

// END OF FILE
