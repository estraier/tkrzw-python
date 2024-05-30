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
PyObject* cls_future;
PyObject* cls_dbm;
PyObject* cls_iter;
PyObject* cls_asyncdbm;
PyObject* cls_file;
PyObject* cls_index;
PyObject* cls_indexiter;
PyObject* obj_dbm_any_data;

// Python object of Utility.
struct PyUtility {
  PyObject_HEAD
};

// Python object of Status.
struct PyTkStatus {
  PyObject_HEAD
  tkrzw::Status* status;
};

// Python object of Future.
struct PyFuture {
  PyObject_HEAD
  tkrzw::StatusFuture* future;
  bool concurrent;
  bool is_str;
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

// Python object of AsyncDBM.
struct PyAsyncDBM {
  PyObject_HEAD
  tkrzw::AsyncDBM* async;
  bool concurrent;
};

// Python object of File.
struct PyFile {
  PyObject_HEAD
  tkrzw::PolyFile* file;
  bool concurrent;
};

// Python object of Index.
struct PyIndex {
  PyObject_HEAD
  tkrzw::PolyIndex* index;
  bool concurrent;
};

// Python object of IndexIterator.
struct PyIndexIterator {
  PyObject_HEAD
  tkrzw::PolyIndex::Iterator* iter;
  bool concurrent;
};

// Creates a new string of Python.
static PyObject* CreatePyString(std::string_view str) {
  return PyUnicode_DecodeUTF8(str.data(), str.size(), "replace");
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

// Creates a status object of Python, in moving context.
static PyObject* CreatePyTkStatusMove(tkrzw::Status&& status) {
  PyTypeObject* pytype = (PyTypeObject*)cls_status;
  PyTkStatus* obj = (PyTkStatus*)pytype->tp_alloc(pytype, 0);
  if (!obj) return nullptr;
  obj->status = new tkrzw::Status(std::move(status));
  return (PyObject*)obj;
}

// Creates a status future object of Python, in moving context.
static PyObject* CreatePyFutureMove(
    tkrzw::StatusFuture&& future, bool concurrent, bool is_str = false) {
  PyTypeObject* pytype = (PyTypeObject*)cls_future;
  PyFuture* obj = (PyFuture*)pytype->tp_alloc(pytype, 0);
  if (!obj) return nullptr;
  obj->future = new tkrzw::StatusFuture(std::move(future));
  obj->concurrent = concurrent;
  obj->is_str = is_str;
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

  void Release() {
    if (thstate_) {
      PyEval_RestoreThread(thstate_);
    }
    thstate_ = nullptr;
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
      num = PyLong_AsLong(pylong);
      Py_DECREF(pylong);
    }
    return num;
  }
  return 0;
}

// Converts a numeric parameter to a double.
static double PyObjToDouble(PyObject* pyobj) {
  if (PyLong_Check(pyobj)) {
    return PyLong_AsLong(pyobj);
  } else if (PyFloat_Check(pyobj)) {
    return PyFloat_AsDouble(pyobj);
  } else if (PyUnicode_Check(pyobj) || PyBytes_Check(pyobj)) {
    SoftString str(pyobj);
    return tkrzw::StrToDouble(str.Get());
  } else if (pyobj != Py_None) {
    double num = 0;
    PyObject* pyfloat = PyNumber_Float(pyobj);
    if (pyfloat) {
      num = PyFloat_AsDouble(pyfloat);
      Py_DECREF(pyfloat);
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


// Extracts a list of pairs of string views and functions from a sequence object.
std::vector<std::pair<std::string, std::shared_ptr<tkrzw::DBM::RecordProcessor>>> ExtractKFPairs(
    PyObject* pyseq) {
  std::vector<std::pair<std::string, std::shared_ptr<tkrzw::DBM::RecordProcessor>>> result;
  const size_t size = PySequence_Size(pyseq);
  result.reserve(size);
  for (size_t i = 0; i < size; i++) {
    PyObject* pypair = PySequence_GetItem(pyseq, i);
    if (PySequence_Check(pypair) && PySequence_Size(pypair) >= 2) {
      PyObject* pykey = PySequence_GetItem(pypair, 0);
      PyObject* pyfunc = PySequence_GetItem(pypair, 1);
      if (PyCallable_Check(pyfunc)) {
        SoftString key(pykey);
        class Processor final : public tkrzw::DBM::RecordProcessor {
         public:
          Processor(PyObject* pyfunc) : pyfunc_(pyfunc) {
            Py_INCREF(pyfunc_);
          }
          ~Processor() {
            Py_DECREF(pyfunc_);
          }
          std::string_view ProcessFull(std::string_view key, std::string_view value) override {
            PyObject* pyfuncargs = PyTuple_New(2);
            PyTuple_SET_ITEM(pyfuncargs, 0, CreatePyBytes(key));
            PyTuple_SET_ITEM(pyfuncargs, 1, CreatePyBytes(value));
            PyObject* pyfuncrv = PyObject_CallObject(pyfunc_, pyfuncargs);
            std::string_view funcrv = tkrzw::DBM::RecordProcessor::NOOP;
            if (pyfuncrv != nullptr) {
              if (pyfuncrv == Py_None) {
                funcrv = tkrzw::DBM::RecordProcessor::NOOP;
              } else if (pyfuncrv == Py_False) {
                funcrv = tkrzw::DBM::RecordProcessor::REMOVE;
              } else {
                funcrvstr_ = std::make_unique<SoftString>(pyfuncrv);
                funcrv = funcrvstr_->Get();
              }
              Py_DECREF(pyfuncrv);
            }
            Py_DECREF(pyfuncargs);
            return funcrv;
          }
          std::string_view ProcessEmpty(std::string_view key) override {
            PyObject* pyfuncargs = PyTuple_New(2);
            PyTuple_SET_ITEM(pyfuncargs, 0, CreatePyBytes(key));
            Py_INCREF(Py_None);
            PyTuple_SET_ITEM(pyfuncargs, 1, Py_None);
            PyObject* pyfuncrv = PyObject_CallObject(pyfunc_, pyfuncargs);
            std::string_view funcrv = tkrzw::DBM::RecordProcessor::NOOP;
            if (pyfuncrv != nullptr) {
              if (pyfuncrv == Py_None) {
                funcrv = tkrzw::DBM::RecordProcessor::NOOP;
              } else if (pyfuncrv == Py_False) {
                funcrv = tkrzw::DBM::RecordProcessor::REMOVE;
              } else {
                funcrvstr_ = std::make_unique<SoftString>(pyfuncrv);
                funcrv = funcrvstr_->Get();
              }
              Py_DECREF(pyfuncrv);
            }
            Py_DECREF(pyfuncargs);
            return funcrv;
          }
         private:
          PyObject* pyfunc_;
          std::unique_ptr<SoftString> funcrvstr_;
        };
        auto proc = std::make_shared<Processor>(pyfunc);
        result.emplace_back(std::make_pair(key.Get(), proc));
      }
      Py_DECREF(pyfunc);
      Py_DECREF(pykey);
    }
    Py_DECREF(pypair);
  }
  return result;
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
          if (pyvalue == obj_dbm_any_data) {
            value_view = tkrzw::DBM::ANY_DATA;
          } else {
            SoftString value(pyvalue);
            placeholder->emplace_back(std::string(value.Get()));
            value_view = placeholder->back();
          }
        }
        result.emplace_back(std::make_pair(key_view, value_view));
      }
      Py_DECREF(pyvalue);
      Py_DECREF(pykey);
    }
    Py_DECREF(pypair);
  }
  return result;
}

// Defines the module.
static bool DefineModule() {
  static PyModuleDef module_def = {PyModuleDef_HEAD_INIT};
  const size_t zoff = offsetof(PyModuleDef, m_name);
  std::memset((char*)&module_def + zoff, 0, sizeof(module_def) - zoff);
  module_def.m_name = "tkrzw";
  module_def.m_doc = "a set of implementations of DBM";
  module_def.m_size = -1;
  static PyMethodDef methods[] = {
    {nullptr, nullptr, 0, nullptr},
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

// Implementation of Utility.SerializeInt.
static PyObject* utility_SerializeInt(PyObject* self, PyObject* pyargs) {
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc != 1) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pynum = PyTuple_GET_ITEM(pyargs, 0);
  const int64_t num = PyObjToInt(pynum);
  const std::string str = tkrzw::IntToStrBigEndian(num, sizeof(int64_t));
  return CreatePyBytes(str);
}

// Implementation of Utility.DeserializeInt.
static PyObject* utility_DeserializeInt(PyObject* self, PyObject* pyargs) {
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc != 1) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pydata = PyTuple_GET_ITEM(pyargs, 0);
  SoftString data(pydata);
  const int64_t num = tkrzw::StrToIntBigEndian(data.Get());
  return PyLong_FromLongLong(num);
}

// Implementation of Utility.SerializeFloat.
static PyObject* utility_SerializeFloat(PyObject* self, PyObject* pyargs) {
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc != 1) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pynum = PyTuple_GET_ITEM(pyargs, 0);
  const double num = PyObjToDouble(pynum);
  const std::string str = tkrzw::FloatToStrBigEndian(num, sizeof(double));
  return CreatePyBytes(str);
}

// Implementation of Utility.DeserializeFloat.
static PyObject* utility_DeserializeFloat(PyObject* self, PyObject* pyargs) {
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc != 1) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pydata = PyTuple_GET_ITEM(pyargs, 0);
  SoftString data(pydata);
  const double num = tkrzw::StrToFloatBigEndian(data.Get());
  return PyFloat_FromDouble(num);
}

// Defines the Utility class.
static bool DefineUtility() {
  static PyTypeObject pytype = {PyVarObject_HEAD_INIT(nullptr, 0)};
  const size_t zoff = offsetof(PyTypeObject, tp_name);
  std::memset((char*)&pytype + zoff, 0, sizeof(pytype) - zoff);
  pytype.tp_name = "tkrzw.Utility";
  pytype.tp_basicsize = sizeof(PyUtility);
  pytype.tp_itemsize = 0;
  pytype.tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
  pytype.tp_doc = "Library utilities.";
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
    {"SerializeInt", (PyCFunction)utility_SerializeInt, METH_CLASS | METH_VARARGS,
     "Serializes an integer into a big-endian binary sequence."},
    {"DeserializeInt", (PyCFunction)utility_DeserializeInt, METH_CLASS | METH_VARARGS,
     "Deserializes a big-endian binary sequence into an integer."},
    {"SerializeFloat", (PyCFunction)utility_SerializeFloat, METH_CLASS | METH_VARARGS,
     "Serializes a floating-point number into a big-endian binary sequence."},
    {"DeserializeFloat", (PyCFunction)utility_DeserializeFloat, METH_CLASS | METH_VARARGS,
     "Deserializes a big-endian binary sequence into a floating-point number."},
    {nullptr, nullptr, 0, nullptr},
  };
  pytype.tp_methods = methods;
  if (PyType_Ready(&pytype) != 0) return false;
  cls_utility = (PyObject*)&pytype;
  Py_INCREF(cls_utility);
  if (!SetConstStr(cls_utility, "VERSION", tkrzw::PACKAGE_VERSION)) return false;
  if (!SetConstStr(cls_utility, "OS_NAME", tkrzw::OS_NAME)) return false;
  if (!SetConstLong(cls_utility, "PAGE_SIZE", tkrzw::PAGE_SIZE)) return false;
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
    return nullptr;
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

// Implementation of Status#Join.
static PyObject* status_Join(PyTkStatus* self, PyObject* pyargs) {
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc != 1) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pyrht = PyTuple_GET_ITEM(pyargs, 0);
  if (!PyObject_IsInstance(pyrht, cls_status)) {
    ThrowInvalidArguments("the argument is not a Status");
    return nullptr;
  }
  PyTkStatus* rht = (PyTkStatus*)pyrht;
  (*self->status) |= (*rht->status);
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

// Implementation of Status.CodeName.
static PyObject* status_CodeName(PyObject* self, PyObject* pyargs) {
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc != 1) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pycode = PyTuple_GET_ITEM(pyargs, 0);
  const tkrzw::Status::Code code = (tkrzw::Status::Code)PyLong_AsLong(pycode);
  return CreatePyString(tkrzw::ToString(tkrzw::Status::CodeName(code)));
}

// Defines the Status class.
static bool DefineStatus() {
  static PyTypeObject pytype = {PyVarObject_HEAD_INIT(nullptr, 0)};
  const size_t zoff = offsetof(PyTypeObject, tp_name);
  std::memset((char*)&pytype + zoff, 0, sizeof(pytype) - zoff);
  pytype.tp_name = "tkrzw.Status";
  pytype.tp_basicsize = sizeof(PyTkStatus);
  pytype.tp_itemsize = 0;
  pytype.tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
  pytype.tp_doc = "Status of operations.";
  pytype.tp_new = status_new;
  pytype.tp_dealloc = (destructor)status_dealloc;
  pytype.tp_init = (initproc)status_init;
  pytype.tp_repr = (unaryfunc)status_repr;
  pytype.tp_str = (unaryfunc)status_str;
  pytype.tp_richcompare = (richcmpfunc)status_richcmp;
  static PyMethodDef methods[] = {
    {"Set", (PyCFunction)status_Set, METH_VARARGS,
     "Set the code and the message."},
    {"Join", (PyCFunction)status_Join, METH_VARARGS,
     "Assigns the internal state only if the current state is success."},
    {"GetCode", (PyCFunction)status_GetCode, METH_NOARGS,
     "Gets the status code.."},
    {"GetMessage", (PyCFunction)status_GetMessage, METH_NOARGS,
     "Gets the status message."},
    {"IsOK", (PyCFunction)status_IsOK, METH_NOARGS,
     "Returns true if the status is success."},
    {"OrDie", (PyCFunction)status_OrDie, METH_NOARGS,
     "Raises a runtime error if the status is not success."},
    {"CodeName", (PyCFunction)status_CodeName, METH_CLASS | METH_VARARGS,
     "Gets the string name of a status code."},
    {nullptr, nullptr, 0, nullptr},
  };
  pytype.tp_methods = methods;
  if (PyType_Ready(&pytype) != 0) return false;
  cls_status = (PyObject*)&pytype;
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
  if (!SetConstLong(cls_status, "NETWORK_ERROR",
                    (int64_t)tkrzw::Status::NETWORK_ERROR)) return false;
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
  static PyTypeObject pytype = {PyVarObject_HEAD_INIT(nullptr, 0)};
  const size_t zoff = offsetof(PyTypeObject, tp_name);
  std::memset((char*)&pytype + zoff, 0, sizeof(pytype) - zoff);
  pytype.tp_name = "tkrzw.StatusException";
  pytype.tp_basicsize = sizeof(PyException);
  pytype.tp_itemsize = 0;
  pytype.tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
  pytype.tp_doc = "Exception to convey the status of operations.";
  pytype.tp_new = expt_new;
  pytype.tp_dealloc = (destructor)expt_dealloc;
  pytype.tp_init = (initproc)expt_init;
  pytype.tp_repr = (unaryfunc)expt_repr;
  pytype.tp_str = (unaryfunc)expt_str;
  static PyMethodDef methods[] = {
    {"GetStatus", (PyCFunction)expt_GetStatus, METH_NOARGS,
     "Get the status object." },
    {nullptr, nullptr, 0, nullptr}
  };
  pytype.tp_methods = methods;
  pytype.tp_base = (PyTypeObject*)PyExc_RuntimeError;
  if (PyType_Ready(&pytype) != 0) return false;
  cls_expt = (PyObject*)&pytype;
  Py_INCREF(cls_expt);
  if (PyModule_AddObject(mod_tkrzw, "StatusException", cls_expt) != 0) return false;
  return true;
}

// Implementation of Future.new.
static PyObject* future_new(PyTypeObject* pytype, PyObject* pyargs, PyObject* pykwds) {
  PyFuture* self = (PyFuture*)pytype->tp_alloc(pytype, 0);
  if (!self) return nullptr;
  self->future = nullptr;
  self->concurrent = false;
  self->is_str = false;
  return (PyObject*)self;
}

// Implementation of Future#dealloc.
static void future_dealloc(PyFuture* self) {
  delete self->future;
  Py_TYPE(self)->tp_free((PyObject*)self);
}

// Implementation of Future#__init__.
static int future_init(PyFuture* self, PyObject* pyargs, PyObject* pykwds) {
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc != 0) {
    ThrowInvalidArguments("too many arguments");
    return -1;
  }
  ThrowStatusException(tkrzw::Status(tkrzw::Status::NOT_IMPLEMENTED_ERROR));
  return -1;
}

// Implementation of Future#__repr__.
static PyObject* future_repr(PyFuture* self) {
  const std::string& str = tkrzw::SPrintF("<tkrzw.Future: %p>", (void*)self->future);
  return CreatePyString(str);
}

// Implementation of Future#__str__.
static PyObject* future_str(PyFuture* self) {
  const std::string& str = tkrzw::SPrintF("Future:%p", (void*)self->future);
  return CreatePyString(str);
}

// Implementation of Future#__iter__.
static PyObject* future_iter(PyFuture* self) {
  Py_INCREF(self);  
  return (PyObject*)self;
}

// Implementation of Future#__next__.
static PyObject* future_iternext(PyFuture* self) {
  PyErr_SetString(PyExc_StopIteration, "end of iteration");
  return nullptr;
}

// Implementation of Future#__await__.
static PyObject* future_await(PyFuture* self) {
  {
    NativeLock lock(self->concurrent);
    self->future->Wait();
  }
  self->concurrent = false;
  Py_INCREF(self);  
  return (PyObject*)self;
}

// Implementation of Future#Wait.
static PyObject* future_Wait(PyFuture* self, PyObject* pyargs) {
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc > 1) {
    ThrowInvalidArguments("too many arguments");
    return nullptr;
  }
  const double timeout = argc > 0 ? PyObjToDouble(PyTuple_GET_ITEM(pyargs, 0)) : -1.0;
  bool ok = false;
  {
    NativeLock lock(self->concurrent);
    ok = self->future->Wait(timeout);
  }
  if (ok) {
    self->concurrent = false;
    Py_RETURN_TRUE;
  }
  Py_RETURN_FALSE;
}

// Implementation of Future#Get.
static PyObject* future_Get(PyFuture* self) {
  const auto& type = self->future->GetExtraType();
  if (type == typeid(tkrzw::Status)) {
    NativeLock lock(self->concurrent);
    tkrzw::Status status = self->future->Get();
    lock.Release();
    delete self->future;
    self->future = nullptr;
    return CreatePyTkStatusMove(std::move(status));
  }
  if (type == typeid(std::pair<tkrzw::Status, std::string>)) {
    NativeLock lock(self->concurrent);
    const auto& result = self->future->GetString();
    lock.Release();
    delete self->future;
    self->future = nullptr;
    PyObject* pyrv = PyTuple_New(2);
    PyTuple_SET_ITEM(pyrv, 0, CreatePyTkStatus(std::move(result.first)));
    if (self->is_str) {
      PyTuple_SET_ITEM(pyrv, 1, CreatePyString(result.second));
    } else {
      PyTuple_SET_ITEM(pyrv, 1, CreatePyBytes(result.second));
    }
    return pyrv;
  }
  if (type == typeid(std::pair<tkrzw::Status, std::pair<std::string, std::string>>)) {
    NativeLock lock(self->concurrent);
    const auto& result = self->future->GetStringPair();
    lock.Release();
    delete self->future;
    self->future = nullptr;
    PyObject* pyrv = PyTuple_New(3);
    PyTuple_SET_ITEM(pyrv, 0, CreatePyTkStatus(std::move(result.first)));
    if (self->is_str) {
      PyTuple_SET_ITEM(pyrv, 1, CreatePyString(result.second.first));
      PyTuple_SET_ITEM(pyrv, 2, CreatePyString(result.second.second));
    } else {
      PyTuple_SET_ITEM(pyrv, 1, CreatePyBytes(result.second.first));
      PyTuple_SET_ITEM(pyrv, 2, CreatePyBytes(result.second.second));
    }
    return pyrv;
  }
  if (type == typeid(std::pair<tkrzw::Status, std::vector<std::string>>)) {
    NativeLock lock(self->concurrent);
    const auto& result = self->future->GetStringVector();
    lock.Release();
    delete self->future;
    self->future = nullptr;
    PyObject* pyrv = PyTuple_New(2);
    PyTuple_SET_ITEM(pyrv, 0, CreatePyTkStatus(std::move(result.first)));
    PyObject* pylist = PyTuple_New(result.second.size());
    for (size_t i = 0; i < result.second.size(); i++) {
      if (self->is_str) {
        PyTuple_SET_ITEM(pylist, i, CreatePyString(result.second[i]));
      } else {
        PyTuple_SET_ITEM(pylist, i, CreatePyBytes(result.second[i]));
      }
    }
    PyTuple_SET_ITEM(pyrv, 1, pylist);
    return pyrv;
  }
  if (type == typeid(std::pair<tkrzw::Status, std::map<std::string, std::string>>)) {
    NativeLock lock(self->concurrent);
    const auto& result = self->future->GetStringMap();
    lock.Release();
    delete self->future;
    self->future = nullptr;
    PyObject* pyrv = PyTuple_New(2);
    PyTuple_SET_ITEM(pyrv, 0, CreatePyTkStatus(std::move(result.first)));
    PyObject* pydict = PyDict_New();
    for (const auto& rec : result.second) {
      if (self->is_str) {
        PyObject* pykey = CreatePyString(rec.first);
        PyObject* pyvalue = CreatePyString(rec.second);
        PyDict_SetItem(pydict, pykey, pyvalue);
        Py_DECREF(pyvalue);
        Py_DECREF(pykey);
      } else {
        PyObject* pykey = CreatePyBytes(rec.first);
        PyObject* pyvalue = CreatePyBytes(rec.second);
        PyDict_SetItem(pydict, pykey, pyvalue);
        Py_DECREF(pyvalue);
        Py_DECREF(pykey);
      }
    }
    PyTuple_SET_ITEM(pyrv, 1, pydict);
    return pyrv;
  }
  if (type == typeid(std::pair<tkrzw::Status, int64_t>)) {
    NativeLock lock(self->concurrent);
    const auto& result = self->future->GetInteger();
    lock.Release();
    delete self->future;
    self->future = nullptr;
    PyObject* pyrv = PyTuple_New(2);
    PyTuple_SET_ITEM(pyrv, 0, CreatePyTkStatus(std::move(result.first)));
    PyTuple_SET_ITEM(pyrv, 1, PyLong_FromLongLong(result.second));
    return pyrv;
  }
  ThrowStatusException(tkrzw::Status(tkrzw::Status::NOT_IMPLEMENTED_ERROR));
  return nullptr;
}

// Defines the Future class.
static bool DefineFuture() {
  static PyTypeObject pytype = {PyVarObject_HEAD_INIT(nullptr, 0)};
  const size_t zoff = offsetof(PyTypeObject, tp_name);
  std::memset((char*)&pytype + zoff, 0, sizeof(pytype) - zoff);
  pytype.tp_name = "tkrzw.Future";
  pytype.tp_basicsize = sizeof(PyFuture);
  pytype.tp_itemsize = 0;
  pytype.tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
  pytype.tp_doc = "Future to monitor the result of asynchronous operations.";
  pytype.tp_new = future_new;
  pytype.tp_dealloc = (destructor)future_dealloc;
  pytype.tp_init = (initproc)future_init;
  pytype.tp_repr = (unaryfunc)future_repr;
  pytype.tp_str = (unaryfunc)future_str;
  static PyMethodDef methods[] = {
    {"Wait", (PyCFunction)future_Wait, METH_VARARGS,
     "Waits for the operation to be done."},
    {"Get", (PyCFunction)future_Get, METH_NOARGS,
     "Waits for the operation to be done and gets the result status." },
    {nullptr, nullptr, 0, nullptr}
  };
  pytype.tp_methods = methods;
  static PyAsyncMethods async_methods;
  std::memset(&async_methods, 0, sizeof(async_methods));
  async_methods.am_await = (unaryfunc)future_await;
  pytype.tp_as_async = &async_methods;
  static PyMappingMethods map_methods;
  std::memset(&map_methods, 0, sizeof(map_methods));
  pytype.tp_iter = (getiterfunc)future_iter;
  pytype.tp_iternext = (iternextfunc)future_iternext;
  pytype.tp_as_mapping = &map_methods;
  if (PyType_Ready(&pytype) != 0) return false;
  cls_future = (PyObject*)&pytype;
  Py_INCREF(cls_future);
  if (PyModule_AddObject(mod_tkrzw, "Future", cls_future) != 0) return false;
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
    if (tkrzw::StrToBool(tkrzw::SearchMap(params, "sync_hard", "false"))) {
      open_options |= tkrzw::File::OPEN_SYNC_HARD;
    }
    params.erase("concurrent");
    params.erase("truncate");
    params.erase("no_create");
    params.erase("no_wait");
    params.erase("no_lock");
    params.erase("sync_hard");
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
  return CreatePyTkStatusMove(std::move(status));
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
  delete self->dbm;
  self->dbm = nullptr;
  return CreatePyTkStatusMove(std::move(status));
}

// Implementation of DBM#Process.
static PyObject* dbm_Process(PyDBM* self, PyObject* pyargs) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
  if (self->concurrent) {
    return CreatePyTkStatusMove(tkrzw::Status(
        tkrzw::Status::PRECONDITION_ERROR, "the concurrent mode is not supported"));
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc != 3) {
    ThrowInvalidArguments(argc < 3 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pykey = PyTuple_GET_ITEM(pyargs, 0);
  PyObject* pyfunc = PyTuple_GET_ITEM(pyargs, 1);
  const bool writable = PyObject_IsTrue(PyTuple_GET_ITEM(pyargs, 2));
  if (!PyCallable_Check(pyfunc)) {
    ThrowInvalidArguments("non callable is given");
    return nullptr;
  }
  SoftString key(pykey);
  std::unique_ptr<SoftString> funcrvstr;
  auto func = [&](std::string_view k, std::string_view v) -> std::string_view {
    PyObject* pyfuncargs = PyTuple_New(2);
    PyTuple_SET_ITEM(pyfuncargs, 0, CreatePyBytes(k));
    if (v.data() == tkrzw::DBM::RecordProcessor::NOOP.data()) {
      Py_INCREF(Py_None);
      PyTuple_SET_ITEM(pyfuncargs, 1, Py_None);
    } else {
      PyTuple_SET_ITEM(pyfuncargs, 1, CreatePyBytes(v));
    }
    PyObject* pyfuncrv = PyObject_CallObject(pyfunc, pyfuncargs);
    std::string_view funcrv = tkrzw::DBM::RecordProcessor::NOOP;
    if (pyfuncrv != nullptr) {
      if (pyfuncrv == Py_None) {
        funcrv = tkrzw::DBM::RecordProcessor::NOOP;
      } else if (pyfuncrv == Py_False) {
        funcrv = tkrzw::DBM::RecordProcessor::REMOVE;
      } else {
        funcrvstr = std::make_unique<SoftString>(pyfuncrv);
        funcrv = funcrvstr->Get();
      }
      Py_DECREF(pyfuncrv);
    }
    Py_DECREF(pyfuncargs);
    return funcrv;
  };
  tkrzw::Status status = self->dbm->Process(key.Get(), func, writable);
  return CreatePyTkStatusMove(std::move(status));
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
    self->dbm->GetMulti(key_views, &records);
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
  std::vector<std::string_view> key_views(keys.begin(), keys.end());
  std::map<std::string, std::string> records;
  {
    NativeLock lock(self->concurrent);
    self->dbm->GetMulti(key_views, &records);
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
  return CreatePyTkStatusMove(std::move(status));
}

// Implementation of DBM#SetMulti.
static PyObject* dbm_SetMulti(PyDBM* self, PyObject* pyargs, PyObject* pykwds) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc > 1) {
    ThrowInvalidArguments("too many arguments");
    return nullptr;
  }
  PyObject* pyoverwrite = argc > 0 ? PyTuple_GET_ITEM(pyargs, 0) : Py_True;
  const bool overwrite = PyObject_IsTrue(pyoverwrite);
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
    status = self->dbm->SetMulti(record_views, overwrite);
  }
  return CreatePyTkStatusMove(std::move(status));
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
  PyTuple_SET_ITEM(pytuple, 0, CreatePyTkStatusMove(std::move(status)));
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
  return CreatePyTkStatusMove(std::move(status));
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
  return CreatePyTkStatusMove(std::move(status));
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
  const bool success = status == tkrzw::Status::SUCCESS;
  PyTuple_SET_ITEM(pytuple, 0, CreatePyTkStatusMove(std::move(status)));
  if (success) {
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
  return CreatePyTkStatusMove(std::move(status));
}

// Implementation of DBM#AppendMulti.
static PyObject* dbm_AppendMulti(PyDBM* self, PyObject* pyargs, PyObject* pykwds) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc > 1) {
    ThrowInvalidArguments("too many arguments");
    return nullptr;
  }
  PyObject* pydelim = argc > 0 ? PyTuple_GET_ITEM(pyargs, 0) : nullptr;
  SoftString delim(pydelim == nullptr ? Py_None : pydelim);
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
    status = self->dbm->AppendMulti(record_views, delim.Get());
  }
  return CreatePyTkStatusMove(std::move(status));
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
    if (pyexpected == obj_dbm_any_data) {
      expected_view = tkrzw::DBM::ANY_DATA;
    } else {
      expected = std::make_unique<SoftString>(pyexpected);
      expected_view = expected->Get();
    }
  }
  std::unique_ptr<SoftString> desired;
  std::string_view desired_view;
  if (pydesired != Py_None) {
    if (pydesired == obj_dbm_any_data) {
      desired_view = tkrzw::DBM::ANY_DATA;
    } else {
      desired = std::make_unique<SoftString>(pydesired);
      desired_view = desired->Get();
    }
  }
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->dbm->CompareExchange(key.Get(), expected_view, desired_view);
  }
  return CreatePyTkStatusMove(std::move(status));
}

// Implementation of DBM#CompareExchangeAndGet.
static PyObject* dbm_CompareExchangeAndGet(PyDBM* self, PyObject* pyargs) {
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
    if (pyexpected == obj_dbm_any_data) {
      expected_view = tkrzw::DBM::ANY_DATA;
    } else {
      expected = std::make_unique<SoftString>(pyexpected);
      expected_view = expected->Get();
    }
  }
  std::unique_ptr<SoftString> desired;
  std::string_view desired_view;
  if (pydesired != Py_None) {
    if (pydesired == obj_dbm_any_data) {
      desired_view = tkrzw::DBM::ANY_DATA;
    } else {
      desired = std::make_unique<SoftString>(pydesired);
      desired_view = desired->Get();
    }
  }
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  std::string actual;
  bool found = false;
  {
    NativeLock lock(self->concurrent);
    status = self->dbm->CompareExchange(key.Get(), expected_view, desired_view, &actual, &found);
  }
  PyObject* pytuple = PyTuple_New(2);
  PyTuple_SET_ITEM(pytuple, 0, CreatePyTkStatusMove(std::move(status)));
  if (found) {
    PyObject* pyactual = PyUnicode_Check(pyexpected) || PyUnicode_Check(pydesired) ?
        CreatePyString(actual) : CreatePyBytes(actual);
    PyTuple_SET_ITEM(pytuple, 1, pyactual);
  } else {
    Py_INCREF(Py_None);
    PyTuple_SET_ITEM(pytuple, 1, Py_None);
  }
  return pytuple;
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


// Implementation of DBM#ProcessMulti.
static PyObject* dbm_ProcessMulti(PyDBM* self, PyObject* pyargs) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
  if (self->concurrent) {
    return CreatePyTkStatusMove(tkrzw::Status(
        tkrzw::Status::PRECONDITION_ERROR, "the concurrent mode is not supported"));
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc != 2) {
    ThrowInvalidArguments(argc < 2 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pykfpairs = PyTuple_GET_ITEM(pyargs, 0);
  const bool writable = PyObject_IsTrue(PyTuple_GET_ITEM(pyargs, 1));
  if (!PySequence_Check(pykfpairs)) {
    ThrowInvalidArguments("parameters must be sequences of tuples and strings and functions");
    return nullptr;
  }
  const auto& kfpairs_ph = ExtractKFPairs(pykfpairs);
  std::vector<std::pair<std::string_view, tkrzw::DBM::RecordProcessor*>> kfpairs;
  kfpairs.reserve(kfpairs_ph.size());
  for (const auto& key_proc : kfpairs_ph) {
    auto kfpair = std::make_pair(std::string_view(key_proc.first), key_proc.second.get());
    kfpairs.emplace_back(std::move(kfpair));
  }
  tkrzw::Status status = self->dbm->ProcessMulti(kfpairs, writable);
  return CreatePyTkStatusMove(std::move(status));
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
  return CreatePyTkStatusMove(std::move(status));
}

// Implementation of DBM#Rekey.
static PyObject* dbm_Rekey(PyDBM* self, PyObject* pyargs) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc < 2 || argc > 4) {
    ThrowInvalidArguments(argc < 2 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pyold_key = PyTuple_GET_ITEM(pyargs, 0);
  PyObject* pynew_key = PyTuple_GET_ITEM(pyargs, 1);
  const bool overwrite = argc > 2 ? PyObject_IsTrue(PyTuple_GET_ITEM(pyargs, 2)) : true;
  const bool copying = argc > 3 ? PyObject_IsTrue(PyTuple_GET_ITEM(pyargs, 3)) : false;
  SoftString old_key(pyold_key);
  SoftString new_key(pynew_key);
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->dbm->Rekey(old_key.Get(), new_key.Get(), overwrite, copying);
  }
  return CreatePyTkStatusMove(std::move(status));
}

// Implementation of DBM#PopFirst.
static PyObject* dbm_PopFirst(PyDBM* self, PyObject* pyargs) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
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
    status = self->dbm->PopFirst(&key, &value);
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

// Implementation of DBM#PopFirstStr.
static PyObject* dbm_PopFirstStr(PyDBM* self, PyObject* pyargs) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
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
    status = self->dbm->PopFirst(&key, &value);
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

// Implementation of DBM#PushLast.
static PyObject* dbm_PushLast(PyDBM* self, PyObject* pyargs) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc < 1 || argc > 2) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pyvalue = PyTuple_GET_ITEM(pyargs, 0);
  const double wtime = argc > 1 ? PyObjToDouble(PyTuple_GET_ITEM(pyargs, 1)) : -1;
  SoftString value(pyvalue);
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->dbm->PushLast(value.Get(), wtime);
  }
  return CreatePyTkStatusMove(std::move(status));
}

// Implementation of DBM#ProcessEach.
static PyObject* dbm_ProcessEach(PyDBM* self, PyObject* pyargs) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
  if (self->concurrent) {
    return CreatePyTkStatusMove(tkrzw::Status(
        tkrzw::Status::PRECONDITION_ERROR, "the concurrent mode is not supported"));
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc != 2) {
    ThrowInvalidArguments(argc < 2 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pyfunc = PyTuple_GET_ITEM(pyargs, 0);
  const bool writable = PyObject_IsTrue(PyTuple_GET_ITEM(pyargs, 1));
  if (!PyCallable_Check(pyfunc)) {
    ThrowInvalidArguments("non callable is given");
    return nullptr;
  }
  std::unique_ptr<SoftString> funcrvstr;
  auto func = [&](std::string_view k, std::string_view v) -> std::string_view {
    PyObject* pyfuncargs = PyTuple_New(2);
    if (k.data() == tkrzw::DBM::RecordProcessor::NOOP.data()) {
      Py_INCREF(Py_None);
      PyTuple_SET_ITEM(pyfuncargs, 0, Py_None);
    } else {
      PyTuple_SET_ITEM(pyfuncargs, 0, CreatePyBytes(k));
    }
    if (v.data() == tkrzw::DBM::RecordProcessor::NOOP.data()) {
      Py_INCREF(Py_None);
      PyTuple_SET_ITEM(pyfuncargs, 1, Py_None);
    } else {
      PyTuple_SET_ITEM(pyfuncargs, 1, CreatePyBytes(v));
    }
    PyObject* pyfuncrv = PyObject_CallObject(pyfunc, pyfuncargs);
    std::string_view funcrv = tkrzw::DBM::RecordProcessor::NOOP;
    if (pyfuncrv != nullptr) {
      if (pyfuncrv == Py_None) {
        funcrv = tkrzw::DBM::RecordProcessor::NOOP;
      } else if (pyfuncrv == Py_False) {
        funcrv = tkrzw::DBM::RecordProcessor::REMOVE;
      } else {
        funcrvstr = std::make_unique<SoftString>(pyfuncrv);
        funcrv = funcrvstr->Get();
      }
      Py_DECREF(pyfuncrv);
    }
    Py_DECREF(pyfuncargs);
    return funcrv;
  };
  tkrzw::Status status = self->dbm->ProcessEach(func, writable);
  return CreatePyTkStatusMove(std::move(status));
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

// Implementation of DBM#GetTimestamp.
static PyObject* dbm_GetTimestamp(PyDBM* self) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
  double timestamp = -1;
  {
    NativeLock lock(self->concurrent);
    timestamp = self->dbm->GetTimestampSimple();
  }
  if (timestamp >= 0) {
    return PyFloat_FromDouble(timestamp);
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
  return CreatePyTkStatusMove(std::move(status));
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
  return CreatePyTkStatusMove(std::move(status));
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
  return CreatePyTkStatusMove(std::move(status));
}

// Implementation of DBM#CopyFileData.
static PyObject* dbm_CopyFileData(PyDBM* self, PyObject* pyargs) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc < 1 || argc > 2) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pydest = PyTuple_GET_ITEM(pyargs, 0);
  bool sync_hard = false;
  if (argc > 1) {
    PyObject* pysync_hard = PyTuple_GET_ITEM(pyargs, 1);
    sync_hard = PyObject_IsTrue(pysync_hard);
  }
  SoftString dest(pydest);
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->dbm->CopyFileData(std::string(dest.Get()), sync_hard);
  }
  return CreatePyTkStatusMove(std::move(status));
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
  return CreatePyTkStatusMove(std::move(status));
}

// Implementation of DBM#ExportToFlatRecords.
static PyObject* dbm_ExportToFlatRecords(PyDBM* self, PyObject* pyargs) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc != 1) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pydest_file = PyTuple_GET_ITEM(pyargs, 0);
  if (!PyObject_IsInstance(pydest_file, cls_file)) {
    ThrowInvalidArguments("the argument is not a File");
    return nullptr;
  }
  PyFile* dest_file = (PyFile*)pydest_file;
  if (dest_file->file == nullptr) {
    ThrowInvalidArguments("not opened file");
    return nullptr;
  }
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = tkrzw::ExportDBMToFlatRecords(self->dbm, dest_file->file);
  }
  return CreatePyTkStatusMove(std::move(status));
}

// Implementation of DBM#ImportFromFlatRecords.
static PyObject* dbm_ImportFromFlatRecords(PyDBM* self, PyObject* pyargs) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc != 1) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pysrc_file = PyTuple_GET_ITEM(pyargs, 0);
  if (!PyObject_IsInstance(pysrc_file, cls_file)) {
    ThrowInvalidArguments("the argument is not a File");
    return nullptr;
  }
  PyFile* src_file = (PyFile*)pysrc_file;
  if (src_file->file == nullptr) {
    ThrowInvalidArguments("not opened file");
    return nullptr;
  }
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = tkrzw::ImportDBMFromFlatRecords(self->dbm, src_file->file);
  }
  return CreatePyTkStatusMove(std::move(status));
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
  PyObject* pydest_file = PyTuple_GET_ITEM(pyargs, 0);
  if (!PyObject_IsInstance(pydest_file, cls_file)) {
    ThrowInvalidArguments("the argument is not a File");
    return nullptr;
  }
  PyFile* dest_file = (PyFile*)pydest_file;
  if (dest_file->file == nullptr) {
    ThrowInvalidArguments("not opened file");
    return nullptr;
  }
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = tkrzw::ExportDBMKeysAsLines(self->dbm, dest_file->file);
  }
  return CreatePyTkStatusMove(std::move(status));
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

// Implementation of DBM#IsWritable.
static PyObject* dbm_IsWritable(PyDBM* self) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
  const bool writable = self->dbm->IsWritable();
  if (writable) {
    Py_RETURN_TRUE;
  }
  Py_RETURN_FALSE;
}

// Implementation of DBM#IsHealthy.
static PyObject* dbm_IsHealthy(PyDBM* self) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return nullptr;
  }
  const bool healthy = self->dbm->IsHealthy();
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
  const bool ordered = self->dbm->IsOrdered();
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
  if (argc < 2 || argc > 3) {
    ThrowInvalidArguments(argc < 2 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pymode = PyTuple_GET_ITEM(pyargs, 0);
  PyObject* pypattern = PyTuple_GET_ITEM(pyargs, 1);
  int32_t capacity = 0;
  if (argc > 2) {
    capacity = PyObjToInt(PyTuple_GET_ITEM(pyargs, 2));
  }
  SoftString pattern(pypattern);
  SoftString mode(pymode);
  std::vector<std::string> keys;
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = tkrzw::SearchDBMModal(self->dbm, mode.Get(), pattern.Get(), &keys, capacity);
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
  if (argc < 2 || argc > 5) {
    ThrowInvalidArguments(argc < 2 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  SoftString old_file_path(PyTuple_GET_ITEM(pyargs, 0));
  SoftString new_file_path(PyTuple_GET_ITEM(pyargs, 1));
  SoftString class_name(argc > 2 ? PyTuple_GET_ITEM(pyargs, 2) : Py_None);
  const int64_t end_offset = argc > 3 ? PyObjToInt(PyTuple_GET_ITEM(pyargs, 3)) : -1;
  SoftString cipher_key(argc > 4 ? PyTuple_GET_ITEM(pyargs, 4) : Py_None);
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  int32_t num_shards = 0;
  if (tkrzw::ShardDBM::GetNumberOfShards(std::string(old_file_path.Get()), &num_shards) ==
      tkrzw::Status::SUCCESS) {
    NativeLock lock(true);
    status = tkrzw::ShardDBM::RestoreDatabase(
        std::string(old_file_path.Get()), std::string(new_file_path.Get()),
        std::string(class_name.Get()), end_offset, cipher_key.Get());
  } else {
    NativeLock lock(true);
    status = tkrzw::PolyDBM::RestoreDatabase(
        std::string(old_file_path.Get()), std::string(new_file_path.Get()),
        std::string(class_name.Get()), end_offset, cipher_key.Get());
  }
  return CreatePyTkStatusMove(std::move(status));
}

// Implementation of DBM#__len__.
static Py_ssize_t dbm_len(PyDBM* self) {
  if (self->dbm == nullptr) {
    return 0;
  }
  int64_t count = -1;
  {
    NativeLock lock(self->concurrent);
    count = self->dbm->CountSimple();
  }
  return std::max<int64_t>(count, 0);
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

// Implementation of DBM#__contains__.
static int dbm_contains(PyDBM* self, PyObject* pykey) {
  if (self->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return -1;
  }
  SoftString key(pykey);
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->dbm->Get(key.Get());
  }
  if (status == tkrzw::Status::SUCCESS) {
    return 1;
  }
  if (status == tkrzw::Status::NOT_FOUND_ERROR) {
    return 0;
  }
  return -1;
}

// Implementation of DBM#__setitem__ and DBM#__delitem__.
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
  static PyTypeObject pytype = {PyVarObject_HEAD_INIT(nullptr, 0)};
  const size_t zoff = offsetof(PyTypeObject, tp_name);
  std::memset((char*)&pytype + zoff, 0, sizeof(pytype) - zoff);
  pytype.tp_name = "tkrzw.DBM";
  pytype.tp_basicsize = sizeof(PyDBM);
  pytype.tp_itemsize = 0;
  pytype.tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
  pytype.tp_doc = "Polymorphic database manager.";
  pytype.tp_new = dbm_new;
  pytype.tp_dealloc = (destructor)dbm_dealloc;
  pytype.tp_init = (initproc)dbm_init;
  pytype.tp_repr = (unaryfunc)dbm_repr;
  pytype.tp_str = (unaryfunc)dbm_str;
  static PyMethodDef methods[] = {
    {"Open", (PyCFunction)dbm_Open, METH_VARARGS | METH_KEYWORDS,
     "Opens a database file."},
    {"Close", (PyCFunction)dbm_Close, METH_NOARGS,
     "Closes the database file."},
    {"Process", (PyCFunction)dbm_Process, METH_VARARGS,
     "Processes a record with an arbitrary function."},
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
    {"AppendMulti", (PyCFunction)dbm_AppendMulti, METH_VARARGS | METH_KEYWORDS,
     "Appends data to multiple records of the keyword arguments."},
    {"CompareExchange", (PyCFunction)dbm_CompareExchange, METH_VARARGS,
     "Compares the value of a record and exchanges if the condition meets."},
    {"CompareExchangeAndGet", (PyCFunction)dbm_CompareExchangeAndGet, METH_VARARGS,
     "Does compare-and-exchange and/or gets the old value of the record."},
    {"Increment", (PyCFunction)dbm_Increment, METH_VARARGS,
     "Increments the numeric value of a record."},
    {"ProcessMulti", (PyCFunction)dbm_ProcessMulti, METH_VARARGS,
     "Processes multiple records with arbitrary functions."},
    {"CompareExchangeMulti", (PyCFunction)dbm_CompareExchangeMulti, METH_VARARGS,
     "Compares the values of records and exchanges if the condition meets."},
    {"Rekey", (PyCFunction)dbm_Rekey, METH_VARARGS,
     "Changes the key of a record."},
    {"PopFirst", (PyCFunction)dbm_PopFirst, METH_VARARGS,
     "Gets the first record and removes it."},
    {"PopFirstStr", (PyCFunction)dbm_PopFirstStr, METH_VARARGS,
     "Gets the first record as strings and removes it."},
    {"PushLast", (PyCFunction)dbm_PushLast, METH_VARARGS,
     "Adds a record with a key of the current timestamp."},
    {"ProcessEach", (PyCFunction)dbm_ProcessEach, METH_VARARGS,
     "Processes each and every record in the database with an arbitrary function."},
    {"Count", (PyCFunction)dbm_Count, METH_NOARGS,
     "Gets the number of records."},
    {"GetFileSize", (PyCFunction)dbm_GetFileSize, METH_NOARGS,
     "Gets the current file size of the database."},
    {"GetFilePath", (PyCFunction)dbm_GetFilePath, METH_NOARGS,
     "Gets the path of the database file."},
    {"GetTimestamp", (PyCFunction)dbm_GetTimestamp, METH_NOARGS,
     "Gets the timestamp in seconds of the last modified time."},
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
    {"ExportToFlatRecords", (PyCFunction)dbm_ExportToFlatRecords, METH_VARARGS,
     "Exports all records of a database to a flat record file."},
    {"ImportFromFlatRecords", (PyCFunction)dbm_ImportFromFlatRecords, METH_VARARGS,
     "Imports records to a database from a flat record file."},
    {"ExportKeysAsLines", (PyCFunction)dbm_ExportKeysAsLines, METH_VARARGS,
     "Exports the keys of all records as lines to a text file."},
    {"Inspect", (PyCFunction)dbm_Inspect, METH_NOARGS,
     "Inspects the database."},
    {"IsOpen", (PyCFunction)dbm_IsOpen, METH_NOARGS,
     "Checks whether the database is open."},
    {"IsWritable", (PyCFunction)dbm_IsWritable, METH_NOARGS,
     "Checks whether the database is writable."},
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
  pytype.tp_methods = methods;
  static PyMappingMethods map_methods;
  std::memset(&map_methods, 0, sizeof(map_methods));
  map_methods.mp_length = (lenfunc)dbm_len;
  map_methods.mp_subscript = (binaryfunc)dbm_getitem;
  map_methods.mp_ass_subscript = (objobjargproc)dbm_setitem;
  pytype.tp_as_mapping = &map_methods;
  static PySequenceMethods seq_methods;
  std::memset(&seq_methods, 0, sizeof(seq_methods));
  seq_methods.sq_contains = (objobjproc)dbm_contains;
  pytype.tp_as_sequence = &seq_methods;
  pytype.tp_iter = (getiterfunc)dbm_iter;
  if (PyType_Ready(&pytype) != 0) return false;
  cls_dbm = (PyObject*)&pytype;
  Py_INCREF(cls_dbm);
  obj_dbm_any_data = PyBytes_FromStringAndSize("\0[ANY]\0", 7);
  if (PyObject_GenericSetAttr(
          cls_dbm, PyUnicode_FromString("ANY_DATA"), obj_dbm_any_data) != 0) {
    return false;
  }
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
  return CreatePyString(tkrzw::StrCat(
      "<tkrzw.Iterator: key=", tkrzw::StrEscapeC(key, true), ">"));
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
  return CreatePyTkStatusMove(std::move(status));
}

// Implementation of Iterator#Last.
static PyObject* iter_Last(PyIterator* self) {
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->iter->Last();
  }
  return CreatePyTkStatusMove(std::move(status));
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
  return CreatePyTkStatusMove(std::move(status));
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
  return CreatePyTkStatusMove(std::move(status));
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
  return CreatePyTkStatusMove(std::move(status));
}

// Implementation of Iterator#Next.
static PyObject* iter_Next(PyIterator* self) {
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->iter->Next();
  }
  return CreatePyTkStatusMove(std::move(status));
}

// Implementation of Iterator#Previous.
static PyObject* iter_Previous(PyIterator* self) {
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->iter->Previous();
  }
  return CreatePyTkStatusMove(std::move(status));
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
  return CreatePyTkStatusMove(std::move(status));
}

// Implementation of Iterator#Remove.
static PyObject* iter_Remove(PyIterator* self) {
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->iter->Remove();
  }
  return CreatePyTkStatusMove(std::move(status));
}

// Implementation of Iterator#Step.
static PyObject* iter_Step(PyIterator* self, PyObject* pyargs) {
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
    status = self->iter->Step(&key, &value);
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

// Implementation of Iterator#StepStr.
static PyObject* iter_StepStr(PyIterator* self, PyObject* pyargs) {
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
    status = self->iter->Step(&key, &value);
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
  static PyTypeObject pytype = {PyVarObject_HEAD_INIT(nullptr, 0)};
  const size_t zoff = offsetof(PyTypeObject, tp_name);
  std::memset((char*)&pytype + zoff, 0, sizeof(pytype) - zoff);
  pytype.tp_name = "tkrzw.Iterator";
  pytype.tp_basicsize = sizeof(PyIterator);
  pytype.tp_itemsize = 0;
  pytype.tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
  pytype.tp_doc = "Iterator for each record.";
  pytype.tp_new = iter_new;
  pytype.tp_dealloc = (destructor)iter_dealloc;
  pytype.tp_init = (initproc)iter_init;
  pytype.tp_repr = (unaryfunc)iter_repr;
  pytype.tp_str = (unaryfunc)iter_str;
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
    {"Step", (PyCFunction)iter_Step, METH_VARARGS,
     "Gets the current record and moves the iterator to the next record."},
    {"StepStr", (PyCFunction)iter_StepStr, METH_VARARGS,
     "Gets the current record and moves the iterator to the next record, as strings."},
    {nullptr, nullptr, 0, nullptr}
  };
  pytype.tp_methods = methods;
  pytype.tp_iternext = (iternextfunc)iter_iternext;
  if (PyType_Ready(&pytype) != 0) return false;
  cls_iter = (PyObject*)&pytype;
  Py_INCREF(cls_iter);
  if (PyModule_AddObject(mod_tkrzw, "Iterator", cls_iter) != 0) return false;
  return true;
}

// Implementation of AsyncDBM.new.
static PyObject* asyncdbm_new(PyTypeObject* pytype, PyObject* pyargs, PyObject* pykwds) {
  PyAsyncDBM* self = (PyAsyncDBM*)pytype->tp_alloc(pytype, 0);
  if (!self) return nullptr;
  self->async = nullptr;
  self->concurrent = false;
  return (PyObject*)self;
}

// Implementation of AsyncDBM#dealloc.
static void asyncdbm_dealloc(PyAsyncDBM* self) {
  delete self->async;
  Py_TYPE(self)->tp_free((PyObject*)self);
}

// Implementation of AsyncDBM#__init__.
static int asyncdbm_init(PyAsyncDBM* self, PyObject* pyargs, PyObject* pykwds) {
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc != 2) {
    ThrowInvalidArguments(argc < 2 ? "too few arguments" : "too many arguments");    
    return -1;
  }
  PyObject* pydbm = PyTuple_GET_ITEM(pyargs, 0);
  if (!PyObject_IsInstance(pydbm, cls_dbm)) {
    ThrowInvalidArguments("the argument is not a DBM");
    return -1;
  }
  PyDBM* dbm = (PyDBM*)pydbm;
  if (dbm->dbm == nullptr) {
    ThrowInvalidArguments("not opened database");
    return -1;
  }
  PyObject* pynum_threads = PyTuple_GET_ITEM(pyargs, 1);
  const int32_t num_threads = PyObjToInt(pynum_threads);
  self->async = new tkrzw::AsyncDBM(dbm->dbm, num_threads);
  self->concurrent = dbm->concurrent;
  return 0;
}

// Implementation of AsyncDBM#__repr__.
static PyObject* asyncdbm_repr(PyAsyncDBM* self) {
  const std::string& str = tkrzw::SPrintF("<tkrzw.AsyncDBM: %p>", (void*)self->async);
  return CreatePyString(str);
}

// Implementation of AsyncDBM#__str__.
static PyObject* asyncdbm_str(PyAsyncDBM* self) {
  const std::string& str = tkrzw::SPrintF("AsyncDBM:%p", (void*)self->async);
  return CreatePyString(str);
}

// Implementation of AsyncDBM#Destruct.
static PyObject* asyncdbm_Destruct(PyAsyncDBM* self) {
  if (self->async == nullptr) {
    ThrowInvalidArguments("destructed object");
    return nullptr;
  }
  delete self->async;
  self->async = nullptr;
  Py_RETURN_NONE;  
}

// Implementation of AsyncDBM#Get.
static PyObject* asyncdbm_Get(PyAsyncDBM* self, PyObject* pyargs) {
  if (self->async == nullptr) {
    ThrowInvalidArguments("destructed object");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc != 1) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pykey = PyTuple_GET_ITEM(pyargs, 0);
  SoftString key(pykey);
  tkrzw::StatusFuture future(self->async->Get(key.Get()));
  return CreatePyFutureMove(std::move(future), self->concurrent);
}

// Implementation of AsyncDBM#GetStr.
static PyObject* asyncdbm_GetStr(PyAsyncDBM* self, PyObject* pyargs) {
  if (self->async == nullptr) {
    ThrowInvalidArguments("destructed object");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc != 1) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pykey = PyTuple_GET_ITEM(pyargs, 0);
  SoftString key(pykey);
  tkrzw::StatusFuture future(self->async->Get(key.Get()));
  return CreatePyFutureMove(std::move(future), self->concurrent, true);
}

// Implementation of AsyncDBM#GetMulti.
static PyObject* asyncdbm_GetMulti(PyAsyncDBM* self, PyObject* pyargs) {
  if (self->async == nullptr) {
    ThrowInvalidArguments("destructed object");
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
  tkrzw::StatusFuture future(self->async->GetMulti(keys));
  return CreatePyFutureMove(std::move(future), self->concurrent);
}

// Implementation of AsyncDBM#GetMultiStr.
static PyObject* asyncdbm_GetMultiStr(PyAsyncDBM* self, PyObject* pyargs) {
  if (self->async == nullptr) {
    ThrowInvalidArguments("destructed object");
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
  tkrzw::StatusFuture future(self->async->GetMulti(keys));
  return CreatePyFutureMove(std::move(future), self->concurrent, true);
}

// Implementation of AsyncDBM#Set.
static PyObject* asyncdbm_Set(PyAsyncDBM* self, PyObject* pyargs) {
  if (self->async == nullptr) {
    ThrowInvalidArguments("destructed object");
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
  tkrzw::StatusFuture future(self->async->Set(key.Get(), value.Get(), overwrite));
  return CreatePyFutureMove(std::move(future), self->concurrent);
}

// Implementation of AsyncDBM#SetMulti.
static PyObject* asyncdbm_SetMulti(PyAsyncDBM* self, PyObject* pyargs, PyObject* pykwds) {
  if (self->async == nullptr) {
    ThrowInvalidArguments("destructed object");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc > 1) {
    ThrowInvalidArguments("too many arguments");
    return nullptr;
  }
  PyObject* pyoverwrite = argc > 0 ? PyTuple_GET_ITEM(pyargs, 0) : Py_True;
  const bool overwrite = PyObject_IsTrue(pyoverwrite);
  std::map<std::string, std::string> records;
  if (pykwds != nullptr) {
    records = MapKeywords(pykwds);
  }
  std::map<std::string_view, std::string_view> record_views;
  for (const auto& record : records) {
    record_views.emplace(std::make_pair(
        std::string_view(record.first), std::string_view(record.second)));
  }
  tkrzw::StatusFuture future(self->async->SetMulti(record_views, overwrite));
  return CreatePyFutureMove(std::move(future), self->concurrent);
}

// Implementation of AsyncDBM#Remove.
static PyObject* asyncdbm_Remove(PyAsyncDBM* self, PyObject* pyargs) {
  if (self->async == nullptr) {
    ThrowInvalidArguments("destructed object");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc != 1) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pykey = PyTuple_GET_ITEM(pyargs, 0);
  SoftString key(pykey);
  tkrzw::StatusFuture future(self->async->Remove(key.Get()));
  return CreatePyFutureMove(std::move(future), self->concurrent);
}

// Implementation of AsyncDBM#RemoveMulti.
static PyObject* asyncdbm_RemoveMulti(PyAsyncDBM* self, PyObject* pyargs) {
  if (self->async == nullptr) {
    ThrowInvalidArguments("destructed object");
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
  tkrzw::StatusFuture future(self->async->RemoveMulti(key_views));
  return CreatePyFutureMove(std::move(future), self->concurrent);
}

// Implementation of AsyncDBM#Append.
static PyObject* asyncdbm_Append(PyAsyncDBM* self, PyObject* pyargs) {
  if (self->async == nullptr) {
    ThrowInvalidArguments("destructed object");
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
  tkrzw::StatusFuture future(self->async->Append(key.Get(), value.Get(), delim.Get()));
  return CreatePyFutureMove(std::move(future), self->concurrent);
}

// Implementation of AsyncDBM#AppendMulti.
static PyObject* asyncdbm_AppendMulti(PyAsyncDBM* self, PyObject* pyargs, PyObject* pykwds) {
  if (self->async == nullptr) {
    ThrowInvalidArguments("destructed object");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc > 1) {
    ThrowInvalidArguments("too many arguments");
    return nullptr;
  }
  PyObject* pydelim = argc > 0 ? PyTuple_GET_ITEM(pyargs, 0) : nullptr;
  SoftString delim(pydelim == nullptr ? Py_None : pydelim);
  std::map<std::string, std::string> records;
  if (pykwds != nullptr) {
    records = MapKeywords(pykwds);
  }
  std::map<std::string_view, std::string_view> record_views;
  for (const auto& record : records) {
    record_views.emplace(std::make_pair(
        std::string_view(record.first), std::string_view(record.second)));
  }
  tkrzw::StatusFuture future(self->async->AppendMulti(record_views, delim.Get()));
  return CreatePyFutureMove(std::move(future), self->concurrent);
}

// Implementation of AsyncDBM#CompareExchange.
static PyObject* asyncdbm_CompareExchange(PyAsyncDBM* self, PyObject* pyargs) {
  if (self->async == nullptr) {
    ThrowInvalidArguments("destructed object");
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
    if (pyexpected == obj_dbm_any_data) {
      expected_view = tkrzw::DBM::ANY_DATA;
    } else {
      expected = std::make_unique<SoftString>(pyexpected);
      expected_view = expected->Get();
    }
  }
  std::unique_ptr<SoftString> desired;
  std::string_view desired_view;
  if (pydesired != Py_None) {
    if (pydesired == obj_dbm_any_data) {
      desired_view = tkrzw::DBM::ANY_DATA;
    } else {
      desired = std::make_unique<SoftString>(pydesired);
      desired_view = desired->Get();
    }
  }
  tkrzw::StatusFuture future(self->async->CompareExchange(
      key.Get(), expected_view, desired_view));
  return CreatePyFutureMove(std::move(future), self->concurrent);
}

// Implementation of AsyncDBM#Increment.
static PyObject* asyncdbm_Increment(PyAsyncDBM* self, PyObject* pyargs) {
  if (self->async == nullptr) {
    ThrowInvalidArguments("destructed object");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc < 1 || argc > 3) {
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
  tkrzw::StatusFuture future(self->async->Increment(key.Get(), inc, init));
  return CreatePyFutureMove(std::move(future), self->concurrent);
}

// Implementation of AsyncDBM#CompareExchangeMulti.
static PyObject* asyncdbm_CompareExchangeMulti(PyAsyncDBM* self, PyObject* pyargs) {
  if (self->async == nullptr) {
    ThrowInvalidArguments("destructed object");
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
  tkrzw::StatusFuture future(self->async->CompareExchangeMulti(expected, desired));
  return CreatePyFutureMove(std::move(future), self->concurrent);
}

// Implementation of AsyncDBM#Rekey.
static PyObject* asyncdbm_Rekey(PyAsyncDBM* self, PyObject* pyargs) {
  if (self->async == nullptr) {
    ThrowInvalidArguments("destructed object");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc < 2 || argc > 4) {
    ThrowInvalidArguments(argc < 2 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pyold_key = PyTuple_GET_ITEM(pyargs, 0);
  PyObject* pynew_key = PyTuple_GET_ITEM(pyargs, 1);
  const bool overwrite = argc > 2 ? PyObject_IsTrue(PyTuple_GET_ITEM(pyargs, 2)) : true;
  const bool copying = argc > 3 ? PyObject_IsTrue(PyTuple_GET_ITEM(pyargs, 3)) : false;
  SoftString old_key(pyold_key);
  SoftString new_key(pynew_key);
  tkrzw::StatusFuture future(self->async->Rekey(
      old_key.Get(), new_key.Get(), overwrite, copying));
  return CreatePyFutureMove(std::move(future), self->concurrent);
}

// Implementation of AsyncDBM#PopFirst.
static PyObject* asyncdbm_PopFirst(PyAsyncDBM* self) {
  if (self->async == nullptr) {
    ThrowInvalidArguments("destructed object");
    return nullptr;
  }
  tkrzw::StatusFuture future(self->async->PopFirst());
  return CreatePyFutureMove(std::move(future), self->concurrent);
}

// Implementation of AsyncDBM#PopFirstStr.
static PyObject* asyncdbm_PopFirstStr(PyAsyncDBM* self) {
  if (self->async == nullptr) {
    ThrowInvalidArguments("destructed object");
    return nullptr;
  }
  tkrzw::StatusFuture future(self->async->PopFirst());
  return CreatePyFutureMove(std::move(future), self->concurrent, true);
}

// Implementation of AsyncDBM#PushLast.
static PyObject* asyncdbm_PushLast(PyAsyncDBM* self, PyObject* pyargs) {
  if (self->async == nullptr) {
    ThrowInvalidArguments("destructed object");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc < 1 || argc > 2) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pyvalue = PyTuple_GET_ITEM(pyargs, 0);
  const double wtime = argc > 1 ? PyObjToDouble(PyTuple_GET_ITEM(pyargs, 1)) : -1;
  SoftString value(pyvalue);
  tkrzw::StatusFuture future(self->async->PushLast(value.Get(), wtime));
  return CreatePyFutureMove(std::move(future), self->concurrent);
}

// Implementation of AsyncDBM#Clear.
static PyObject* asyncdbm_Clear(PyAsyncDBM* self) {
  if (self->async == nullptr) {
    ThrowInvalidArguments("destructed object");
    return nullptr;
  }
  tkrzw::StatusFuture future(self->async->Clear());
  return CreatePyFutureMove(std::move(future), self->concurrent);
}

// Implementation of AsyncDBM#Rebuild.
static PyObject* asyncdbm_Rebuild(PyAsyncDBM* self, PyObject* pyargs, PyObject* pykwds) {
  if (self->async == nullptr) {
    ThrowInvalidArguments("destructed object");
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
  tkrzw::StatusFuture future(self->async->Rebuild(params));
  return CreatePyFutureMove(std::move(future), self->concurrent);
}

// Implementation of AsyncDBM#Synchronize.
static PyObject* asyncdbm_Synchronize(PyAsyncDBM* self, PyObject* pyargs, PyObject* pykwds) {
  if (self->async == nullptr) {
    ThrowInvalidArguments("destructed object");
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
  tkrzw::StatusFuture future(self->async->Synchronize(hard, nullptr, params));
  return CreatePyFutureMove(std::move(future), self->concurrent);
}

// Implementation of AsyncDBM#CopyFileData.
static PyObject* asyncdbm_CopyFileData(PyAsyncDBM* self, PyObject* pyargs) {
  if (self->async == nullptr) {
    ThrowInvalidArguments("destructed object");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc < 1 || argc > 2) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  bool sync_hard = false;
  if (argc > 1) {
    PyObject* pysync_hard = PyTuple_GET_ITEM(pyargs, 1);
    sync_hard = PyObject_IsTrue(pysync_hard);
  }  
  PyObject* pydest = PyTuple_GET_ITEM(pyargs, 0);
  SoftString dest(pydest);
  tkrzw::StatusFuture future(self->async->CopyFileData(std::string(dest.Get()), sync_hard));
  return CreatePyFutureMove(std::move(future), self->concurrent);
}

// Implementation of AsyncDBM#Export.
static PyObject* asyncdbm_Export(PyAsyncDBM* self, PyObject* pyargs) {
  if (self->async == nullptr) {
    ThrowInvalidArguments("destructed object");
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
  tkrzw::StatusFuture future(self->async->Export(dest->dbm));
  return CreatePyFutureMove(std::move(future), self->concurrent);
}

// Implementation of AsyncDBM#ExportToFlatRecords.
static PyObject* asyncdbm_ExportToFlatRecords(PyAsyncDBM* self, PyObject* pyargs) {
  if (self->async == nullptr) {
    ThrowInvalidArguments("destructed object");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc != 1) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pydest_file = PyTuple_GET_ITEM(pyargs, 0);
  if (!PyObject_IsInstance(pydest_file, cls_file)) {
    ThrowInvalidArguments("the argument is not a File");
    return nullptr;
  }
  PyFile* dest_file = (PyFile*)pydest_file;
  if (dest_file->file == nullptr) {
    ThrowInvalidArguments("not opened file");
    return nullptr;
  }
  tkrzw::StatusFuture future(self->async->ExportToFlatRecords(dest_file->file));
  return CreatePyFutureMove(std::move(future), self->concurrent);
}

// Implementation of AsyncDBM#ImportFromFlatRecords.
static PyObject* asyncdbm_ImportFromFlatRecords(PyAsyncDBM* self, PyObject* pyargs) {
  if (self->async == nullptr) {
    ThrowInvalidArguments("destructed object");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc != 1) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pysrc_file = PyTuple_GET_ITEM(pyargs, 0);
  if (!PyObject_IsInstance(pysrc_file, cls_file)) {
    ThrowInvalidArguments("the argument is not a File");
    return nullptr;
  }
  PyFile* src_file = (PyFile*)pysrc_file;
  if (src_file->file == nullptr) {
    ThrowInvalidArguments("not opened file");
    return nullptr;
  }
  tkrzw::StatusFuture future(self->async->ImportFromFlatRecords(src_file->file));
  return CreatePyFutureMove(std::move(future), self->concurrent);
}

// Implementation of AsyncDBM#Search.
static PyObject* asyncdbm_Search(PyAsyncDBM* self, PyObject* pyargs) {
  if (self->async == nullptr) {
    ThrowInvalidArguments("destructed object");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc < 2 || argc > 3) {
    ThrowInvalidArguments(argc < 2 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pymode = PyTuple_GET_ITEM(pyargs, 0);
  PyObject* pypattern = PyTuple_GET_ITEM(pyargs, 1);
  int32_t capacity = 0;
  if (argc > 2) {
    capacity = PyObjToInt(PyTuple_GET_ITEM(pyargs, 2));
  }
  SoftString pattern(pypattern);
  SoftString mode(pymode);
  tkrzw::StatusFuture future(self->async->SearchModal(mode.Get(), pattern.Get(), capacity));
  return CreatePyFutureMove(std::move(future), self->concurrent, true);
}

// Defines the AsyncDBM class.
static bool DefineAsyncDBM() {
  static PyTypeObject pytype = {PyVarObject_HEAD_INIT(nullptr, 0)};
  const size_t zoff = offsetof(PyTypeObject, tp_name);
  std::memset((char*)&pytype + zoff, 0, sizeof(pytype) - zoff);
  pytype.tp_name = "tkrzw.AsyncDBM";
  pytype.tp_basicsize = sizeof(PyAsyncDBM);
  pytype.tp_itemsize = 0;
  pytype.tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
  pytype.tp_doc = "Polymorphic database manager.";
  pytype.tp_new = asyncdbm_new;
  pytype.tp_dealloc = (destructor)asyncdbm_dealloc;
  pytype.tp_init = (initproc)asyncdbm_init;
  pytype.tp_repr = (unaryfunc)asyncdbm_repr;
  pytype.tp_str = (unaryfunc)asyncdbm_str;
  static PyMethodDef methods[] = {
    {"Destruct", (PyCFunction)asyncdbm_Destruct, METH_NOARGS,
     "Destructs the asynchronous database adapter."},
    {"Get", (PyCFunction)asyncdbm_Get, METH_VARARGS,
     "Gets the value of a record of a key."},
    {"GetStr", (PyCFunction)asyncdbm_GetStr, METH_VARARGS,
     "Gets the value of a record of a key, as a string."},
    {"GetMulti", (PyCFunction)asyncdbm_GetMulti, METH_VARARGS,
     "Gets the values of multiple records of keys."},
    {"GetMultiStr", (PyCFunction)asyncdbm_GetMultiStr, METH_VARARGS,
     "Gets the values of multiple records of keys, as strings."},
    {"Set", (PyCFunction)asyncdbm_Set, METH_VARARGS,
     "Sets a record of a key and a value."},
    {"SetMulti", (PyCFunction)asyncdbm_SetMulti, METH_VARARGS | METH_KEYWORDS,
     "Sets multiple records specified by an initializer list of pairs of strings."},
    {"Remove", (PyCFunction)asyncdbm_Remove, METH_VARARGS,
     "Removes a record of a key."},
    {"RemoveMulti", (PyCFunction)asyncdbm_RemoveMulti, METH_VARARGS,
     "Removes records of keys."},
    {"Append", (PyCFunction)asyncdbm_Append, METH_VARARGS,
     "Appends data at the end of a record of a key."},
    {"AppendMulti", (PyCFunction)asyncdbm_AppendMulti, METH_VARARGS | METH_KEYWORDS,
     "Appends data to multiple records of the keyword arguments."},
    {"CompareExchange", (PyCFunction)asyncdbm_CompareExchange, METH_VARARGS,
     "Compares the value of a record and exchanges if the condition meets."},
    {"Increment", (PyCFunction)asyncdbm_Increment, METH_VARARGS,
     "Increments the numeric value of a record."},
    {"CompareExchangeMulti", (PyCFunction)asyncdbm_CompareExchangeMulti, METH_VARARGS,
     "Compares the values of records and exchanges if the condition meets."},
    {"Rekey", (PyCFunction)asyncdbm_Rekey, METH_VARARGS,
     "Changes the key of a record."},
    {"PopFirst", (PyCFunction)asyncdbm_PopFirst, METH_NOARGS,
     "Gets the first record and removes it."},
    {"PopFirstStr", (PyCFunction)asyncdbm_PopFirstStr, METH_NOARGS,
     "Gets the first record as strings and removes it."},
    {"PushLast", (PyCFunction)asyncdbm_PushLast, METH_VARARGS,
     "Adds a record with a key of the current timestamp."},
    {"Clear", (PyCFunction)asyncdbm_Clear, METH_NOARGS,
     "Removes all records."},
    {"Rebuild", (PyCFunction)asyncdbm_Rebuild, METH_VARARGS | METH_KEYWORDS,
     "Rebuilds the entire database."},
    {"Synchronize", (PyCFunction)asyncdbm_Synchronize, METH_VARARGS | METH_KEYWORDS,
     "Synchronizes the content of the database to the file system."},
    {"CopyFileData", (PyCFunction)asyncdbm_CopyFileData, METH_VARARGS,
     "Copies the content of the database file to another file."},
    {"Export", (PyCFunction)asyncdbm_Export, METH_VARARGS,
     "Exports all records to another database."},
    {"ExportToFlatRecords", (PyCFunction)asyncdbm_ExportToFlatRecords, METH_VARARGS,
     "Exports all records of a database to a flat record file."},
    {"ImportFromFlatRecords", (PyCFunction)asyncdbm_ImportFromFlatRecords, METH_VARARGS,
     "Imports records to a database from a flat record file."},
    {"Search", (PyCFunction)asyncdbm_Search, METH_VARARGS,
     "Searches the database and get keys which match a pattern."},
    {nullptr, nullptr, 0, nullptr},
  };
  pytype.tp_methods = methods;
  if (PyType_Ready(&pytype) != 0) return false;
  cls_asyncdbm = (PyObject*)&pytype;
  Py_INCREF(cls_asyncdbm);
  if (PyModule_AddObject(mod_tkrzw, "AsyncDBM", cls_asyncdbm) != 0) return false;
  return true;
}

// Implementation of File.new.
static PyObject* file_new(PyTypeObject* pytype, PyObject* pyargs, PyObject* pykwds) {
  PyFile* self = (PyFile*)pytype->tp_alloc(pytype, 0);
  if (!self) return nullptr;
  self->file = nullptr;
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
  if (self->file == nullptr) {
    return CreatePyString("<tkrzw.File:(unopened)>");
  }
  std::string class_name = "unknown";
  auto* in_file = self->file->GetInternalFile();
  if (in_file != nullptr) {
    const auto& file_type = in_file->GetType();
    if (file_type == typeid(tkrzw::StdFile)) {
      class_name = "StdFile";
    } else if (file_type == typeid(tkrzw::MemoryMapParallelFile)) {
      class_name = "MemoryMapParallelFile";
    } else if (file_type == typeid(tkrzw::MemoryMapAtomicFile)) {
      class_name = "MemoryMapAtomicFile";
    } else if (file_type == typeid(tkrzw::PositionalParallelFile)) {
      class_name = "PositionalParallelFile";
    } else if (file_type == typeid(tkrzw::PositionalAtomicFile)) {
      class_name = "PositionalAtomicFile";
    }
  }
  const std::string path = self->file->GetPathSimple();
  const int64_t size = self->file->GetSizeSimple();
  const std::string& str = tkrzw::StrCat(
      "<tkrzw.File: class=", class_name,
      " path=", tkrzw::StrEscapeC(path, true), " size=", size, ">");
  return CreatePyString(str);
}

// Implementation of File#__str__.
static PyObject* file_str(PyFile* self) {
  if (self->file == nullptr) {
    return CreatePyString("(unopened)");
  }
  std::string class_name = "unknown";
  auto* in_file = self->file->GetInternalFile();
  if (in_file != nullptr) {
    const auto& file_type = in_file->GetType();
    if (file_type == typeid(tkrzw::StdFile)) {
      class_name = "StdFile";
    } else if (file_type == typeid(tkrzw::MemoryMapParallelFile)) {
      class_name = "MemoryMapParallelFile";
    } else if (file_type == typeid(tkrzw::MemoryMapAtomicFile)) {
      class_name = "MemoryMapAtomicFile";
    } else if (file_type == typeid(tkrzw::PositionalParallelFile)) {
      class_name = "PositionalParallelFile";
    } else if (file_type == typeid(tkrzw::PositionalAtomicFile)) {
      class_name = "PositionalAtomicFile";
    }
  }
  const std::string path = self->file->GetPathSimple();
  const int64_t size = self->file->GetSizeSimple();
  const std::string& str = tkrzw::StrCat(
      "class=", class_name, " path=", tkrzw::StrEscapeC(path, true), " size=", size);
  return CreatePyString(str);
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
    if (tkrzw::StrToBool(tkrzw::SearchMap(params, "sync_hard", "false"))) {
      open_options |= tkrzw::File::OPEN_SYNC_HARD;
    }
    params.erase("concurrent");
    params.erase("truncate");
    params.erase("no_create");
    params.erase("no_wait");
    params.erase("no_lock");
    params.erase("sync_hard");
  }
  self->file = new tkrzw::PolyFile();
  self->concurrent = concurrent;
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->file->OpenAdvanced(std::string(path.Get()), writable, open_options, params);
  }
  if (status != tkrzw::Status::SUCCESS) {
    delete self->file;
    self->file = nullptr;
  }
  return CreatePyTkStatusMove(std::move(status));
}

// Implementation of File#Close
static PyObject* file_Close(PyFile* self) {
  if (self->file == nullptr) {
    ThrowInvalidArguments("not opened file");
    return nullptr;
  }
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->file->Close();
  }
  delete self->file;
  self->file = nullptr;
  return CreatePyTkStatusMove(std::move(status));
}

static PyObject* file_Read(PyFile* self, PyObject* pyargs) {
  if (self->file == nullptr) {
    ThrowInvalidArguments("not opened file");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc < 2 || argc > 3) {
    ThrowInvalidArguments(argc < 2 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  const int64_t off = std::max<int64_t>(0, PyObjToInt(PyTuple_GET_ITEM(pyargs, 0)));
  const int64_t size = std::max<int64_t>(0, PyObjToInt(PyTuple_GET_ITEM(pyargs, 1)));
  PyObject* pystatus = nullptr;
  if (argc > 2) {
    pystatus = PyTuple_GET_ITEM(pyargs, 2);
    if (pystatus == Py_None) {
      pystatus = nullptr;
    } else if (!PyObject_IsInstance(pystatus, cls_status)) {
      ThrowInvalidArguments("not a status object");
      return nullptr;
    }
  }
  char* buf = new char[size];
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->file->Read(off, buf, size);
  }
  if (pystatus != nullptr) {
    *((PyTkStatus*)pystatus)->status = status;
  }
  if (status != tkrzw::Status::SUCCESS) {
    delete[] buf;
    Py_RETURN_NONE;
  }
  PyObject* pydata = CreatePyBytes(std::string_view(buf, size));
  delete[] buf;
  return pydata;
}

static PyObject* file_ReadStr(PyFile* self, PyObject* pyargs) {
  if (self->file == nullptr) {
    ThrowInvalidArguments("not opened file");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc < 2 || argc > 3) {
    ThrowInvalidArguments(argc < 2 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  const int64_t off = std::max<int64_t>(0, PyObjToInt(PyTuple_GET_ITEM(pyargs, 0)));
  const int64_t size = std::max<int64_t>(0, PyObjToInt(PyTuple_GET_ITEM(pyargs, 1)));
  PyObject* pystatus = nullptr;
  if (argc > 2) {
    pystatus = PyTuple_GET_ITEM(pyargs, 2);
    if (pystatus == Py_None) {
      pystatus = nullptr;
    } else if (!PyObject_IsInstance(pystatus, cls_status)) {
      ThrowInvalidArguments("not a status object");
      return nullptr;
    }
  }
  char* buf = new char[size];
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->file->Read(off, buf, size);
  }
  if (pystatus != nullptr) {
    *((PyTkStatus*)pystatus)->status = status;
  }
  if (status != tkrzw::Status::SUCCESS) {
    delete[] buf;
    Py_RETURN_NONE;
  }
  PyObject* pystr = CreatePyString(std::string_view(buf, size));
  delete[] buf;
  return pystr;
}

static PyObject* file_Write(PyFile* self, PyObject* pyargs) {
  if (self->file == nullptr) {
    ThrowInvalidArguments("not opened file");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc != 2) {
    ThrowInvalidArguments(argc < 2 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  const int64_t off = std::max<int64_t>(0, PyObjToInt(PyTuple_GET_ITEM(pyargs, 0)));
  PyObject* pydata = PyTuple_GET_ITEM(pyargs, 1);
  SoftString data(pydata);
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->file->Write(off, data.Get().data(), data.Get().size());
  }
  return CreatePyTkStatusMove(std::move(status));
}

static PyObject* file_Append(PyFile* self, PyObject* pyargs) {
  if (self->file == nullptr) {
    ThrowInvalidArguments("not opened file");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc < 1 || argc > 2) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pydata = PyTuple_GET_ITEM(pyargs, 0);
  SoftString data(pydata);
  PyObject* pystatus = nullptr;
  if (argc > 2) {
    pystatus = PyTuple_GET_ITEM(pyargs, 1);
    if (pystatus == Py_None) {
      pystatus = nullptr;
    } else if (!PyObject_IsInstance(pystatus, cls_status)) {
      ThrowInvalidArguments("not a status object");
      return nullptr;
    }
  }
  int64_t new_off = 0;
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->file->Append(data.Get().data(), data.Get().size(), &new_off);
  }
  if (pystatus != nullptr) {
    *((PyTkStatus*)pystatus)->status = status;
  }
  if (status != tkrzw::Status::SUCCESS) {
    Py_RETURN_NONE;
  }
  return PyLong_FromLongLong(new_off);
}

static PyObject* file_Truncate(PyFile* self, PyObject* pyargs) {
  if (self->file == nullptr) {
    ThrowInvalidArguments("not opened file");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc != 1) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  const int64_t size = std::max<int64_t>(0, PyObjToInt(PyTuple_GET_ITEM(pyargs, 0)));
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->file->Truncate(size);
  }
  return CreatePyTkStatusMove(std::move(status));
}

static PyObject* file_Synchronize(PyFile* self, PyObject* pyargs) {
  if (self->file == nullptr) {
    ThrowInvalidArguments("not opened file");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc < 1 || argc > 3) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pyhard = PyTuple_GET_ITEM(pyargs, 0);
  const bool hard = PyObject_IsTrue(pyhard);
  int64_t off = 0;
  int64_t size = 0;
  if (argc > 1) {
    off = std::max<int64_t>(0, PyObjToInt(PyTuple_GET_ITEM(pyargs, 1)));
  }
  if (argc > 2) {
    size = std::max<int64_t>(0, PyObjToInt(PyTuple_GET_ITEM(pyargs, 2)));
  }
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->file->Synchronize(hard, off, size);
  }
  return CreatePyTkStatusMove(std::move(status));
}

static PyObject* file_GetSize(PyFile* self) {
  if (self->file == nullptr) {
    ThrowInvalidArguments("not opened file");
    return nullptr;
  }
  int64_t size = -1;
  {
    NativeLock lock(self->concurrent);
    size = self->file->GetSizeSimple();
  }
  if (size >= 0) {
    return PyLong_FromLongLong(size);
  }
  Py_RETURN_NONE;
}

static PyObject* file_GetPath(PyFile* self) {
  std::string path;
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->file->GetPath(&path);
  }
  if (status == tkrzw::Status::SUCCESS) {
    return CreatePyString(path);
  }
  Py_RETURN_NONE;
}

// Implementation of File#Search.
static PyObject* file_Search(PyFile* self, PyObject* pyargs) {
  if (self->file == nullptr) {
    ThrowInvalidArguments("not opened file");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc < 2 || argc > 3) {
    ThrowInvalidArguments(argc < 2 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pymode = PyTuple_GET_ITEM(pyargs, 0);
  PyObject* pypattern = PyTuple_GET_ITEM(pyargs, 1);
  int32_t capacity = 0;
  if (argc > 2) {
    capacity = PyObjToInt(PyTuple_GET_ITEM(pyargs, 2));
  }
  SoftString pattern(pypattern);
  SoftString mode(pymode);
  std::vector<std::string> lines;
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = tkrzw::SearchTextFileModal(self->file, mode.Get(), pattern.Get(), &lines, capacity);
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
  static PyTypeObject pytype = {PyVarObject_HEAD_INIT(nullptr, 0)};
  const size_t zoff = offsetof(PyTypeObject, tp_name);
  std::memset((char*)&pytype + zoff, 0, sizeof(pytype) - zoff);
  pytype.tp_name = "tkrzw.File";
  pytype.tp_basicsize = sizeof(PyFile);
  pytype.tp_itemsize = 0;
  pytype.tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
  pytype.tp_doc = "Generic file implemenation.";
  pytype.tp_new = file_new;
  pytype.tp_dealloc = (destructor)file_dealloc;
  pytype.tp_init = (initproc)file_init;
  pytype.tp_repr = (unaryfunc)file_repr;
  pytype.tp_str = (unaryfunc)file_str;
  static PyMethodDef methods[] = {
    {"Open", (PyCFunction)file_Open, METH_VARARGS | METH_KEYWORDS,
     "Opens a text file."},
    {"Close", (PyCFunction)file_Close, METH_NOARGS,
     "Closes the text file."},
    {"Read", (PyCFunction)file_Read, METH_VARARGS,
     "Reads data."},
    {"ReadStr", (PyCFunction)file_ReadStr, METH_VARARGS,
     "Reads data as a string."},
    {"Write", (PyCFunction)file_Write, METH_VARARGS,
     "Writes data."},
    {"Append", (PyCFunction)file_Append, METH_VARARGS,
     "Appends data at the end of the file."},
    {"Truncate", (PyCFunction)file_Truncate, METH_VARARGS,
     "Truncates the file."},
    {"Synchronize", (PyCFunction)file_Synchronize, METH_VARARGS,
     "Synchronizes the content of the file to the file system."},
    {"GetSize", (PyCFunction)file_GetSize, METH_NOARGS,
     "Gets the size of the file."},
    {"GetPath", (PyCFunction)file_GetPath, METH_NOARGS,
     "Gets the path of the file."},
    {"Search", (PyCFunction)file_Search, METH_VARARGS,
     "Searches the text file and get lines which match a pattern."},
    {nullptr, nullptr, 0, nullptr}
  };
  pytype.tp_methods = methods;
  if (PyType_Ready(&pytype) != 0) return false;
  cls_file = (PyObject*)&pytype;
  Py_INCREF(cls_file);
  if (PyModule_AddObject(mod_tkrzw, "File", cls_file) != 0) return false;
  return true;
}

// Implementation of Index.new.
static PyObject* index_new(PyTypeObject* pytype, PyObject* pyargs, PyObject* pykwds) {
  PyIndex* self = (PyIndex*)pytype->tp_alloc(pytype, 0);
  if (!self) return nullptr;
  self->index = nullptr;
  self->concurrent = false;
  return (PyObject*)self;
}

// Implementation of Index#dealloc.
static void index_dealloc(PyIndex* self) {
  delete self->index;
  Py_TYPE(self)->tp_free((PyObject*)self);
}

// Implementation of Index#__init__.
static int index_init(PyIndex* self, PyObject* pyargs, PyObject* pykwds) {
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc != 0) {
    ThrowInvalidArguments("too many arguments");
    return -1;
  }
  return 0;
}

// Implementation of Index#__repr__.
static PyObject* index_repr(PyIndex* self) {
  std::string path = "-";
  int64_t num_records = -1;
  if (self->index != nullptr) {
    NativeLock lock(self->concurrent);
    path = self->index->GetFilePath();
    num_records = self->index->Count();
  }
  const std::string& str = tkrzw::StrCat(
      "<tkrzw.Index: path=", tkrzw::StrEscapeC(path, true), " num_records=", num_records, ">");
  return CreatePyString(str);
}

// Implementation of Index#__str__.
static PyObject* index_str(PyIndex* self) {
  std::string path = "-";
  int64_t num_records = -1;
  if (self->index != nullptr) {
    NativeLock lock(self->concurrent);
    path = self->index->GetFilePath();
    num_records = self->index->Count();
  }
  const std::string& str = tkrzw::StrCat(
      "path=", tkrzw::StrEscapeC(path, true), " num_records=", num_records);
  return CreatePyString(str);
}

// Implementation of Index#Open.
static PyObject* index_Open(PyIndex* self, PyObject* pyargs, PyObject* pykwds) {
  if (self->index != nullptr) {
    ThrowInvalidArguments("opened index");
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
    if (tkrzw::StrToBool(tkrzw::SearchMap(params, "sync_hard", "false"))) {
      open_options |= tkrzw::File::OPEN_SYNC_HARD;
    }
    params.erase("concurrent");
    params.erase("truncate");
    params.erase("no_create");
    params.erase("no_wait");
    params.erase("no_lock");
    params.erase("sync_hard");
  }
  self->index = new tkrzw::PolyIndex();
  self->concurrent = concurrent;
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->index->Open(std::string(path.Get()), writable, open_options, params);
  }
  if (status != tkrzw::Status::SUCCESS) {
    delete self->index;
    self->index = nullptr;
  }
  return CreatePyTkStatusMove(std::move(status));
}

// Implementation of Index#Close.
static PyObject* index_Close(PyIndex* self) {
  if (self->index == nullptr) {
    ThrowInvalidArguments("not opened index");
    return nullptr;
  }
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->index->Close();
  }
  delete self->index;
  self->index = nullptr;
  return CreatePyTkStatusMove(std::move(status));
}

// Implementation of Index#GetValues.
static PyObject* index_GetValues(PyIndex* self, PyObject* pyargs) {
  if (self->index == nullptr) {
    ThrowInvalidArguments("not opened index");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc < 1 || argc > 2) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pykey = PyTuple_GET_ITEM(pyargs, 0);
  SoftString key(pykey);
  int32_t capacity = 0;
  if (argc > 1) {
    capacity = PyObjToInt(PyTuple_GET_ITEM(pyargs, 1));
  }
  std::vector<std::string> values;
  {
    NativeLock lock(self->concurrent);
    values = self->index->GetValues(key.Get(), capacity);
  }
  PyObject* pyrv = PyList_New(values.size());
  for (size_t i = 0; i < values.size(); i++) {
    PyList_SET_ITEM(pyrv, i, CreatePyBytes(values[i]));
  }
  return pyrv;
}

// Implementation of Index#GetValuesStr.
static PyObject* index_GetValuesStr(PyIndex* self, PyObject* pyargs) {
  if (self->index == nullptr) {
    ThrowInvalidArguments("not opened index");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc < 1 || argc > 2) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pykey = PyTuple_GET_ITEM(pyargs, 0);
  SoftString key(pykey);
  int32_t capacity = 0;
  if (argc > 1) {
    capacity = PyObjToInt(PyTuple_GET_ITEM(pyargs, 1));
  }
  std::vector<std::string> values;
  {
    NativeLock lock(self->concurrent);
    values = self->index->GetValues(key.Get(), capacity);
  }
  PyObject* pyrv = PyList_New(values.size());
  for (size_t i = 0; i < values.size(); i++) {
    PyList_SET_ITEM(pyrv, i, CreatePyString(values[i]));
  }
  return pyrv;
}

// Implementation of Index#Add.
static PyObject* index_Add(PyIndex* self, PyObject* pyargs) {
  if (self->index == nullptr) {
    ThrowInvalidArguments("not opened index");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc != 2) {
    ThrowInvalidArguments(argc < 2 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pykey = PyTuple_GET_ITEM(pyargs, 0);
  PyObject* pyvalue = PyTuple_GET_ITEM(pyargs, 1);
  SoftString key(pykey);
  SoftString value(pyvalue);
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->index->Add(key.Get(), value.Get());
  }
  return CreatePyTkStatusMove(std::move(status));
}

// Implementation of Index#Remove.
static PyObject* index_Remove(PyIndex* self, PyObject* pyargs) {
  if (self->index == nullptr) {
    ThrowInvalidArguments("not opened index");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc != 2) {
    ThrowInvalidArguments(argc < 2 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pykey = PyTuple_GET_ITEM(pyargs, 0);
  PyObject* pyvalue = PyTuple_GET_ITEM(pyargs, 1);
  SoftString key(pykey);
  SoftString value(pyvalue);
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->index->Remove(key.Get(), value.Get());
  }
  return CreatePyTkStatusMove(std::move(status));
}

// Implementation of Index#Count.
static PyObject* index_Count(PyIndex* self) {
  if (self->index == nullptr) {
    ThrowInvalidArguments("not opened index");
    return nullptr;
  }
  int64_t count = -1;
  {
    NativeLock lock(self->concurrent);
    count = self->index->Count();
  }
  return PyLong_FromLongLong(count);
}

// Implementation of Index#GetFilePath.
static PyObject* index_GetFilePath(PyIndex* self) {
  if (self->index == nullptr) {
    ThrowInvalidArguments("not opened index");
    return nullptr;
  }
  std::string path;
  {
    NativeLock lock(self->concurrent);
    path = self->index->GetFilePath();
  }
  return CreatePyString(path);
}

// Implementation of Index#Clear.
static PyObject* index_Clear(PyIndex* self) {
  if (self->index == nullptr) {
    ThrowInvalidArguments("not opened index");
    return nullptr;
  }
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->index->Clear();
  }
  return CreatePyTkStatusMove(std::move(status));
}

// Implementation of Index#Rebuild.
static PyObject* index_Rebuild(PyIndex* self) {
  if (self->index == nullptr) {
    ThrowInvalidArguments("not opened index");
    return nullptr;
  }
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->index->Rebuild();
  }
  return CreatePyTkStatusMove(std::move(status));
}

// Implementation of Index#Synchronize.
static PyObject* index_Synchronize(PyIndex* self, PyObject* pyargs) {
  if (self->index == nullptr) {
    ThrowInvalidArguments("not opened index");
    return nullptr;
  }
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc != 1) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pyhard = PyTuple_GET_ITEM(pyargs, 0);
  const bool hard = PyObject_IsTrue(pyhard);
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  {
    NativeLock lock(self->concurrent);
    status = self->index->Synchronize(hard);
  }
  return CreatePyTkStatusMove(std::move(status));
}

// Implementation of Index#IsOpen.
static PyObject* index_IsOpen(PyIndex* self) {
  if (self->index == nullptr) {
    Py_RETURN_FALSE;
  }
  Py_RETURN_TRUE;  
}

// Implementation of Index#IsWritable.
static PyObject* index_IsWritable(PyIndex* self) {
  if (self->index == nullptr) {
    ThrowInvalidArguments("not opened index");
    return nullptr;
  }
  const bool writable = self->index->IsWritable();
  if (writable) {
    Py_RETURN_TRUE;
  }
  Py_RETURN_FALSE;
}

// Implementation of Index#MakeIterator.
static PyObject* index_MakeIterator(PyIndex* self) {
  if (self->index == nullptr) {
    ThrowInvalidArguments("not opened index");
    return nullptr;
  }
  PyTypeObject* pyitertype = (PyTypeObject*)cls_indexiter;
  PyIndexIterator* pyiter = (PyIndexIterator*)pyitertype->tp_alloc(pyitertype, 0);
  if (!pyiter) return nullptr;
  {
    NativeLock lock(self->concurrent);
    pyiter->iter = self->index->MakeIterator().release();
  }
  pyiter->concurrent = self->concurrent;
  return (PyObject*)pyiter;
}

// Implementation of Index#__len__.
static Py_ssize_t index_len(PyIndex* self) {
  if (self->index == nullptr) {
    return 0;
  }
  int64_t count = -1;
  {
    NativeLock lock(self->concurrent);
    count = self->index->Count();
  }
  return std::max<int64_t>(count, 0);
}

// Implementation of Index#__contains__.
static int index_contains(PyIndex* self, PyObject* pyrec) {
  if (self->index == nullptr) {
    ThrowInvalidArguments("not opened index");
    return -1;
  }
  if (!PySequence_Check(pyrec)) {
    ThrowInvalidArguments("not sequence argument");
    return -1;
  }
  if (PySequence_Size(pyrec) != 2) {
    ThrowInvalidArguments("not pair argument");
    return -1;
  }
  PyObject* pykey = PySequence_GetItem(pyrec, 0);
  PyObject* pyvalue = PySequence_GetItem(pyrec, 1);
  SoftString key(pykey);
  SoftString value(pyvalue);
  bool check = false;
  {
    NativeLock lock(self->concurrent);
    check = self->index->Check(key.Get(), value.Get());
  }
  Py_DECREF(pykey);
  Py_DECREF(pyvalue);  
  return check ? 1 : 0;
}

// Implementation of Index#__iter__.
static PyObject* index_iter(PyIndex* self) {
  if (self->index == nullptr) {
    ThrowInvalidArguments("not opened index");
    return nullptr;
  }
  PyTypeObject* pyitertype = (PyTypeObject*)cls_indexiter;
  PyIndexIterator* pyiter = (PyIndexIterator*)pyitertype->tp_alloc(pyitertype, 0);
  if (!pyiter) return nullptr;
  {
    NativeLock lock(self->concurrent);
    pyiter->iter = self->index->MakeIterator().release();
    pyiter->concurrent = self->concurrent;
    pyiter->iter->First();
  }
  return (PyObject*)pyiter;
}

// Defines the Index class.
static bool DefineIndex() {
  static PyTypeObject pytype = {PyVarObject_HEAD_INIT(nullptr, 0)};
  const size_t zoff = offsetof(PyTypeObject, tp_name);
  std::memset((char*)&pytype + zoff, 0, sizeof(pytype) - zoff);
  pytype.tp_name = "tkrzw.Index";
  pytype.tp_basicsize = sizeof(PyIndex);
  pytype.tp_itemsize = 0;
  pytype.tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
  pytype.tp_doc = "Secondary index.";
  pytype.tp_new = index_new;
  pytype.tp_dealloc = (destructor)index_dealloc;
  pytype.tp_init = (initproc)index_init;
  pytype.tp_repr = (unaryfunc)index_repr;
  pytype.tp_str = (unaryfunc)index_str;
  static PyMethodDef methods[] = {
    {"Open", (PyCFunction)index_Open, METH_VARARGS | METH_KEYWORDS,
     "Opens an index file."},
    {"Close", (PyCFunction)index_Close, METH_NOARGS,
     "Closes the index file."},
    {"GetValues", (PyCFunction)index_GetValues, METH_VARARGS,
     "Gets all values of records of a key."},
    {"GetValuesStr", (PyCFunction)index_GetValuesStr, METH_VARARGS,
     "Gets all values of records of a key, as strings."},
    {"Add", (PyCFunction)index_Add, METH_VARARGS,
     "Adds a record."},
    {"Remove", (PyCFunction)index_Remove, METH_VARARGS,
     "Removes a record."},
    {"Count", (PyCFunction)index_Count, METH_NOARGS,
     "Gets the number of records."},
    {"GetFilePath", (PyCFunction)index_GetFilePath, METH_NOARGS,
     "Gets the path of the index file."},
    {"Clear", (PyCFunction)index_Clear, METH_NOARGS,
     "Removes all records."},
    {"Rebuild", (PyCFunction)index_Rebuild, METH_NOARGS,
     "Rebuilds the entire index."},
    {"Synchronize", (PyCFunction)index_Synchronize, METH_VARARGS,
     "Synchronizes the content of the index to the file system."},
    {"IsOpen", (PyCFunction)index_IsOpen, METH_NOARGS,
     "Checks whether the index is open."},
    {"IsWritable", (PyCFunction)index_IsWritable, METH_NOARGS,
     "Checks whether the index is writable."},
    {"MakeIterator", (PyCFunction)index_MakeIterator, METH_NOARGS,
     "Makes an iterator for each record."},
    {nullptr, nullptr, 0, nullptr},
  };
  pytype.tp_methods = methods;
  static PyMappingMethods map_methods;
  std::memset(&map_methods, 0, sizeof(map_methods));
  map_methods.mp_length = (lenfunc)index_len;
  pytype.tp_as_mapping = &map_methods;
  static PySequenceMethods seq_methods;
  std::memset(&seq_methods, 0, sizeof(seq_methods));
  seq_methods.sq_contains = (objobjproc)index_contains;
  pytype.tp_as_sequence = &seq_methods;
  pytype.tp_iter = (getiterfunc)index_iter;
  if (PyType_Ready(&pytype) != 0) return false;
  cls_index = (PyObject*)&pytype;
  Py_INCREF(cls_index);
  if (PyModule_AddObject(mod_tkrzw, "Index", cls_index) != 0) return false;
  return true;
}

// Implementation of IndexIterator.new.
static PyObject* indexiter_new(PyTypeObject* pytype, PyObject* pyargs, PyObject* pykwds) {
  PyIndexIterator* self = (PyIndexIterator*)pytype->tp_alloc(pytype, 0);
  if (!self) return nullptr;
  self->iter = nullptr;
  self->concurrent = false;
  return (PyObject*)self;
}

// Implementation of IndexIterator#dealloc.
static void indexiter_dealloc(PyIndexIterator* self) {
  delete self->iter;
  Py_TYPE(self)->tp_free((PyObject*)self);
}

// Implementation of IndexIterator#__init__.
static int indexiter_init(PyIndexIterator* self, PyObject* pyargs, PyObject* pykwds) {
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc != 1) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return -1;
  }
  PyObject* pyindex_obj = PyTuple_GET_ITEM(pyargs, 0);
  if (!PyObject_IsInstance(pyindex_obj, cls_index)) {
    ThrowInvalidArguments("the argument is not an Index");
    return -1;
  }
  PyIndex* pyindex = (PyIndex*)pyindex_obj;
  {
    NativeLock lock(pyindex->concurrent);
    self->iter = pyindex->index->MakeIterator().release();
  }
  self->concurrent = pyindex->concurrent;
  return 0;
}

// Implementation of IndexIterator#__repr__.
static PyObject* indexiter_repr(PyIndexIterator* self) {
  std::string key;
  {
    NativeLock lock(self->concurrent);
    if (!self->iter->Get(&key)) {
      key = "(unlocated)";
    }
  }
  return CreatePyString(tkrzw::StrCat(
      "<tkrzw.IndexIterator: key=", tkrzw::StrEscapeC(key, true), ">"));
}

// Implementation of IndexIterator#__str__.
static PyObject* indexiter_str(PyIndexIterator* self) {
  std::string key;
  {
    NativeLock lock(self->concurrent);
    if (!self->iter->Get(&key)) {
      key = "(unlocated)";
    }
  }
  return CreatePyString(tkrzw::StrEscapeC(key, true));
}

// Implementation of IndexIterator#First.
static PyObject* indexiter_First(PyIndexIterator* self) {
  {
    NativeLock lock(self->concurrent);
    self->iter->First();
  }
  Py_RETURN_NONE;
}

// Implementation of IndexIterator#Last.
static PyObject* indexiter_Last(PyIndexIterator* self) {
  {
    NativeLock lock(self->concurrent);
    self->iter->Last();
  }
  Py_RETURN_NONE;
}

// Implementation of IndexIterator#Jump.
static PyObject* indexiter_Jump(PyIndexIterator* self, PyObject* pyargs) {
  const int32_t argc = PyTuple_GET_SIZE(pyargs);
  if (argc < 1 || argc > 2) {
    ThrowInvalidArguments(argc < 1 ? "too few arguments" : "too many arguments");
    return nullptr;
  }
  PyObject* pykey = PyTuple_GET_ITEM(pyargs, 0);
  SoftString key(pykey);
  if (argc > 1) {
    PyObject* pyvalue = PyTuple_GET_ITEM(pyargs, 1);
    SoftString value(pyvalue);
    NativeLock lock(self->concurrent);
    self->iter->Jump(key.Get(), value.Get());
  } else {
    NativeLock lock(self->concurrent);
    self->iter->Jump(key.Get());
  }
  Py_RETURN_NONE;
}

// Implementation of IndexIterator#Next.
static PyObject* indexiter_Next(PyIndexIterator* self) {
  {
    NativeLock lock(self->concurrent);
    self->iter->Next();
  }
  Py_RETURN_NONE;
}

// Implementation of IndexIterator#Previous.
static PyObject* indexiter_Previous(PyIndexIterator* self) {
  {
    NativeLock lock(self->concurrent);
    self->iter->Previous();
  }
  Py_RETURN_NONE;
}

// Implementation of IndexIterator#Get.
static PyObject* indexiter_Get(PyIndexIterator* self) {
  std::string key, value;
  bool ok = false;
  {
    NativeLock lock(self->concurrent);
    ok = self->iter->Get(&key, &value);
  }
  if (ok) {
    PyObject* pykey = CreatePyBytes(key);
    PyObject* pyvalue = CreatePyBytes(value);
    PyObject * pyrv = PyTuple_Pack(2, pykey, pyvalue);
    Py_DECREF(pyvalue);
    Py_DECREF(pykey);
    return pyrv;
  }
  Py_RETURN_NONE;
}

// Implementation of IndexIterator#GetStr.
static PyObject* indexiter_GetStr(PyIndexIterator* self) {
  std::string key, value;
  bool ok = false;
  {
    NativeLock lock(self->concurrent);
    ok = self->iter->Get(&key, &value);
  }
  if (ok) {
    PyObject* pykey = CreatePyString(key);
    PyObject* pyvalue = CreatePyString(value);
    PyObject * pyrv = PyTuple_Pack(2, pykey, pyvalue);
    Py_DECREF(pyvalue);
    Py_DECREF(pykey);
    return pyrv;
  }
  Py_RETURN_NONE;
}

// Implementation of IndexIterator#__next__.
static PyObject* indexiter_iternext(PyIndexIterator* self) {
  std::string key, value;
  bool ok = false;
  {
    NativeLock lock(self->concurrent);
    ok = self->iter->Get(&key, &value);
  }
  PyObject* pyrv = nullptr;
  if (ok) {
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

// Defines the IndexIterator class.
static bool DefineIndexIterator() {
  static PyTypeObject pytype = {PyVarObject_HEAD_INIT(nullptr, 0)};
  const size_t zoff = offsetof(PyTypeObject, tp_name);
  std::memset((char*)&pytype + zoff, 0, sizeof(pytype) - zoff);
  pytype.tp_name = "tkrzw.IndexIterator";
  pytype.tp_basicsize = sizeof(PyIndexIterator);
  pytype.tp_itemsize = 0;
  pytype.tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
  pytype.tp_doc = "Iterator for each record of the secondary index.";
  pytype.tp_new = indexiter_new;
  pytype.tp_dealloc = (destructor)indexiter_dealloc;
  pytype.tp_init = (initproc)indexiter_init;
  pytype.tp_repr = (unaryfunc)indexiter_repr;
  pytype.tp_str = (unaryfunc)indexiter_str;
  static PyMethodDef methods[] = {
    {"First", (PyCFunction)indexiter_First, METH_NOARGS,
     "Initializes the iterator to indicate the first record."},
    {"Last", (PyCFunction)indexiter_Last, METH_NOARGS,
     "Initializes the iterator to indicate the last record."},
    {"Jump", (PyCFunction)indexiter_Jump, METH_VARARGS,
     "Initializes the iterator to indicate a specific range."},
    {"Next", (PyCFunction)indexiter_Next, METH_NOARGS,
     "Moves the iterator to the next record."},
    {"Previous", (PyCFunction)indexiter_Previous, METH_NOARGS,
     "Moves the iterator to the previous record."},
    {"Get", (PyCFunction)indexiter_Get, METH_NOARGS,
     "Gets the key and the value of the current record of the iterator."},
    {"GetStr", (PyCFunction)indexiter_GetStr, METH_NOARGS,
     "Gets the key and the value of the current record of the iterator, as strings."},
    {nullptr, nullptr, 0, nullptr}
  };
  pytype.tp_methods = methods;
  pytype.tp_iternext = (iternextfunc)indexiter_iternext;
  if (PyType_Ready(&pytype) != 0) return false;
  cls_indexiter = (PyObject*)&pytype;
  Py_INCREF(cls_indexiter);
  if (PyModule_AddObject(mod_tkrzw, "IndexIterator", cls_indexiter) != 0) return false;
  return true;
}

// Entry point of the library.
PyMODINIT_FUNC PyInit_tkrzw() {
  if (!DefineModule()) return nullptr;
  if (!DefineUtility()) return nullptr;
  if (!DefineStatus()) return nullptr;
  if (!DefineStatusException()) return nullptr;
  if (!DefineFuture()) return nullptr;
  if (!DefineDBM()) return nullptr;
  if (!DefineIterator()) return nullptr;
  if (!DefineAsyncDBM()) return nullptr;
  if (!DefineFile()) return nullptr;
  if (!DefineIndex()) return nullptr;
  if (!DefineIndexIterator()) return nullptr;
  return mod_tkrzw;
}

}  // extern "C"

// END OF FILE
