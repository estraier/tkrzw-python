#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Test cases
#
# Copyright 2020 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
# except in compliance with the License.  You may obtain a copy of the License at
#     https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed under the
# License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied.  See the License for the specific language governing permissions
# and limitations under the License.
#--------------------------------------------------------------------------------------------------

import asyncio
import math
import os
import random
import re
import shutil
import sys
import tempfile
import threading
import time
import unittest

from tkrzw import *


# Unit testing framework.
class TestTkrzw(unittest.TestCase):

  # Prepares resources.
  def setUp(self):
    tmp_prefix = "tkrzw-python-"
    self.test_dir = tempfile.mkdtemp(prefix=tmp_prefix)

  # Cleanups resources.
  def tearDown(self):
    shutil.rmtree(self.test_dir)

  # Makes a temporary path.
  def _make_tmp_path(self, name):
    return os.path.join(self.test_dir, name)

  # Utility tests.
  def testUtility(self):
    self.assertTrue(re.search(r"^\d+.\d+.\d+$", Utility.VERSION))
    self.assertTrue(len(Utility.OS_NAME))
    self.assertTrue(Utility.PAGE_SIZE > 0)
    self.assertEqual(-2 ** 31, Utility.INT32MIN)
    self.assertEqual(2 ** 31 - 1, Utility.INT32MAX)
    self.assertEqual(2 ** 32 - 1, Utility.UINT32MAX)
    self.assertEqual(-2 ** 63, Utility.INT64MIN)
    self.assertEqual(2 ** 63 - 1, Utility.INT64MAX)
    self.assertEqual(2 ** 64 - 1, Utility.UINT64MAX)
    if Utility.OS_NAME == "Linux":
      self.assertTrue(Utility.GetMemoryCapacity() > 0)
      self.assertTrue(Utility.GetMemoryUsage() > 0)
    self.assertTrue(3042090208, Utility.PrimaryHash("abc", (1 << 32) - 1))
    self.assertTrue(16973900370012003622, Utility.PrimaryHash("abc"))
    self.assertTrue(702176507, Utility.SecondaryHash("abc", (1 << 32) - 1))
    self.assertTrue(1765794342254572867, Utility.SecondaryHash("abc"))
    self.assertEqual(0, Utility.EditDistanceLev("", ""))
    self.assertEqual(1, Utility.EditDistanceLev("ac", "abc"))
    self.assertEqual(1, Utility.EditDistanceLev("あいう", "あう"))
    int_seq = Utility.SerializeInt(-123456)
    self.assertEqual(8, len(int_seq))
    self.assertEqual(-123456, Utility.DeserializeInt(int_seq))
    float_seq = Utility.SerializeFloat(-123.456)
    self.assertEqual(8, len(float_seq))
    self.assertEqual(-123.456, Utility.DeserializeFloat(float_seq))

  # Status tests.
  def testStatus(self):
    status = Status()
    self.assertEqual(Status.SUCCESS, status)
    self.assertEqual(Status.SUCCESS, status.GetCode())
    self.assertEqual("", status.GetMessage())
    self.assertTrue(status.IsOK())
    status.Set(Status.NOT_FOUND_ERROR, "foobar")
    self.assertEqual(Status.NOT_FOUND_ERROR, status)
    self.assertEqual("NOT_FOUND_ERROR: foobar", str(status))
    self.assertTrue("foobar" in repr(status))
    self.assertFalse(status.IsOK())
    s2 = Status(Status.NOT_IMPLEMENTED_ERROR, "void")
    status.Join(s2)
    self.assertEqual("NOT_FOUND_ERROR: foobar", str(status))
    status.Set(Status.SUCCESS, "OK")
    status.Join(s2)
    self.assertEqual("NOT_IMPLEMENTED_ERROR: void", str(status))
    try:
      status.OrDie()
    except StatusException as e:
      self.assertTrue("void" in str(e))
    else:
      self.fail("no exception")
    self.assertEqual("SUCCESS", Status.CodeName(Status.SUCCESS))
    self.assertEqual("INFEASIBLE_ERROR", Status.CodeName(Status.INFEASIBLE_ERROR))

  # Basic tests.
  def testBasic(self):
    confs = [
      {"path": "casket.tkh",
       "open_params":
       {"update_mode": "UPDATE_APPENDING", "offset_width": 3, "align_pow": 1, "num_buckets": 100,
        "fbp_capacity": 64, "concurrent": True},
       "rebuild_params":
       {"update_mode": "UPDATE_APPENDING", "offset_width": 3, "align_pow": 1, "num_buckets": 100,
        "fbp_capacity": 64},
       "synchronize_params": {},
       "expected_class": "HashDBM"},
      {"path": "casket.tkt",
       "open_params":
       {"update_mode": "UPDATE_APPENDING", "offset_width": 3, "align_pow": 1, "num_buckets": 100,
        "fbp_capacity": 64, "max_page_size": 100, "max_branches": 4, "max_cached_pages": 1,
        "key_comparator": "decimal", "concurrent": True},
       "rebuild_params":
       {"update_mode": "UPDATE_APPENDING", "offset_width": 3, "align_pow": 1, "num_buckets": 100,
        "fbp_capacity": 64, "max_page_size": 100, "max_branches": 4, "max_cached_pages": 1},
       "synchronize_params": {},
       "expected_class": "TreeDBM"},
      {"path": "casket.tks",
       "open_params":
       {"offset_width": 3, "step_unit": 2, "max_level": 3, "sort_mem_size": 100,
        "insert_in_order": False, "max_cached_records": 8, "concurrent": True},
       "rebuild_params":
       {"offset_width": 3, "step_unit": 2, "max_level": 3, "sort_mem_size": 100,
        "max_cached_records": 8},
       "synchronize_params": {"reducer": "last"},
       "expected_class": "SkipDBM"},
      {"path": "casket.tiny",
       "open_params": {"num_buckets": 10},
       "rebuild_params": {"num_buckets": 10},
       "synchronize_params": {},
       "expected_class": "TinyDBM"},
      {"path": "casket.baby",
       "open_params": {"key_comparator": "decimal"},
       "rebuild_params": {},
       "synchronize_params": {},
       "expected_class": "BabyDBM"},
      {"path": "casket.cache",
       "open_params": {"cap_rec_num": 10000, "cap_mem_size": 1000000},
       "rebuild_params": {"cap_rec_num": 10000},
       "synchronize_params": {},
       "expected_class": "CacheDBM"},
      {"path": "casket.stdhash",
       "open_params": {"num_buckets": 10},
       "rebuild_params": {},
       "synchronize_params": {},
       "expected_class": "StdHashDBM"},
      {"path": "casket.stdtree",
       "open_params": {},
       "rebuild_params": {},
       "synchronize_params": {},
       "expected_class": "StdTreeDBM"},
      {"path": "casket",
       "open_params":
       {"num_shards": 4, "dbm": "hash", "num_buckets": 100},
       "rebuild_params": {},
       "synchronize_params": {},
       "expected_class": "HashDBM"},
    ]
    for conf in confs:
      path = conf["path"]
      path = self._make_tmp_path(path) if path else ""
      dbm = DBM()
      self.assertEqual(0, len(dbm))
      open_params = conf["open_params"].copy()
      open_params["truncate"] = True
      self.assertFalse(dbm.IsOpen())
      self.assertEqual(Status.SUCCESS, dbm.Open(path, True, **open_params))
      self.assertTrue(dbm.IsOpen())
      inspect = dbm.Inspect()
      class_name = inspect["class"]
      self.assertEqual(conf["expected_class"], class_name)
      for i in range(0, 20):
        key = "{:08d}".format(i)
        value = "{:d}".format(i)
        self.assertEqual(Status.SUCCESS, dbm.Set(key, value, False))
      for i in range(0, 20, 2):
        key = "{:08d}".format(i)
        self.assertEqual(Status.SUCCESS, dbm.Remove(key))
      if class_name in ("HashDBM", "TreeDBM", "TinyDBM", "BabyDBM"):
        self.assertEqual(Status.SUCCESS, dbm.Set("日本", "東京"))
        self.assertEqual("東京", dbm.GetStr("日本"))
        self.assertEqual(Status.SUCCESS, dbm.Remove("日本"))
      self.assertEqual(Status.SUCCESS, dbm.Synchronize(False, **conf["synchronize_params"]))
      self.assertEqual(10, dbm.Count())
      self.assertTrue(dbm.GetFileSize() > 0)
      if path:
        self.assertTrue(path in dbm.GetFilePath())
        self.assertTrue(dbm.GetTimestamp() > 0)
      self.assertTrue(dbm.IsWritable())
      self.assertTrue(dbm.IsHealthy())
      if class_name in ("TreeDBM", "SkipDBM", "BabyDBM", "StdTreeDBM"):
        self.assertTrue(dbm.IsOrdered())
      else:
        self.assertFalse(dbm.IsOrdered())
      self.assertEqual(10, len(dbm))
      self.assertTrue(conf["expected_class"] in repr(dbm))
      self.assertTrue(conf["expected_class"] in str(dbm))
      for i in range(0, 20):
        key = "{:08d}".format(i)
        value = "new-{:d}".format(i)
        status = dbm.Set(key, value, False)
        if i % 2 == 0:
          self.assertTrue(status == Status.SUCCESS)
        else:
          self.assertTrue(status == Status.DUPLICATION_ERROR)
      sv = dbm.SetAndGet("98765", "apple", False)
      self.assertEqual(Status.SUCCESS, sv[0])
      self.assertEqual(None, sv[1])
      if class_name in ("TreeDBM", "TreeDBM", "TinyDBM", "BabyDBM"):
        sv = dbm.SetAndGet("98765", "orange", False)
        self.assertEqual(Status.DUPLICATION_ERROR, sv[0])
        self.assertEqual("apple", sv[1])
        sv = dbm.SetAndGet("98765", b"orange", True)
        self.assertEqual(Status.SUCCESS, sv[0])
        self.assertEqual(b"apple", sv[1])
        self.assertEqual("orange", dbm.GetStr("98765"))
        sv = dbm.RemoveAndGet("98765")
        self.assertEqual(Status.SUCCESS, sv[0])
        self.assertEqual("orange", sv[1])
        sv = dbm.RemoveAndGet("98765")
        self.assertEqual(Status.NOT_FOUND_ERROR, sv[0])
        self.assertEqual(None, sv[1])
        self.assertEqual(Status.SUCCESS, dbm.Set("98765", "banana"))
      self.assertEqual(Status.SUCCESS, dbm.Remove("98765"))
      self.assertEqual(Status.SUCCESS, dbm.Synchronize(False, **conf["synchronize_params"]))
      records = {}
      for i in range(0, 20):
        key = "{:08d}".format(i)
        value = "new-{:d}".format(i) if i % 2 == 0 else "{:d}".format(i)
        self.assertEqual(value.encode(), dbm.Get(key.encode()))
        self.assertEqual(value.encode(), dbm.Get(key))
        self.assertEqual(value, dbm.GetStr(key.encode()))
        self.assertEqual(value, dbm.GetStr(key))
        status = Status()
        rec_value = dbm.Get(key.encode(), status)
        self.assertEqual(value.encode(), rec_value)
        self.assertEqual(Status.SUCCESS, status)
        records[key] = value
      self.assertEqual(Status.SUCCESS, dbm.Rebuild(**conf["rebuild_params"]))
      it_records = {}
      iter = dbm.MakeIterator()
      status = Status()
      record = iter.Get(status)
      self.assertEqual(Status.NOT_FOUND_ERROR, status)
      self.assertEqual(None, record)
      self.assertEqual(Status.SUCCESS, iter.First())
      self.assertTrue("0000" in repr(iter))
      self.assertTrue("0000" in str(iter))
      while True:
        status = Status()
        record = iter.Get(status)
        if status != Status.SUCCESS:
          self.assertEqual(Status.NOT_FOUND_ERROR, status)
          break
        self.assertEqual(2, len(record))
        it_records[record[0].decode()] = record[1].decode()
        record_str = iter.GetStr(status)
        self.assertEqual(Status.SUCCESS, status)
        self.assertEqual(2, len(record_str))
        self.assertEqual(record[0].decode(), record_str[0])
        self.assertEqual(record[1].decode(), record_str[1])
        self.assertEqual(record_str[0], iter.GetKeyStr())
        self.assertEqual(record_str[1], iter.GetValueStr())
        self.assertEqual(Status.SUCCESS, iter.Next())
      self.assertEqual(records, it_records)
      it_records = {}
      for key, value in dbm:
        it_records[key.decode()] = value.decode()
      self.assertEqual(records, it_records)
      self.assertEqual(Status.SUCCESS, iter.Jump("00000011"))
      self.assertEqual("00000011", iter.GetKey().decode())
      self.assertEqual("11", iter.GetValue().decode())
      if dbm.IsOrdered():
        self.assertEqual(Status.SUCCESS, iter.Last())
        it_records = {}
        while True:
          status = Status()
          record = iter.Get(status)
          if status != Status.SUCCESS:
            self.assertEqual(Status.NOT_FOUND_ERROR, status)
            break
          self.assertEqual(2, len(record))
          it_records[record[0].decode()] = record[1].decode()
          self.assertEqual(Status.SUCCESS, iter.Previous())
        self.assertEqual(records, it_records)
      if path:
        base, ext = os.path.splitext(path)
        copy_path = base + "-copy" + ext
        self.assertEqual(Status.SUCCESS, dbm.CopyFileData(copy_path))
        copy_dbm = DBM()
        if "." in path:
          self.assertEqual(Status.SUCCESS, copy_dbm.Open(copy_path, False))
        else:
          params = {"dbm": conf["expected_class"]}
          if "num_shards" in open_params:
            params["num_shards"] = 0
          self.assertEqual(Status.SUCCESS, copy_dbm.Open(copy_path, False, **params))
        self.assertTrue(copy_dbm.IsHealthy())
        it_records = {}
        for key, value in copy_dbm:
          it_records[key.decode()] = value.decode()
        self.assertEqual(records, it_records)
        self.assertEqual(Status.SUCCESS, copy_dbm.Close())
        if class_name in ("HashDBM", "TreeDBM"):
          restored_path = copy_path + "-restored"
          self.assertEqual(Status.SUCCESS, DBM.RestoreDatabase(
            copy_path, restored_path, class_name, -1))
      export_dbm = DBM()
      self.assertEqual(Status.SUCCESS, export_dbm.Open("", True, dbm="BabyDBM"))
      self.assertEqual(Status.SUCCESS, dbm.Export(export_dbm))
      it_records = {}
      for key, value in export_dbm:
        it_records[key.decode()] = value.decode()
      self.assertEqual(records, it_records)
      self.assertEqual(Status.SUCCESS, export_dbm.Clear())
      self.assertEqual(0, len(export_dbm))
      self.assertEqual(Status.SUCCESS, export_dbm.Set("1", "100"))
      value = export_dbm.Increment("10000", 2, 10000, status)
      self.assertEqual(Status.SUCCESS, status)
      self.assertEqual(value, 10002)
      value = export_dbm.Increment("10000", Utility.INT64MIN, 0, status)
      self.assertEqual(Status.SUCCESS, status)
      self.assertEqual(value, 10002)
      self.assertEqual(Status.DUPLICATION_ERROR, export_dbm.Set("1", "101", False))
      self.assertEqual(Status.SUCCESS, export_dbm.CompareExchange("1", "100", "101"))
      value = export_dbm.Increment("10000", 2)
      self.assertEqual(value, 10004)
      self.assertEqual(Status.INFEASIBLE_ERROR, export_dbm.CompareExchange("1", "100", "101"))
      self.assertEqual(Status.SUCCESS, export_dbm.CompareExchange("1", "101", None))
      value = export_dbm.Get("1", status)
      self.assertEqual(Status.NOT_FOUND_ERROR, status)
      self.assertEqual(Status.SUCCESS, export_dbm.CompareExchange("1", None, "zzz"))
      self.assertEqual(Status.INFEASIBLE_ERROR, export_dbm.CompareExchange("1", None, "yyy"))
      self.assertEqual("zzz", export_dbm.GetStr("1", status))
      self.assertEqual(Status.SUCCESS, export_dbm.CompareExchange("1", "zzz", None))
      self.assertEqual(Status.INFEASIBLE_ERROR, export_dbm.CompareExchange(
        "xyz", DBM.ANY_DATA, DBM.ANY_DATA))
      self.assertEqual(Status.SUCCESS, export_dbm.CompareExchange("xyz", None, "abc"))
      self.assertEqual(Status.SUCCESS, export_dbm.CompareExchange(
        "xyz", DBM.ANY_DATA, DBM.ANY_DATA))
      self.assertEqual("abc", export_dbm.GetStr("xyz", status))
      self.assertEqual(Status.SUCCESS, export_dbm.CompareExchange(
        "xyz", DBM.ANY_DATA, "def"))
      self.assertEqual("def", export_dbm.GetStr("xyz", status))
      self.assertEqual(Status.SUCCESS, export_dbm.CompareExchange(
        "xyz", DBM.ANY_DATA, None))
      self.assertTrue(export_dbm.GetStr("xyz", status) == None)
      self.assertEqual(Status.SUCCESS, export_dbm.CompareExchangeMulti(
        (("hop", None), ("step", None)),
        (("hop", "one"), ("step", "two"))))
      self.assertEqual("one", export_dbm.GetStr("hop"))
      self.assertEqual("two", export_dbm.GetStr("step"))
      status, value = export_dbm.CompareExchangeAndGet("xyz", None, "123");
      self.assertEqual(Status.SUCCESS, status)
      self.assertEqual(None, value)
      status, value = export_dbm.CompareExchangeAndGet("xyz", "123", DBM.ANY_DATA);
      self.assertEqual(Status.SUCCESS, status)
      self.assertEqual("123", value)
      status, value = export_dbm.CompareExchangeAndGet("xyz", DBM.ANY_DATA, None);
      self.assertEqual(Status.SUCCESS, status)
      self.assertEqual(b"123", value)
      self.assertEqual(Status.INFEASIBLE_ERROR, export_dbm.CompareExchangeMulti(
        (("hop", "one"), ("step", None)),
        (("hop", "uno"), ("step", "dos"))))
      self.assertEqual("one", export_dbm.GetStr("hop"))
      self.assertEqual("two", export_dbm.GetStr("step"))
      self.assertEqual(Status.SUCCESS, export_dbm.CompareExchangeMulti(
        (("hop", "one"), ("step", "two")),
        (("hop", "1"), ("step", "2"))))
      self.assertEqual("1", export_dbm.GetStr("hop"))
      self.assertEqual("2", export_dbm.GetStr("step"))
      self.assertEqual(Status.SUCCESS, export_dbm.CompareExchangeMulti(
        (("hop", "1"), ("step", "2")),
        (("hop", None), ("step", None))))      
      self.assertEqual(None, export_dbm.GetStr("hop"))
      self.assertEqual(None, export_dbm.GetStr("step"))
      self.assertEqual(Status.INFEASIBLE_ERROR, export_dbm.CompareExchangeMulti(
        [("xyz", DBM.ANY_DATA)], [("xyz", "abc")]))
      self.assertEqual(Status.SUCCESS, export_dbm.CompareExchangeMulti(
        [("xyz", None)], [("xyz", "abc")]))
      self.assertEqual(Status.SUCCESS, export_dbm.CompareExchangeMulti(
        [("xyz", DBM.ANY_DATA)], [("xyz", "def")]))
      self.assertEqual("def", export_dbm.GetStr("xyz"))
      self.assertEqual(Status.SUCCESS, export_dbm.CompareExchangeMulti(
        [("xyz", DBM.ANY_DATA)], [("xyz", None)]))
      self.assertEqual(None, export_dbm.GetStr("xyz"))
      export_iter = export_dbm.MakeIterator()
      self.assertEqual(Status.SUCCESS, export_iter.First())
      self.assertEqual(Status.SUCCESS, export_iter.Set("foobar"))
      self.assertEqual(Status.SUCCESS, export_iter.Remove())
      self.assertEqual(0, len(export_dbm))
      self.assertEqual(Status.SUCCESS, export_dbm.Append("foo", "bar", ","))
      self.assertEqual(Status.SUCCESS, export_dbm.Append("foo", "baz", ","))
      self.assertEqual(Status.SUCCESS, export_dbm.Append("foo", "qux", ""))
      self.assertEqual("bar,bazqux", export_dbm.GetStr("foo"))
      export_dbm["abc"] = "defg"
      self.assertEqual("defg", export_dbm["abc"])
      self.assertTrue("abc" in export_dbm)
      del export_dbm["abc"]
      try:
        export_dbm["abc"]
      except StatusException as e:
        self.assertEqual(Status.NOT_FOUND_ERROR, e.GetStatus())
      self.assertFalse("abc" in export_dbm)
      self.assertEqual(Status.SUCCESS, export_dbm.SetMulti(True, one="first", two="second"))
      self.assertEqual(Status.SUCCESS, export_dbm.AppendMulti(":", one="1", two="2"))
      ret_records = export_dbm.GetMulti("one", "two", "three")
      self.assertEqual("first:1".encode(), ret_records.get("one".encode()))
      self.assertEqual("second:2".encode(), ret_records.get("two".encode()))
      self.assertEqual(None, ret_records.get("third".encode()))
      ret_records = export_dbm.GetMultiStr("one", "two", "three")
      self.assertEqual("first:1", ret_records.get("one"))
      self.assertEqual("second:2", ret_records.get("two"))
      self.assertEqual(None, ret_records.get("third"))
      self.assertEqual(Status.SUCCESS, export_dbm.RemoveMulti("one", "two"))
      self.assertEqual(Status.NOT_FOUND_ERROR, export_dbm.RemoveMulti("two", "three"))
      status = Status()
      self.assertEqual(None, export_dbm.Get("one", status))
      self.assertEqual(Status.NOT_FOUND_ERROR, status)
      status = Status()
      self.assertEqual(None, export_dbm.Get("two", status))
      self.assertEqual(Status.NOT_FOUND_ERROR, status)
      status = Status()
      self.assertEqual(None, export_dbm.Get("three", status))
      self.assertEqual(Status.NOT_FOUND_ERROR, status)
      self.assertEqual(Status.SUCCESS, export_dbm.Set("zero", "foo"))
      self.assertEqual(Status.SUCCESS, export_dbm.Rekey("zero", "one", True))
      self.assertEqual(None, export_dbm.Get("zero"))
      self.assertEqual("foo", export_dbm.GetStr("one"))
      step_count = 0
      self.assertEqual(Status.SUCCESS, export_iter.First())
      while True:
        status.Set(Status.UNKNOWN_ERROR)
        record = export_iter.Step(status)
        if not record:
          self.assertEqual(Status.NOT_FOUND_ERROR, status)
          break
        self.assertEqual(Status.SUCCESS, status)
        step_count += 1
      self.assertEqual(export_dbm.Count(), step_count)
      step_count = 0
      self.assertEqual(Status.SUCCESS, export_iter.First())
      while True:
        status.Set(Status.UNKNOWN_ERROR)
        record = export_iter.StepStr(status)
        if not record:
          self.assertEqual(Status.NOT_FOUND_ERROR, status)
          break
        self.assertEqual(Status.SUCCESS, status)
        step_count += 1
      self.assertEqual(export_dbm.Count(), step_count)
      pop_count = 0
      while True:
        status.Set(Status.UNKNOWN_ERROR)
        record = export_dbm.PopFirst(status)
        if not record:
          self.assertEqual(Status.NOT_FOUND_ERROR, status)
          break
        self.assertEqual(Status.SUCCESS, status)
        pop_count += 1
      self.assertEqual(step_count, pop_count)
      self.assertEqual(0, export_dbm.Count())
      self.assertEqual(Status.SUCCESS, export_dbm.SetMulti(
        False, japan="tokyo", china="beijing", korea="seoul", france="paris"))
      pop_count = 0
      while True:
        status.Set(Status.UNKNOWN_ERROR)
        record = export_dbm.PopFirstStr(status)
        if not record:
          self.assertEqual(Status.NOT_FOUND_ERROR, status)
          break
        self.assertEqual(Status.SUCCESS, status)
        pop_count += 1
      self.assertEqual(4, pop_count)
      self.assertEqual(Status.SUCCESS, export_dbm.PushLast("foo", 0))
      record = export_dbm.PopFirst()
      self.assertEqual(record[0], b"\0\0\0\0\0\0\0\0")
      self.assertEqual(record[1], b"foo")
      self.assertEqual(Status.SUCCESS, export_dbm.Close())
      self.assertEqual(Status.SUCCESS, dbm.Close())
      if path:
        self.assertEqual(Status.SUCCESS, dbm.Open(path, False, **open_params))
        self.assertEqual(Status.SUCCESS, dbm.Close())

  # Basic process-related functions.
  def testProcess(self):
    path = self._make_tmp_path("casket.tkh")
    dbm = DBM()
    self.assertEqual(Status.SUCCESS, dbm.Open(path, True, truncate=True, num_buckets=1000))
    self.assertEqual(Status.SUCCESS, dbm.Process("abc", lambda k, v: None, True))
    self.assertEqual(None, dbm.GetStr("abc"))
    self.assertEqual(Status.SUCCESS, dbm.Process("abc", lambda k, v: False, True))
    self.assertEqual(None, dbm.GetStr("abc"))
    self.assertEqual(Status.SUCCESS, dbm.Process("abc", lambda k, v: "ABCDE", True))
    self.assertEqual("ABCDE", dbm.GetStr("abc"))
    def Processor1(k, v):
      self.assertEqual(b"abc", k)
      self.assertEqual(b"ABCDE", v)
      return None
    self.assertEqual(Status.SUCCESS, dbm.Process("abc", Processor1, False))
    self.assertEqual(Status.SUCCESS, dbm.Process("abc", lambda k, v: False, True))
    self.assertEqual(None, dbm.GetStr("abc"))
    def Processor2(k, v):
      self.assertEqual(b"abc", k)
      self.assertEqual(None, v)
      return None
    self.assertEqual(Status.SUCCESS, dbm.Process("abc", Processor2, False))
    for i in range(10):
      self.assertEqual(Status.SUCCESS, dbm.Process(i, lambda k, v: i * i, True))
    self.assertEqual(10, dbm.Count())
    counters = {"empty": 0, "full": 0}
    def Processor3(k, v):
      if k:
        counters["full"] += 1
        self.assertEqual(int(k) ** 2, int(v))
      else:
        counters["empty"] += 1
      return None
    self.assertEqual(Status.SUCCESS, dbm.ProcessEach(Processor3, False))
    self.assertEqual(2, counters["empty"])
    self.assertEqual(10, counters["full"])
    def Processor4(k, v):
      if not k: return
      return int(int(v) ** 0.5)
    self.assertEqual(Status.SUCCESS, dbm.ProcessEach(Processor4, True))
    def Processor5(k, v):
      if not k: return
      self.assertEqual(int(k), int(v))
      return False
    self.assertEqual(Status.SUCCESS, dbm.ProcessEach(Processor5, True))
    self.assertEqual(0, dbm.Count())
    ops = [
      ("one", lambda key, value: "hop"),
      ("two", lambda key, value: "step"),
      ("three", lambda key, value: "jump"),
    ]
    self.assertEqual(Status.SUCCESS, dbm.ProcessMulti(ops, True))
    ops = [
      ("one", lambda key, value: False),
      ("two", lambda key, value: False),
      ("three", lambda key, value: value.decode() * 2 if value else "x"),
      ("four", lambda key, value: value.decode() * 2 if value else "x"),
      ("three", lambda key, value: value.decode() * 2 if value else "x"),
      ("four", lambda key, value: value.decode() * 2 if value else "x"),
    ]
    self.assertEqual(Status.SUCCESS, dbm.ProcessMulti(ops, True))
    self.assertEqual(2, dbm.Count())
    self.assertEqual(None, dbm.GetStr("one"))
    self.assertEqual(None, dbm.GetStr("two"))
    self.assertEqual("jumpjumpjumpjump", dbm.GetStr("three"))
    self.assertEqual("xx", dbm.GetStr("four"))
    self.assertEqual(Status.SUCCESS, dbm.Close())

  # Thread tests.
  def testThread(self):
    dbm = DBM()
    self.assertEqual(Status.SUCCESS, dbm.Open(
      "casket.tkh", True, truncate=True, num_buckets=1000))
    rnd_state = random.Random()
    num_records = 5000
    num_threads = 5
    records = {}
    class Task(threading.Thread):
      def __init__(self, test, thid):
        threading.Thread.__init__(self)
        self.thid = thid
        self.test = test
      def run(self):
        for i in range(0, num_records):
          key_num = rnd_state.randint(1, num_records)
          key_num = key_num - key_num % num_threads + self.thid
          key = str(key_num)
          value = str(key_num * key_num)
          if rnd_state.randint(0, num_records) == 0:
            self.test.assertEqual(Status.SUCCESS, dbm.Rebuild())
          elif rnd_state.randint(0, 10) == 0:
            iter = dbm.MakeIterator()
            iter.Jump(key)
            status = Status()
            record = iter.Get(status)
            if status == Status.SUCCESS:
              self.test.assertEqual(2, len(record))
              self.test.assertEqual(key, record[0].decode())
              self.test.assertEqual(value, record[1].decode())
              status = iter.Next()
              self.test.assertTrue(status == Status.SUCCESS or status == Status.NOT_FOUND_ERROR)
          elif rnd_state.randint(0, 4) == 0:
            status = Status()
            rec_value = dbm.Get(key, status)
            if status == Status.SUCCESS:
              self.test.assertEqual(value, rec_value.decode())
            else:
              self.test.assertEqual(Status.NOT_FOUND_ERROR, status)
          elif rnd_state.randint(0, 4) == 0:
            status = dbm.Remove(key)
            if status == Status.SUCCESS:
              del records[key]
            else:
              self.test.assertEqual(Status.NOT_FOUND_ERROR, status)
          else:
            overwrite = rnd_state.randint(0, 2) == 0
            status = dbm.Set(key, value, overwrite)
            if status == Status.SUCCESS:
              records[key] = value
            else:
              self.test.assertEqual(Status.DUPLICATION_ERROR, status)
          if rnd_state.randint(0, 10) == 0:
            time.sleep(0.00001)
    threads = []
    for thid in range(0, num_threads):
      threads.append(Task(self, thid))
    for th in threads:
      th.start()
    for th in threads:
      th.join()
    it_records = {}
    for key, value in dbm:
      it_records[key.decode()] = value.decode()
    self.assertEqual(records, it_records)
    self.assertEqual(Status.SUCCESS, dbm.Close())

  # Search tests.
  def testSearch(self):
    confs = [
      {"path": "casket.tkh",
       "open_params": {"num_buckets": 100}},
      {"path": "casket.tkt",
       "open_params": {"num_buckets": 100}},
      {"path": "casket.tks",
       "open_params": {"max_level": 8}},
      {"path": "",
       "open_params": {"dbm": "TinyDBM", "num_buckets": 100}},
      {"path": "",
       "open_params": {"dbm": "BabyDBM"}},
    ]
    for conf in confs:
      path = conf["path"]
      path = self._make_tmp_path(path) if path else ""
      dbm = DBM()
      open_params = conf["open_params"].copy()
      open_params["truncate"] = True
      self.assertEqual(Status.SUCCESS, dbm.Open(path, True, **open_params))
      for i in range(1, 101):
        key = "{:08d}".format(i)
        value = "{:d}".format(i)
        self.assertEqual(Status.SUCCESS, dbm.Set(key, value, False))
      self.assertEqual(Status.SUCCESS, dbm.Synchronize(False))
      self.assertEqual(100, dbm.Count())
      self.assertEqual(12, len(dbm.Search("contain", "001")))
      self.assertEqual(3, len(dbm.Search("contain", "001", 3)))
      self.assertEqual(10, len(dbm.Search("begin", "0000001")))
      self.assertEqual(10, len(dbm.Search("end", "1")))
      self.assertEqual(10, len(dbm.Search("regex", r"^\d+1$")))
      self.assertEqual(10, len(dbm.Search("regex", r"^\d+1$", 0,)))
      self.assertEqual(3, len(dbm.Search("edit", "00000100", 3)))
      self.assertEqual(3, len(dbm.Search("editbin", "00000100", 3)))
      with self.assertRaises(StatusException):
        self.assertRaises(dbm.Search("foo", "00000100", 3))
      self.assertEqual(Status.SUCCESS, dbm.Close())

  # Export tests.
  def testExport(self):
    dbm = DBM()
    dest_path = self._make_tmp_path("casket.txt")
    self.assertEqual(Status.SUCCESS, dbm.Open("", True))
    for i in range(1, 101):
      key = "{:08d}".format(i)
      value = "{:d}".format(i)
      self.assertEqual(Status.SUCCESS, dbm.Set(key, value, False))
    file = File()
    self.assertEqual(Status.SUCCESS, file.Open(dest_path, True, truncate=True))
    self.assertEqual(Status.SUCCESS, dbm.ExportToFlatRecords(file))
    self.assertEqual(Status.SUCCESS, dbm.Clear())
    self.assertEqual(0, dbm.Count())
    self.assertEqual(Status.SUCCESS, dbm.ImportFromFlatRecords(file))
    self.assertEqual(100, dbm.Count())
    self.assertEqual(Status.SUCCESS, file.Close())
    file = File()
    self.assertEqual(Status.SUCCESS, file.Open(dest_path, True, truncate=True))
    self.assertEqual(Status.SUCCESS, dbm.ExportKeysAsLines(file))
    self.assertEqual(Status.SUCCESS, file.Close())
    self.assertEqual(Status.SUCCESS, dbm.Close())
    file = File()
    self.assertEqual(Status.SUCCESS, file.Open(dest_path, False))
    self.assertTrue("File" in repr(file))
    self.assertTrue("File" in str(file))
    self.assertEqual(12, len(file.Search("contain", "001")))
    self.assertEqual(3, len(file.Search("contain", "001", 3)))
    self.assertEqual(10, len(file.Search("begin", "0000001")))
    self.assertEqual(10, len(file.Search("end", "1")))
    self.assertEqual(10, len(file.Search("regex", r"^\d+1$")))
    self.assertEqual(10, len(file.Search("regex", r"^\d+1$", 0)))
    self.assertEqual(3, len(file.Search("edit", "00000100", 3)))
    self.assertEqual(3, len(file.Search("editbin", "00000100", 3)))
    with self.assertRaises(StatusException):
      self.assertRaises(file.Search("foo", "00000100", 3))
    self.assertEqual(Status.SUCCESS, file.Close())

  # AsyncDBM tests.
  def testAsyncDBM(self):
    dbm = DBM()
    path = self._make_tmp_path("casket.tkh")
    copy_path = self._make_tmp_path("casket-copy.tkh")
    self.assertEqual(Status.SUCCESS, dbm.Open(path, True, num_buckets=100, concurrent=True))
    adbm = AsyncDBM(dbm, 4)
    self.assertTrue("AsyncDBM" in repr(adbm))
    self.assertTrue("AsyncDBM" in str(adbm))
    set_future = adbm.Set("one", "hop")
    self.assertTrue("Future" in repr(set_future))
    self.assertTrue("Future" in str(set_future))
    set_future.Wait(0)
    self.assertTrue(set_future.Wait())
    self.assertEqual(Status.SUCCESS, set_future.Get())
    self.assertEqual(Status.DUPLICATION_ERROR, adbm.Set("one", "more", False).Get())
    self.assertEqual(Status.SUCCESS, adbm.Set("two", "step", False).Get())
    self.assertEqual(Status.SUCCESS, adbm.Set("three", "jump", False).Get())
    self.assertEqual(Status.SUCCESS, adbm.Append("three", "go", ":").Get())
    get_result = adbm.Get("one").Get()
    self.assertEqual(Status.SUCCESS, get_result[0])
    self.assertEqual(b"hop", get_result[1])
    self.assertEqual("step", adbm.GetStr("two").Get()[1])
    self.assertEqual("jump:go", adbm.GetStr("three").Get()[1])
    self.assertEqual(Status.SUCCESS, adbm.Get("three").Get()[0])
    self.assertEqual(Status.SUCCESS, adbm.Remove("one").Get())
    self.assertEqual(Status.NOT_FOUND_ERROR, adbm.Remove("one").Get())
    self.assertEqual(Status.SUCCESS, adbm.Remove("two").Get())
    self.assertEqual(Status.SUCCESS, adbm.Remove("three").Get())
    self.assertEqual(0, dbm.Count())
    set_future = adbm.SetMulti(False, one="hop", two="step", three="jump")
    self.assertEqual(Status.SUCCESS, set_future.Get())
    self.assertEqual(Status.SUCCESS, adbm.AppendMulti(":", three="go").Get())
    get_result = adbm.GetMulti("one", "two").Get()
    self.assertEqual(Status.SUCCESS, get_result[0])
    self.assertEqual(b"hop", get_result[1][b"one"])
    self.assertEqual(b"step", get_result[1][b"two"])
    get_result = adbm.GetMultiStr("one", "two", "three", "hoge").Get()
    self.assertEqual(Status.NOT_FOUND_ERROR, get_result[0])
    self.assertEqual("hop", get_result[1]["one"])
    self.assertEqual("step", get_result[1]["two"])
    self.assertEqual("jump:go", get_result[1]["three"])
    self.assertEqual(Status.SUCCESS, adbm.RemoveMulti("one", "two", "three").Get())
    self.assertEqual(0, dbm.Count())
    incr_result = adbm.Increment("num", 5, 100).Get()
    self.assertEqual(Status.SUCCESS, incr_result[0])
    self.assertEqual(105, incr_result[1])
    self.assertEqual(110, adbm.Increment("num", 5, 100).Get()[1])
    self.assertEqual(Status.SUCCESS, adbm.Remove("num").Get())
    self.assertEqual(Status.SUCCESS, adbm.CompareExchange("one", None, "ichi").Get())
    self.assertEqual("ichi", adbm.GetStr("one").Get()[1])
    self.assertEqual(Status.SUCCESS, adbm.CompareExchange("one", "ichi", "ni").Get())
    self.assertEqual("ni", adbm.GetStr("one").Get()[1])
    self.assertEqual(Status.INFEASIBLE_ERROR, adbm.CompareExchange(
      "xyz", DBM.ANY_DATA, DBM.ANY_DATA).Get())
    self.assertEqual(Status.SUCCESS, adbm.CompareExchange(
      "xyz", None, "abc").Get())
    self.assertEqual(Status.SUCCESS, adbm.CompareExchange(
      "xyz", DBM.ANY_DATA, DBM.ANY_DATA).Get())
    self.assertEqual("abc", adbm.GetStr("xyz").Get()[1])
    self.assertEqual(Status.SUCCESS, adbm.CompareExchange("xyz", DBM.ANY_DATA, "def").Get())
    self.assertEqual("def", adbm.GetStr("xyz").Get()[1])
    self.assertEqual(Status.SUCCESS, adbm.CompareExchange("xyz", DBM.ANY_DATA, None).Get())
    self.assertEqual(Status.NOT_FOUND_ERROR, adbm.GetStr("xyz").Get()[0])
    self.assertEqual(Status.SUCCESS, adbm.CompareExchangeMulti(
      [("one", "ni"), ("two", None)], [("one", "san"), ("two", "uno")]).Get())
    self.assertEqual("san", adbm.GetStr("one").Get()[1])
    self.assertEqual("uno", adbm.GetStr("two").Get()[1])
    self.assertEqual(Status.SUCCESS, adbm.CompareExchangeMulti(
      [("one", "san"), ("two", "uno")], [("one", None), ("two", None)]).Get())
    self.assertEqual(Status.INFEASIBLE_ERROR, adbm.CompareExchangeMulti(
      [("xyz", DBM.ANY_DATA)], [("xyz", "abc")]).Get())
    self.assertEqual(Status.SUCCESS, adbm.CompareExchangeMulti(
      [("xyz", None)], [("xyz", "abc")]).Get())
    self.assertEqual(Status.SUCCESS, adbm.CompareExchangeMulti(
      [("xyz", DBM.ANY_DATA)], [("xyz", "abc")]).Get())
    self.assertEqual("abc", adbm.GetStr("xyz").Get()[1])
    self.assertEqual(Status.SUCCESS, adbm.CompareExchangeMulti(
      [("xyz", DBM.ANY_DATA)], [("xyz", None)]).Get())
    self.assertEqual(Status.NOT_FOUND_ERROR, adbm.GetStr("xyz").Get()[0])
    self.assertEqual(0, dbm.Count())
    self.assertEqual(Status.SUCCESS, adbm.Set("hello", "world", False).Get())
    self.assertEqual(Status.SUCCESS, adbm.Synchronize(False).Get())
    self.assertEqual(Status.SUCCESS, adbm.Rebuild().Get())
    self.assertEqual(1, dbm.Count())
    self.assertEqual(Status.SUCCESS, adbm.CopyFileData(copy_path).Get())
    copy_dbm = DBM()
    self.assertEqual(Status.SUCCESS, copy_dbm.Open(copy_path, True))
    self.assertEqual(1, copy_dbm.Count())
    self.assertEqual(Status.SUCCESS, copy_dbm.Clear())
    self.assertEqual(0, copy_dbm.Count())
    self.assertEqual(Status.SUCCESS, adbm.Export(copy_dbm).Get())
    self.assertEqual(1, copy_dbm.Count())
    self.assertEqual(Status.SUCCESS, copy_dbm.Close())
    os.remove(copy_path)
    copy_file = File()
    self.assertEqual(Status.SUCCESS, copy_file.Open(copy_path, True))
    self.assertEqual(Status.SUCCESS, adbm.ExportToFlatRecords(copy_file).Get())
    self.assertEqual(Status.SUCCESS, adbm.Clear().Get())
    self.assertEqual(0, dbm.Count())
    self.assertEqual(Status.SUCCESS, adbm.ImportFromFlatRecords(copy_file).Get())
    self.assertEqual(1, dbm.Count())
    self.assertEqual(Status.SUCCESS, copy_file.Close())
    async def async_main():
      await adbm.Set("hello", "good-bye", True)
      await adbm.Set("hi", "bye", True)
      await adbm.Set("chao", "adios", True)
    asyncio.run(async_main())
    self.assertEqual("good-bye", dbm.GetStr("hello"))
    search_result = adbm.Search("begin", "h").Get()
    self.assertEqual(Status.SUCCESS, search_result[0])
    self.assertEqual(2, len(search_result[1]))
    self.assertTrue("hello" in search_result[1])
    self.assertTrue("hi" in search_result[1])
    self.assertEqual(Status.SUCCESS, adbm.Clear().Get())
    self.assertEqual(Status.SUCCESS, adbm.Set("aa", "AAA").Get())
    self.assertEqual(Status.SUCCESS, adbm.Rekey("aa", "bb").Get())
    get_result = adbm.GetStr("bb").Get()
    self.assertEqual(Status.SUCCESS, get_result[0])
    self.assertEqual("AAA", get_result[1])
    pop_result = adbm.PopFirst().Get()
    self.assertEqual(Status.SUCCESS, pop_result[0])
    self.assertEqual(b"bb", pop_result[1])
    self.assertEqual(b"AAA", pop_result[2])
    self.assertEqual(Status.SUCCESS, adbm.Set("cc", "CCC").Get())
    pop_result = adbm.PopFirstStr().Get()
    self.assertEqual(Status.SUCCESS, pop_result[0])
    self.assertEqual("cc", pop_result[1])
    self.assertEqual("CCC", pop_result[2])
    self.assertEqual(Status.SUCCESS, adbm.PushLast("foo", 0).Get())
    pop_result = adbm.PopFirst().Get()
    self.assertEqual(Status.SUCCESS, pop_result[0])
    self.assertEqual(b"\0\0\0\0\0\0\0\0", pop_result[1])
    self.assertEqual(b"foo", pop_result[2])
    adbm.Destruct()
    self.assertEqual(Status.SUCCESS, dbm.Close())
    
  # File tests.
  def testFile(self):
    path = self._make_tmp_path("casket.txt")
    file = File()
    self.assertEqual(Status.SUCCESS, file.Open(
      path, True, truncate=True, file="pos-atom", block_size=512,
      access_options="padding:pagecache"))
    self.assertEqual(Status.SUCCESS, file.Write(5, "12345"))
    self.assertEqual(Status.SUCCESS, file.Write(0, "ABCDE"))
    self.assertEqual(10, file.Append("FGH"))
    self.assertEqual(13, file.Append("IJ"))
    self.assertEqual(15, file.GetSize())
    self.assertTrue(file.GetPath().find("casket.txt") > 0)
    self.assertEqual(Status.SUCCESS, file.Synchronize(False))
    self.assertEqual(Status.SUCCESS, file.Truncate(12))
    self.assertEqual(12, file.GetSize())
    self.assertEqual(b"ABCDE12345FG", file.Read(0, 12))
    self.assertEqual("ABCDE12345FG", file.ReadStr(0, 12))
    self.assertEqual(b"DE123", file.Read(3, 5))
    self.assertEqual("DE123", file.ReadStr(3, 5))
    status = Status()
    self.assertEqual(None, file.ReadStr(1024, 10, status))
    self.assertEqual(Status.INFEASIBLE_ERROR, status)
    self.assertEqual(Status.SUCCESS, file.Close())
    self.assertEqual(Status.SUCCESS, file.Open(path, False))
    self.assertEqual(512, file.GetSize())
    self.assertEqual("E12345F", file.ReadStr(4, 7))
    self.assertEqual(Status.SUCCESS, file.Close())

  # Index tests.
  def testIndex(self):
    path = self._make_tmp_path("casket.tkt")
    index = Index()
    self.assertEqual(Status.SUCCESS, index.Open(
      path, True, truncate=True, num_buckets=100))
    self.assertTrue("Index" in repr(index))
    self.assertTrue(("path=" + path) in str(index))
    self.assertFalse(("single", "1") in index)
    self.assertFalse(("double", "11") in index)
    self.assertEqual(Status.SUCCESS, index.Add("single", "1"))
    self.assertEqual(Status.SUCCESS, index.Add("double", "11"))
    self.assertTrue(("single", "1") in index)
    self.assertTrue(("double", "11") in index)
    self.assertEqual(Status.SUCCESS, index.Add("single", "2"))
    self.assertEqual(Status.SUCCESS, index.Add("double", "22"))
    self.assertEqual(Status.SUCCESS, index.Add("triple", "222"))
    values = index.GetValues("single")
    self.assertEqual(2, len(values))
    self.assertEqual(b"1", values[0])
    self.assertEqual(b"2", values[1])
    values = index.GetValuesStr("triple", 0)
    self.assertEqual(1, len(values))
    self.assertEqual("222", values[0])
    self.assertEqual(0, len(index.GetValuesStr("foo")))
    self.assertEqual(Status.SUCCESS, index.Remove("single", "1"))
    self.assertEqual(Status.SUCCESS, index.Remove("double", "11"))
    self.assertEqual(Status.NOT_FOUND_ERROR, index.Remove("triple", "x"))
    values = index.GetValuesStr("double")
    self.assertEqual(1, len(values))
    self.assertEqual("22", values[0])
    self.assertEqual(3, index.Count())
    self.assertEqual(path, index.GetFilePath())
    self.assertEqual(Status.SUCCESS, index.Synchronize(False))
    self.assertEqual(Status.SUCCESS, index.Rebuild())
    self.assertEqual(3, len(index))
    self.assertEqual(Status.SUCCESS, index.Clear())
    self.assertEqual(0, index.Count())
    self.assertTrue(index.IsOpen())
    self.assertTrue(index.IsWritable())
    self.assertEqual(Status.SUCCESS, index.Add("first", "1"))
    self.assertEqual(Status.SUCCESS, index.Add("second", "22"))
    self.assertEqual(Status.SUCCESS, index.Add("third", "333"))
    iter = index.MakeIterator()
    self.assertTrue("IndexIterator" in repr(iter))
    self.assertTrue("unlocated" in str(iter))
    iter.First()
    self.assertTrue("first" in repr(iter))
    self.assertTrue("first" in str(iter))
    record = iter.Get()
    self.assertTrue(record != None)
    self.assertTrue(b"first", record[0])
    self.assertTrue(b"1", record[1])
    iter.Next()
    record = iter.Get()
    self.assertTrue(record != None)
    self.assertTrue(b"second", record[0])
    self.assertTrue(b"22", record[1])
    iter.Next()
    record = iter.Get()
    self.assertTrue(record != None)
    self.assertTrue(b"third", record[0])
    self.assertTrue(b"333", record[1])
    iter.Next()
    self.assertEqual(None, iter.Get())
    iter.Last()
    record = iter.GetStr()
    self.assertTrue(record != None)
    self.assertTrue("third", record[0])
    self.assertTrue("333", record[1])
    iter.Previous()
    record = iter.GetStr()
    self.assertTrue(record != None)
    self.assertTrue("second", record[0])
    self.assertTrue("22", record[1])
    iter.Previous()
    record = iter.GetStr()
    self.assertTrue(record != None)
    self.assertTrue("first", record[0])
    self.assertTrue("11", record[1])
    iter.Previous()
    self.assertEqual(None, iter.GetStr())
    iter.Jump("second", "")
    record = iter.GetStr()
    self.assertTrue(record != None)
    self.assertTrue("second", record[0])
    self.assertTrue("22", record[1])
    records = {}
    for key, value in index:
      records[key.decode()] = value.decode()
    self.assertEqual(3, len(records))
    self.assertEqual("1", records["first"])
    self.assertEqual("22", records["second"])
    self.assertEqual("333", records["third"])
    self.assertEqual(Status.SUCCESS, index.Close())


# Main routine.
def main(argv):
  test_names = argv
  if test_names:
    test_suite = unittest.TestSuite()
    for test_name in test_names:
      test_suite.addTest(TestTkrzw(test_name))
  else:
    test_suite = unittest.TestLoader().loadTestsFromTestCase(TestTkrzw)
  unittest.TextTestRunner(verbosity=2).run(test_suite)
  return 0


if __name__ == "__main__":
  sys.exit(main(sys.argv[1:]))


# END OF FILE
