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
    try:
      status.OrDie()
    except StatusException as e:
      self.assertTrue("foobar" in str(e))
    else:
      self.fail("no exception")

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
       {"num_shards": 4, "dbm": "hash", "num_buckets": 100, "lock_mem_buckets": False},
       "rebuild_params": {},
       "synchronize_params": {},
       "expected_class": "HashDBM"},
    ]
    for conf in confs:
      path = conf["path"]
      path = self._make_tmp_path(path) if path else ""
      dbm = DBM()
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
      self.assertEqual(Status.SUCCESS, dbm.Synchronize(False, **conf["synchronize_params"]))
      self.assertEqual(10, dbm.Count())
      self.assertTrue(dbm.GetFileSize() > 0)
      if path:
        self.assertTrue(path in dbm.GetFilePath())
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
      record = iter.Get(status);
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
      self.assertEqual("00000011", iter.GetKey().decode());
      self.assertEqual("11", iter.GetValue().decode());
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
      value = export_dbm.Increment("10000", Utility.INT64MIN, 0, status);
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
      self.assertEqual(Status.SUCCESS, export_dbm.CompareExchangeMulti(
        (("hop", None), ("step", None)),
        (("hop", "one"), ("step", "two"))))
      self.assertEqual("one", export_dbm.GetStr("hop"))
      self.assertEqual("two", export_dbm.GetStr("step"))
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
      iter = export_dbm.MakeIterator()
      self.assertEqual(Status.SUCCESS, iter.First())
      self.assertEqual(Status.SUCCESS, iter.Set("foobar"))
      self.assertEqual(Status.SUCCESS, iter.Remove())
      self.assertEqual(0, len(export_dbm))
      self.assertEqual(Status.SUCCESS, export_dbm.Append("foo", "bar", ","))
      self.assertEqual(Status.SUCCESS, export_dbm.Append("foo", "baz", ","))
      self.assertEqual(Status.SUCCESS, export_dbm.Append("foo", "qux", ""))
      self.assertEqual("bar,bazqux", export_dbm.GetStr("foo"))
      export_dbm["abc"] = "defg"
      self.assertEqual("defg", export_dbm["abc"])
      del export_dbm["abc"]
      try:
        export_dbm["abc"]
      except StatusException as e:
        self.assertEqual(Status.NOT_FOUND_ERROR, e.GetStatus())
      self.assertEqual(Status.SUCCESS, export_dbm.SetMulti(one="first", two="second"))
      ret_records = export_dbm.GetMulti("one", "two", "three")
      self.assertEqual("first".encode(), ret_records.get("one".encode()))
      self.assertEqual("second".encode(), ret_records.get("two".encode()))
      self.assertEqual(None, ret_records.get("third".encode()))
      ret_records = export_dbm.GetMultiStr("one", "two", "three")
      self.assertEqual("first", ret_records.get("one"))
      self.assertEqual("second", ret_records.get("two"))
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
      self.assertEqual(Status.SUCCESS, export_dbm.Close())
      self.assertEqual(Status.SUCCESS, dbm.Close())

  # Iterator tests.
  def testIterator(self):
    confs = [
      {"path": "casket.tkt",
       "open_params": {"num_buckets": 100, "max_page_size": 50, "max_branches": 2}},
      {"path": "casket.tks",
       "open_params": {"step_unit": 3, "max_level": 3}},
      {"path": "casket.tkt",
       "open_params": {"num_shards": 4,
                       "num_buckets": 100, "max_page_size": 50, "max_branches": 2}},
    ]
    for conf in confs:
      path = conf["path"]
      path = self._make_tmp_path(path) if path else ""
      dbm = DBM()
      open_params = conf["open_params"].copy()
      open_params["truncate"] = True
      self.assertEqual(Status.SUCCESS, dbm.Open(path, True, **open_params))
      iter = dbm.MakeIterator()
      self.assertEqual(Status.SUCCESS, iter.First())
      self.assertEqual(Status.SUCCESS, iter.Last())
      self.assertEqual(Status.SUCCESS, iter.Jump(""))
      self.assertEqual(Status.SUCCESS, iter.JumpLower("", True))
      self.assertEqual(Status.SUCCESS, iter.JumpUpper("", True))
      for i in range(1, 101):
        key = "{:03d}".format(i)
        value = str(i * i);
        self.assertEqual(Status.SUCCESS, dbm.Set(key, value, False))
      self.assertEqual(Status.SUCCESS, dbm.Synchronize(False))
      self.assertEqual(Status.SUCCESS, iter.First())
      self.assertEqual("001", iter.GetKeyStr())
      self.assertEqual("1", iter.GetValueStr())
      self.assertEqual(Status.SUCCESS, iter.Last())
      self.assertEqual("100", iter.GetKeyStr())
      self.assertEqual("10000", iter.GetValueStr())
      self.assertEqual(Status.SUCCESS, iter.Jump("050"))
      self.assertEqual("050", iter.GetKeyStr())
      self.assertEqual(Status.SUCCESS, iter.JumpLower("050", True))
      self.assertEqual("050", iter.GetKeyStr())
      self.assertEqual(Status.SUCCESS, iter.Previous())
      self.assertEqual("049", iter.GetKeyStr())
      self.assertEqual(Status.SUCCESS, iter.JumpLower("050", False))
      self.assertEqual("049", iter.GetKeyStr())
      self.assertEqual(Status.SUCCESS, iter.Next())
      self.assertEqual("050", iter.GetKeyStr())
      self.assertEqual(Status.SUCCESS, iter.JumpUpper("050", True))
      self.assertEqual("050", iter.GetKeyStr())
      self.assertEqual(Status.SUCCESS, iter.Previous())
      self.assertEqual("049", iter.GetKeyStr())
      self.assertEqual(Status.SUCCESS, iter.JumpUpper("050", False))
      self.assertEqual("051", iter.GetKeyStr())
      self.assertEqual(Status.SUCCESS, iter.Next())
      self.assertEqual("052", iter.GetKeyStr())
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
    test = self
    class Task(threading.Thread):
      def __init__(self, test, thid):
        threading.Thread.__init__(self)
        self.thid = thid
        test = test
      def run(self):
        for i in range(0, num_records):
          key_num = rnd_state.randint(1, num_records)
          key_num = key_num - key_num % num_threads + self.thid;
          key = str(key_num)
          value = str(key_num * key_num)
          if rnd_state.randint(0, num_records) == 0:
            test.assertEqual(Status.SUCCESS, dbm.Rebuild())
          elif rnd_state.randint(0, 10) == 0:
            iter = dbm.MakeIterator()
            iter.Jump(key)
            status = Status()
            record = iter.Get(status)
            if status == Status.SUCCESS:
              test.assertEqual(2, len(record))
              test.assertEqual(key, record[0].decode())
              test.assertEqual(value, record[1].decode())
              iter.Next().OrDie();
          elif rnd_state.randint(0, 4) == 0:
            status = Status()
            rec_value = dbm.Get(key, status)
            if status == Status.SUCCESS:
              test.assertEqual(value, rec_value.decode())
            else:
              test.assertEqual(Status.NOT_FOUND_ERROR, status)
          elif rnd_state.randint(0, 4) == 0:
            status = dbm.Remove(key)
            if status == Status.SUCCESS:
              del records[key]
            else:
              test.assertEqual(Status.NOT_FOUND_ERROR, status)
          else:
            overwrite = rnd_state.randint(0, 2) == 0
            status = dbm.Set(key, value, overwrite)
            if status == Status.SUCCESS:
              records[key] = value
            else:
              test.assertEqual(Status.DUPLICATION_ERROR, status)
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
    self.assertEqual(Status.SUCCESS, dbm.ExportRecordsToFlatRecords(file))
    self.assertEqual(Status.SUCCESS, dbm.Clear());
    self.assertEqual(0, dbm.Count());
    self.assertEqual(Status.SUCCESS, dbm.ImportRecordsFromFlatRecords(file))
    self.assertEqual(100, dbm.Count());
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
