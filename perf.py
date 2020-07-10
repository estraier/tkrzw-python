#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Performance tests
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

import argparse
import sys
import os
import re
import random
import time
import threading
import shutil

from tkrzw import *


# main routine
def main(argv):
  ap = argparse.ArgumentParser(
    prog="perf.py", description="Performance Checker",
    formatter_class=argparse.RawDescriptionHelpFormatter)
  ap.add_argument("--path", default="")
  ap.add_argument("--params", default="")
  ap.add_argument("--iter", type=int, default=10000)
  ap.add_argument("--threads", type=int, default=1)
  ap.add_argument("--random", action='store_true', default=False)
  args = ap.parse_args(argv)
  path = args.path
  open_params = {}
  open_params_exprs = []
  for field in args.params.split(","):
    columns = field.split("=", 1)
    if len(columns) == 2:
      open_params[columns[0]] = columns[1]
      open_params_exprs.append(columns[0] + "=" + columns[1])
  num_iterations = args.iter
  num_threads = args.threads
  is_random = args.random
  print("path: {}".format(path))
  print("params: {}".format(",".join(open_params_exprs)))
  print("num_iterations: {}".format(num_iterations))
  print("num_threads: {}".format(num_threads))
  print("is_random: {}".format(is_random))
  print("")
  open_params["truncate"] = True
  start_mem_usage = Utility.GetMemoryUsage()
  dbm = DBM()
  dbm.Open(path, True, **open_params).OrDie()
  class Setter(threading.Thread):
    def __init__(self, thid):
      threading.Thread.__init__(self)
      self.thid = thid
    def run(self):
      rnd_state = random.Random(self.thid)
      for i in range(0, num_iterations):
        if is_random:
          key_num = rnd_state.randint(1, num_iterations)
        else:
          key_num = self.thid * num_iterations + i
        key = "{:08d}".format(key_num)
        dbm.Set(key, key).OrDie()
        seq = i + 1
        if self.thid == 0 and seq % (num_iterations / 500) == 0:
          print(".", end="")
          if seq % (num_iterations / 10) == 0:
            print(" ({:08d})".format(seq))
          sys.stdout.flush()
  print("Setting:")
  start_time = time.time()
  threads = []
  for thid in range(0, num_threads):
    th = Setter(thid)
    th.start()
    threads.append(th)
  for th in threads:
    th.join()
  dbm.Synchronize(False).OrDie()
  end_time = time.time()
  elapsed = end_time - start_time
  mem_usage = Utility.GetMemoryUsage() - start_mem_usage
  print("Setting done: num_records={:d} file_size={:d} time={:.3f} qps={:.0f} mem={:d}".format(
    dbm.Count(), dbm.GetFileSize() or -1,
    elapsed, num_iterations * num_threads / elapsed, mem_usage))
  print("")  
  class Getter(threading.Thread):
    def __init__(self, thid):
      threading.Thread.__init__(self)
      self.thid = thid
    def run(self):
      rnd_state = random.Random(self.thid)
      for i in range(0, num_iterations):
        if is_random:
          key_num = rnd_state.randint(1, num_threads * num_iterations)
        else:
          key_num = self.thid * num_iterations + i
        key = "{:08d}".format(key_num)
        status = Status()
        value = dbm.Get(key, status)
        if status != Status.SUCCESS and status != Status.NOT_FOUND_ERROR:
          raise RuntimeError("Get failed: " + str(status))
        seq = i + 1
        if self.thid == 0 and seq % (num_iterations / 500) == 0:
          print(".", end="")
          if seq % (num_iterations / 10) == 0:
            print(" ({:08d})".format(seq))
          sys.stdout.flush()
  print("Getting:")
  start_time = time.time()
  threads = []
  for thid in range(0, num_threads):
    th = Getter(thid)
    th.start()
    threads.append(th)
  for th in threads:
    th.join()
  end_time = time.time()
  elapsed = end_time - start_time
  mem_usage = Utility.GetMemoryUsage() - start_mem_usage
  print("Getting done: num_records={:d} file_size={:d} time={:.3f} qps={:.0f} mem={:d}".format(
    dbm.Count(), dbm.GetFileSize() or -1,
    elapsed, num_iterations * num_threads / elapsed, mem_usage))
  print("")
  class Remover(threading.Thread):
    def __init__(self, thid):
      threading.Thread.__init__(self)
      self.thid = thid
    def run(self):
      rnd_state = random.Random(self.thid)
      for i in range(0, num_iterations):
        if is_random:
          key_num = rnd_state.randint(1, num_iterations)
        else:
          key_num = self.thid * num_iterations + i
        key = "{:08d}".format(key_num)
        status = dbm.Remove(key)
        if status != Status.SUCCESS and status != Status.NOT_FOUND_ERROR:
          raise RuntimeError("Remove failed: " + str(status))
        seq = i + 1
        if self.thid == 0 and seq % (num_iterations / 500) == 0:
          print(".", end="")
          if seq % (num_iterations / 10) == 0:
            print(" ({:08d})".format(seq))
          sys.stdout.flush()
  print("Removing:")
  start_time = time.time()
  threads = []
  for thid in range(0, num_threads):
    th = Remover(thid)
    th.start()
    threads.append(th)
  for th in threads:
    th.join()
  dbm.Synchronize(False).OrDie()
  end_time = time.time()
  elapsed = end_time - start_time
  mem_usage = Utility.GetMemoryUsage() - start_mem_usage
  print("Removing done: num_records={:d} file_size={:d} time={:.3f} qps={:.0f} mem={:d}".format(
    dbm.Count(), dbm.GetFileSize() or -1,
    elapsed, num_iterations * num_threads / elapsed, mem_usage))
  print("")  
  dbm.Close().OrDie()
  return 0


if __name__ == "__main__":
  sys.exit(main(sys.argv[1:]))


# END OF FILE
