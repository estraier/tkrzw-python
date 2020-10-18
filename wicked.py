#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Wicked test cases
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
    prog="wicked.py", description="Random operation tests.",
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
  print("path: {}".format(path))
  print("params: {}".format(",".join(open_params_exprs)))
  print("num_iterations: {}".format(num_iterations))
  print("num_threads: {}".format(num_threads))
  print("")
  open_params["truncate"] = True
  start_mem_usage = Utility.GetMemoryUsage()
  dbm = DBM()
  dbm.Open(path, True, **open_params).OrDie()
  class Task(threading.Thread):
    def __init__(self, thid):
      threading.Thread.__init__(self)
      self.thid = thid
    def run(self):
      rnd_state = random.Random()
      for i in range(0, num_iterations):
        key_num = rnd_state.randint(1, num_iterations)
        key = "{:d}".format(key_num)
        value = "{:d}".format(i)
        if rnd_state.randint(0, num_iterations / 2) == 0:
          dbm.Rebuild().OrDie()
        elif rnd_state.randint(0, num_iterations / 2) == 0:
          dbm.Clear().OrDie()
        elif rnd_state.randint(0, num_iterations / 2) == 0:
          dbm.Synchronize(False).OrDie()
        elif rnd_state.randint(0, 100) == 0:
          it = dbm.MakeIterator()
          if dbm.IsOrdered() and rnd_state.randint(0, 3) == 0:
            if rnd_state.randint(0, 3) == 0:
              it.Jump(key)
            else:
              it.Last()
            while rnd_state.randint(0, 10) == 0:
              status = Status()
              it.Get(status)
              if status != Status.NOT_FOUND_ERROR:
                status.OrDie()
              it.Previous()
          else:
            if rnd_state.randint(0, 3) == 0:
              it.Jump(key)
            else:
              it.First()
            while rnd_state.randint(0, 10) == 0:
              status = Status()
              it.Get(status)
              if status != Status.NOT_FOUND_ERROR:
                status.OrDie()
              it.Next()
        elif rnd_state.randint(0, 3) == 0:
          status = Status()
          dbm.Get(key, status)
          if status != Status.NOT_FOUND_ERROR:
            status.OrDie()
        elif rnd_state.randint(0, 3) == 0:
          status = dbm.Remove(key)
          if status != Status.NOT_FOUND_ERROR:
            status.OrDie()
        elif rnd_state.randint(0, 3) == 0:
          status = dbm.Set(key, value, False)
          if status != Status.DUPLICATION_ERROR:
            status.OrDie()
        else:
          dbm.Set(key, value).OrDie()
        seq = i + 1
        if self.thid == 0 and seq % (num_iterations / 500) == 0:
          print(".", end="")
          if seq % (num_iterations / 10) == 0:
            print(" ({:08d})".format(seq))
          sys.stdout.flush()
  print("Doing:")
  start_time = time.time()
  threads = []
  for thid in range(0, num_threads):
    th = Task(thid)
    th.start()
    threads.append(th)
  for th in threads:
    th.join()
  status = dbm.Synchronize(False).OrDie()
  end_time = time.time()
  elapsed = end_time - start_time
  mem_usage = Utility.GetMemoryUsage() - start_mem_usage
  print("Done: num_records={:d} file_size={:d} time={:.3f} qps={:.0f} mem={:d}".format(
    dbm.Count(), dbm.GetFileSize() or -1,
    elapsed, num_iterations * num_threads / elapsed, mem_usage))
  print("")
  dbm.Close().OrDie()
  return 0


if __name__ == "__main__":
  sys.exit(main(sys.argv[1:]))


# END OF FILE
