#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Coroutine tests
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
import time
import tkrzw

NUM_ITERS = 100
NUM_COROUTINES = 1000
NUM_REPEATS = 100

async def set_sync(dbm, cor_id, iter_count):
  for i in range(0, NUM_REPEATS):
    key = str(cor_id * NUM_COROUTINES * NUM_REPEATS + iter_count * NUM_REPEATS + i)
    dbm.Set(key, key).OrDie()

async def set_async(adbm, cor_id, iter_count):
  futures = []
  for i in range(0, NUM_REPEATS):
    key = str(cor_id * NUM_COROUTINES * NUM_REPEATS + iter_count * NUM_REPEATS + i)
    futures.append(adbm.Set(key, key))
  for future in futures:
    future.Get().OrDie()

async def get_sync(dbm, cor_id, iter_count):
  for i in range(0, NUM_REPEATS):
    key = str(cor_id * NUM_COROUTINES * NUM_REPEATS + iter_count * NUM_REPEATS + i)
    status = tkrzw.Status()
    dbm.Get(key, status)
    status.OrDie()

async def get_async(adbm, cor_id, iter_count):
  futures = []
  for i in range(0, NUM_REPEATS):
    key = str(cor_id * NUM_COROUTINES * NUM_REPEATS + iter_count * NUM_REPEATS + i)
    futures.append(adbm.Get(key))
  for future in futures:
    status, value = future.Get()
    status.OrDie()

async def main():
  dbm = tkrzw.DBM()
  num_buckets = NUM_ITERS * NUM_COROUTINES * NUM_REPEATS / 2
  dbm.Open("casket.tkh", True, concurrent=True, truncate=True,
       num_buckets=num_buckets, file="pos-para")
  adbm = tkrzw.AsyncDBM(dbm, 4)
  def make_set_sync(cor_id, iter_count):
    return set_sync(dbm, cor_id, iter_count)
  def make_set_async(cor_id, iter_count):
    return set_async(adbm, cor_id, iter_count)
  def make_get_sync(cor_id, iter_count):
    return get_sync(dbm, cor_id, iter_count)
  def make_get_async(cor_id, iter_count):
    return get_async(adbm, cor_id, iter_count)
  confs = [
    {"label": "SET SYNC", "op": make_set_sync},
    {"label": "SET ASYNC", "op": make_set_async},
    {"label": "GET SYNC", "op": make_get_sync},
    {"label": "GET ASYNC", "op": make_get_async},
  ]
  for conf in confs:
    start_time = time.time()  
    for iter_count in range(0, NUM_ITERS):
      coroutines = []
      for cor_id in range(0, NUM_COROUTINES):
        coroutines.append(conf["op"](cor_id, iter_count))
      for coroutine in coroutines:
        await coroutine
    end_time = time.time()
    elapsed = end_time - start_time
    print("{:10s}: {:8.0f} QPS".format(
      conf["label"], (NUM_ITERS * NUM_COROUTINES * NUM_REPEATS) / elapsed))
  adbm.Destruct()
  dbm.Close()

asyncio.run(main())

# END OF FILE
