#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Example for basic usage of the hash database
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
import tkrzw

async def main():
    # Prepares the database.
    dbm = tkrzw.DBM()
    dbm.Open("casket.tkh", True, truncate=True, num_buckets=100)

    # Prepares the asynchronous adapter with 4 worker threads.
    adbm = tkrzw.AsyncDBM(dbm, 4)

    # Executes the Set method asynchronously.
    future = adbm.Set("hello", "world")
    # Does something in the foreground.
    print("Setting a record")
    # Checks the result after awaiting the Set operation.
    # Calling Future#Get doesn't yield the coroutine ownership.
    status = future.Get()
    if status != tkrzw.Status.SUCCESS:
        print("ERROR: " + str(status))

    # Executes the Get method asynchronously.
    future = adbm.GetStr("hello")
    # Does something in the foreground.
    print("Getting a record")
    # Awaits the operation while yielding the coroutine ownership.
    await future
    status, value = future.Get()
    if status == tkrzw.Status.SUCCESS:
        print("VALUE: " + value)

    # Releases the asynchronous adapter.
    adbm.Destruct()

    # Closes the database.
    dbm.Close()

asyncio.run(main())

# END OF FILE
