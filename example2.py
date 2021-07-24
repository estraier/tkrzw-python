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

import tkrzw

# Prepares the database.
# Options are given by dictionary expansion.
# All methods except for [] and []= don't raise exceptions.
dbm = tkrzw.DBM()
open_params = {
    "max_page_size": 4080,
    "max_branches": 256,
    "key_comparator": "decimal",
    "concurrent": True,
    "truncate": True,
}
status = dbm.Open("casket.tkt", True, **open_params)
if not status.IsOK():
    raise tkrzw.StatusException(status)

# Sets records.
# The method OrDie raises a runtime error on failure.
dbm.Set(1, "hop").OrDie()
dbm.Set(2, "step").OrDie()
dbm.Set(3, "jump").OrDie()

# Retrieves records without checking errors.
# On failure, the return value is None.
print(dbm.GetStr(1))
print(dbm.GetStr(2))
print(dbm.GetStr(3))
print(dbm.GetStr(4))

# To know the status of retrieval, give a status object to Get.
# You can compare a status object and a status code directly.
status = tkrzw.Status()
value = dbm.GetStr(1, status)
print("status: " + str(status))
if status == tkrzw.Status.SUCCESS:
    print("value: " + value)

# Rebuilds the database.
# Almost the same options as the Open method can be given.
dbm.Rebuild(align_pow=0, max_page_size=1024).OrDie()

# Traverses records with an iterator.
iter = dbm.MakeIterator()
iter.First()
while True:
    status = tkrzw.Status()
    record = iter.GetStr(status)
    if not status.IsOK():
        break
    print(record[0], record[1])
    iter.Next()

# Closes the database.
dbm.Close()

# END OF FILE
