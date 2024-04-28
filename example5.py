#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Example for secondary index
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

# Opens the index.
index = tkrzw.Index()
index.Open("casket.tkt", True, truncate=True, num_buckets=100).OrDie()

# Adds records to the index.
# The key is a division name and the value is person name.
index.Add("general", "anne").OrDie()
index.Add("general", "matthew").OrDie()
index.Add("general", "marilla").OrDie()
index.Add("sales", "gilbert").OrDie()

# Anne moves to the sales division.
index.Remove("general", "anne").OrDie()
index.Add("sales", "anne").OrDie()

# Prints all members for each division.
for division in ["general", "sales"]:
  print(division)
  members = index.GetValuesStr(division)
  for member in members:
    print(" -- " + member)

# Prints every record by iterator.
iter = index.MakeIterator()
iter.First()
while True:
  record = iter.GetStr()
  if not record: break
  print(record[0] + ": " + record[1])
  iter.Next()

# Prints every record by the iterable protocol.
for key, value in index:
  print(key.decode() + ": " + value.decode())

# Closes the index
index.Close().OrDie()

# END OF FILE
