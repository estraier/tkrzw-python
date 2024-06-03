#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Example for key comparators of the tree database
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

# Opens a new database with the default key comparator (LexicalKeyComparator).
dbm = tkrzw.DBM()
open_params = {
    "truncate": True,
}
status = dbm.Open("casket.tkt", True, **open_params).OrDie()

# Sets records with the key being a big-endian binary of an integer.
# e.g: "\x00\x00\x00\x00\x00\x00\x00\x31" -> "hop"
dbm.Set(tkrzw.Utility.SerializeInt(1), "hop").OrDie()
dbm.Set(tkrzw.Utility.SerializeInt(256), "step").OrDie()
dbm.Set(tkrzw.Utility.SerializeInt(32), "jump").OrDie()

# Gets records with the key being a big-endian binary of an integer.
print(dbm.GetStr(tkrzw.Utility.SerializeInt(1)))
print(dbm.GetStr(tkrzw.Utility.SerializeInt(256)))
print(dbm.GetStr(tkrzw.Utility.SerializeInt(32)))

# Lists up all records, restoring keys into integers.
iter = dbm.MakeIterator()
iter.First()
while True:
    record = iter.Get()
    if not record: break
    print(str(tkrzw.Utility.DeserializeInt(record[0])) + ": " + record[1].decode())
    iter.Next()

# Closes the database.
dbm.Close().OrDie()

# Opens a new database with the decimal integer comparator.
open_params = {
    "truncate": True,
    "key_comparator": "Decimal",
}
status = dbm.Open("casket.tkt", True, **open_params).OrDie()

# Sets records with the key being a decimal string of an integer.
# e.g: "1" -> "hop"
dbm.Set("1", "hop").OrDie()
dbm.Set("256", "step").OrDie()
dbm.Set("32", "jump").OrDie()

# Gets records with the key being a decimal string of an integer.
print(dbm.GetStr("1"))
print(dbm.GetStr("256"))
print(dbm.GetStr("32"))

# Lists up all records, restoring keys into integers.
iter = dbm.MakeIterator()
iter.First()
while True:
    record = iter.GetStr()
    if not record: break
    print("{:d}: {}".format(int(record[0]), record[1]))
    iter.Next()

# Closes the database.
dbm.Close().OrDie()

# Opens a new database with the decimal real number comparator.
open_params = {
    "truncate": True,
    "key_comparator": "RealNumber",
}
status = dbm.Open("casket.tkt", True, **open_params).OrDie()

# Sets records with the key being a decimal string of a real number.
# e.g: "1.5" -> "hop"
dbm.Set("1.5", "hop").OrDie()
dbm.Set("256.5", "step").OrDie()
dbm.Set("32.5", "jump").OrDie()

# Gets records with the key being a decimal string of a real number.
print(dbm.GetStr("1.5"))
print(dbm.GetStr("256.5"))
print(dbm.GetStr("32.5"))

# Lists up all records, restoring keys into floating-point numbers.
iter = dbm.MakeIterator()
iter.First()
while True:
    record = iter.GetStr()
    if not record: break
    print("{:.3f}: {}".format(float(record[0]), record[1]))
    iter.Next()

# Closes the database.
dbm.Close().OrDie()

# Opens a new database with the big-endian signed integers comparator.
open_params = {
    "truncate": True,
    "key_comparator": "SignedBigEndian",
}
status = dbm.Open("casket.tkt", True, **open_params).OrDie()

# Sets records with the key being a big-endian binary of a signed integer.
# e.g: "\x00\x00\x00\x00\x00\x00\x00\x31" -> "hop"
dbm.Set(tkrzw.Utility.SerializeInt(-1), "hop").OrDie()
dbm.Set(tkrzw.Utility.SerializeInt(-256), "step").OrDie()
dbm.Set(tkrzw.Utility.SerializeInt(-32), "jump").OrDie()

# Gets records with the key being a big-endian binary of a signed integer.
print(dbm.GetStr(tkrzw.Utility.SerializeInt(-1)))
print(dbm.GetStr(tkrzw.Utility.SerializeInt(-256)))
print(dbm.GetStr(tkrzw.Utility.SerializeInt(-32)))

# Lists up all records, restoring keys into signed integers.
iter = dbm.MakeIterator()
iter.First()
while True:
    record = iter.Get()
    if not record: break
    print("{:d}: {}".format(tkrzw.Utility.DeserializeInt(record[0]), record[1].decode()))
    iter.Next()

# Closes the database.
dbm.Close().OrDie()

# Opens a new database with the big-endian floating-point numbers comparator.
open_params = {
    "truncate": True,
    "key_comparator": "FloatBigEndian",
}
status = dbm.Open("casket.tkt", True, **open_params).OrDie()

# Sets records with the key being a big-endian binary of a floating-point number.
# e.g: "\x3F\xF8\x00\x00\x00\x00\x00\x00" -> "hop"
dbm.Set(tkrzw.Utility.SerializeFloat(1.5), "hop").OrDie()
dbm.Set(tkrzw.Utility.SerializeFloat(256.5), "step").OrDie()
dbm.Set(tkrzw.Utility.SerializeFloat(32.5), "jump").OrDie()

# Gets records with the key being a big-endian binary of a floating-point number.
print(dbm.GetStr(tkrzw.Utility.SerializeFloat(1.5)))
print(dbm.GetStr(tkrzw.Utility.SerializeFloat(256.5)))
print(dbm.GetStr(tkrzw.Utility.SerializeFloat(32.5)))

# Lists up all records, restoring keys into floating-point numbers.
iter = dbm.MakeIterator()
iter.First()
while True:
    record = iter.Get()
    if not record: break
    print("{:.3f}: {}".format(tkrzw.Utility.DeserializeFloat(record[0]), record[1].decode()))
    iter.Next()

# Closes the database.
dbm.Close().OrDie()

# END OF FILE
