#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Example for process methods
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
import re

# Opens the database.
dbm = tkrzw.DBM()
dbm.Open("casket.tkh", True, truncate=True, num_buckets=1000)

# Sets records with lambda functions.
dbm.Process("doc-1", lambda key, value: "Tokyo is the capital city of Japan.", True)
dbm.Process("doc-2", lambda key, value: "Is she living in Tokyo, Japan?", True)
dbm.Process("doc-3", lambda key, value: "She must leave Tokyo!", True)

# Lowers record values.
def Lower(key, value):
    # If no matching record, None is given as the value.
    if not value: return None
    # Sets the new value.
    # Note that the key and the value are a "bytes" object.
    return value.decode().lower()
dbm.Process("doc-1", Lower, True)
dbm.Process("doc-2", Lower, True)
dbm.Process("non-existent", Lower, True)

# Does the same thing with a lambda function.
dbm.Process("doc-3",
            lambda key, value: value.decode().lower() if value else None,
            True)

# If you don't update the record, set the third parameter to false.
dbm.Process("doc-3", lambda key, value: print(key, value), False)

# Adds multiple records at once.
dbm.ProcessMulti([
    ("doc-4", lambda key, value: "Tokyo Go!"),
    ("doc-5", lambda key, value: "Japan Go!")], True)
dbm.ProcessMulti([("doc-4", Lower), ("doc-5", Lower)], True)

# Checks the whole content.
# This uses an external iterator and is relavively slow.
for key, value in dbm:
    print(key.decode(), value.decode())
    
# Function for word counting.
word_counts = {}
def WordCounter(key, value):
    if not key: return
    value = value.decode()
    words = [x for x in re.split(r"\W+", value) if x]
    for word in words:
        word_counts[word] = (word_counts.get(word) or 0) + 1

# The second parameter should be false if the value is not updated.
dbm.ProcessEach(WordCounter, False)
print(word_counts)

# Returning False by the callbacks removes the record.
dbm.Process("doc-1", lambda key, value: False, True)
print(dbm.Count())
dbm.ProcessMulti([("doc-2", lambda key, value: False),
                  ("doc-3", lambda key, value: False)], True)
print(dbm.Count())
dbm.ProcessEach(lambda key, value: False, True)
print(dbm.Count())

# Closes the database.
dbm.Close()

# END OF FILE
