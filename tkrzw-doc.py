#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# API document
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

"""
Python Binding of Tkrzw
=======================

Introduction
------------

DBM (Database Manager) is a concept to store an associative array on a permanent storage.  In other words, DBM allows an application program to store key-value pairs in a file and reuse them later.  Each of keys and values is a string or a sequence of bytes.  A key must be unique within the database and a value is associated to it.  You can retrieve a stored record with its key very quickly.  Thanks to simple structure of DBM, its performance can be extremely high.

Tkrzw is a library implementing DBM with various algorithms.  It features high degrees of performance, concurrency, scalability and durability.  The following data structures are provided.

  - HashDBM : File datatabase manager implementation based on hash table.
  - TreeDBM : File datatabase manager implementation based on B+ tree.
  - SkipDBM : File datatabase manager implementation based on skip list.
  - TinyDBM : On-memory datatabase manager implementation based on hash table.
  - BabyDBM : On-memory datatabase manager implementation based on B+ tree.
  - CacheDBM : On-memory datatabase manager implementation with LRU deletion.
  - StdHashDBM : On-memory DBM implementations using std::unordered_map.
  - StdTreeDBM : On-memory DBM implementations using std::map.

Whereas Tkrzw is C++ library, this package provides its Python interface.  All above data structures are available via one adapter class ":class:`DBM`".  Read the `homepage <http://dbmx.net/tkrzw/>`_ for details.

DBM stores key-value pairs of strings.  Each string is represented as bytes in Python.  You can specify any type of objects as keys and values if they can be converted into strings, which are "encoded" into bytes.  When you retreive the value of a record, the type is determined according to the method: Get for bytes, GetStr for string, or [] for the same type as the key.

Symbols of the module "tkrzw" should be imported in each source file of application programs.::

 import tkrzw

An instance of the class ":class:`DBM`" is used in order to handle a database.  You can store, delete, and retrieve records with the instance.  The result status of each operation is represented by an object of the class ":class:`Status`".  Iterator to access access each record is implemented by the class ":class:`Iterator`".

Installation
------------

Install the latest version of Tkrzw beforehand and get the package of the Python binding of Tkrzw.  Python 3.6 or later is required to use this package.

Enter the directory of the extracted package then perform installation.  If your system has the another command except for the "python3" command, edit the Makefile beforehand.::

 make
 make check
 sudo make install

Example
-------

The following code is a typical example to use a database.  A DBM object can be used like a dictionary object.  As DBM implements the generic iterator protocol, you can access each record with the "for" loop.::

 import tkrzw
 
 # Prepares the database.
 dbm = tkrzw.DBM()
 dbm.Open("casket.tkh", True, truncate=True, num_buckets=100)
 
 # Sets records.
 # If the operation fails, a runtime exception is raised.
 # Keys and values are implicitly converted into bytes.
 dbm["first"] = "hop"
 dbm["second"] = "step"
 dbm["third"] = "jump"
 
 # Retrieves record values.
 # If the operation fails, a runtime exception is raised.
 # Retrieved values are strings if keys are strings.
 print(dbm["first"])
 print(dbm["second"])
 print(dbm["third"])
 try:
     print(dbm["fourth"])
 except tkrzw.StatusException as e:
     print(repr(e))
 
 # Traverses records.
 # Retrieved keys and values are always bytes so we decode them.
 for key, value in dbm:
     print(key.decode(), value.decode())
 
 # Closes the database.
 dbm.Close()
  
The following code is a more complex example.  Resources of DBM and Iterator are bound to their objects so when the refenrece count becomes zero, resources are released.  Even if the database is not closed, the destructor closes it implicitly.  The method "OrDie" throws an exception on failure so it is useful for checking errors.::

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
 it = dbm.MakeIterator()
 it.First()
 while True:
     status = tkrzw.Status()
     record = it.GetStr(status)
     if not status.IsOK():
         break
     print(record[0], record[1])
     it.Next()
 
 # Closes the database.
 dbm.Close()
"""


class Utility:
  """Library utilities."""
  
  VERSION = "0.0.0"
  """The package version numbers."""
  INT32MIN = -2 ** 31
  """The minimum value of int32."""
  INT32MAX = 2 ** 31 - 1
  """The minimum value of int32."""
  INT64MIN = -2 ** 63
  """The minimum value of int64."""
  INT64MAX = 2 ** 63 - 1
  """The minimum value of int64."""

  @classmethod
  def GetMemoryUsage(cls):
    """
    Gets the current memory usage of the process.

    :return: The current memory usage of the process.
    """
    pass  # native code

  @classmethod
  def PrimaryHash(cls, data, num_buckets=None):
    """
    Primary hash function for the hash database.

    :param data: The data to calculate the hash value for.
    :param num_buckets: The number of buckets of the hash table.  If it is omitted, 1<<64 is set.
    :return: The hash value.
    """
    pass  # native code

  @classmethod
  def SecondaryHash(cls, data, num_shards=None):
    """
    Secondary hash function for sharding.

    :param data: The data to calculate the hash value for.
    :param num_shards: The number of shards.  If it is omitted, 1<<64 is set.
    :return: The hash value.
    """
    pass  # native code

  @classmethod
  def EditDistanceLev(cls, a, b):
    """
    Gets the Levenshtein edit distance of two Unicode strings.

    :param a: A Unicode string.
    :param b: The other Unicode string.
    :return: The Levenshtein edit distance of the two strings.
    """
    pass  # native code


class Status:
  """
  Status of operations.
  """
  
  SUCCESS = 0
  """Success."""
  UNKNOWN_ERROR = 1
  """Generic error whose cause is unknown."""
  SYSTEM_ERROR = 2
  """Generic error from underlying systems."""
  NOT_IMPLEMENTED_ERROR = 3
  """Error that the feature is not implemented."""
  PRECONDITION_ERROR = 4
  """Error that a precondition is not met."""
  INVALID_ARGUMENT_ERROR = 5
  """Error that a given argument is invalid."""
  CANCELED_ERROR = 6
  """Error that the operation is canceled."""
  NOT_FOUND_ERROR = 7
  """Error that a specific resource is not found."""
  PERMISSION_ERROR = 8
  """Error that the operation is not permitted."""
  INFEASIBLE_ERROR = 9
  """Error that the operation is infeasible."""
  DUPLICATION_ERROR = 10
  """Error that a specific resource is duplicated."""
  BROKEN_DATA_ERROR = 11
  """Error that internal data are broken."""
  APPLICATION_ERROR = 12
  """Generic error caused by the application logic."""

  def __init__(self, code=SUCCESS, message=""):
    """
    Sets the code and the message.

    :param code: The status code.  This can be omitted and then SUCCESS is set.
    :param message: An arbitrary status message.  This can be omitted and the an empty string is set.
    """
    pass  # native code

  def __repr__(self):
    """
    Returns A string representation of the object.

    :return: The string representation of the object.
    """
    pass  # native code

  def __str__(self):
    """
    Returns A string representation of the content.

    :return: The string representation of the content.
    """
    pass  # native code

  def Set(self, code=SUCCESS, message=""):
    """
    Sets the code and the message.

    :param code: The status code.  This can be omitted and then SUCCESS is set.
    :param message: An arbitrary status message.  This can be omitted and the an empty string is set.
    """
    pass  # native code

  def GetCode(self):
    """
    Gets the status code.

    :return: The status code.
    """
    pass  # native code

  def GetMessage(self):
    """
    Gets the status message.

    :return: The status message.
    """
    pass  # native code

  def IsOK(self):
    """
    Returns true if the status is success.

    :return: True if the status is success, or False on failure.
    """
    pass  # native code

  def OrDie(self):
    """
    Raises an exception if the status is not success.

    :raise StatusException: An exception containing the status object.
    """
    pass  # native code


class StatusException(RuntimeError):
  """
  Exception to convey the status of operations.
  """
  
  def __init__(self, status):
    """
    Sets the status.

    :param status: The status object.
    """
    pass  # native code

  def __repr__(self):
    """
    Returns A string representation of the object.

    :return: The string representation of the object.
    """
    pass  # native code

  def __str__(self):
    """
    Returns A string representation of the content.

    :return: The string representation of the content.
    """
    pass  # native code

  def GetStatus(self):
    """
    Gets the status object

    :return: The status object.
    """
    pass  # native code


class DBM:
  """
  Polymorphic database manager.

  All operations except for Open and Close are thread-safe; Multiple threads can access the same database concurrently.  You can specify a data structure when you call the Open method.  Every opened database must be closed explicitly by the Close method to avoid data corruption.
  """

  def __init__(self):
    """
    Does nothing especially.
    """
    pass  # native code

  def __repr__(self):
    """
    Returns A string representation of the object.

    :return: The string representation of the object.
    """
    pass  # native code

  def __str__(self):
    """
    Returns A string representation of the content.

    :return: The string representation of the content.
    """
    pass  # native code

  def __len__(self):
    """
    Gets the number of records, to enable the len operator.

    :return: The number of records on success, or -1 on failure.
    """
    pass  # native code

  def __getitem__(self, key):
    """
    Gets the value of a record, to enable the [] operator.

    :param key: The key of the record.
    :return: The value of the matching record or None on failure.
    :raise StatusException: An exception containing the status object.
    """
    pass  # native code

  def __setitem__(self, key, value):
    """
    Sets a record of a key and a value, to enable the []= operator.

    :param key: The key of the record.
    :param value: The value of the record.
    :return: The value of the matching record or None on failure.
    :raise StatusException: An exception containing the status object.
    """
    pass  # native code

  def __iter__(self):
    """
    Makes an iterator and initialize it, to comply to the iterator protocol.

    :return: The iterator for each record.
    """
    pass  # native code
  
  def Open(self, path, writable, **params):
    """
    Opens a database file.

    :param path: A path of the file.
    :param writable: If true, the file is writable.  If false, it is read-only.
    :param params: Optional parameters.
    :return: The result status.

    The extension of the path indicates the type of the database.
      - .thh : File hash database (HashDBM)
      - .tkt : File tree database (TreeDBM)
      - .tks : File skip database (SkipDBM)
      - .tkmt : On-memory hash database (TinyDBM)
      - .tkmb : On-memory tree database (BabyDBM)
      - .tkmc : On-memory cache database (CacheDBM)
      - .tksh : On-memory STL hash database (StdHashDBM)
      - .tkst : On-memory STL tree database (StdTreeDBM)
    
    The optional parameters can include an option for the concurrency tuning.  By default, database operatins are done under the GIL (Global Interpreter Lock), which means that database operations are not done concurrently even if you use multiple threads.  If the "concurrent" parameter is true, database operations are done outside the GIL, which means that database operations can be done concurrently if you use multiple threads.  However, the downside is that swapping thread data is costly so the actual throughput is often worse in the concurrent mode than in the normal mode.  Therefore, the concurrent mode should be used only if the database is huge and it can cause blocking of threads in multi-thread usage.
    The optional parameters can include options for the file opening operation.
      - truncate (bool): True to truncate the file.
      - no_create (bool): True to omit file creation.
      - no_wait (bool): True to fail if the file is locked by another process.
      - no_lock (bool): True to omit file locking.

    The optional parameter "dbm" supercedes the decision of the database type by the extension.  The value is the type name: "HashDBM", "TreeDBM", "SkipDBM", "TinyDBM", "BabyDBM", "CacheDBM", "StdHashDBM", "StdTreeDBM".

    For HashDBM, these optional parameters are supported.
      - update_mode (string): How to update the database file: "UPDATE_IN_PLACE" for the in-palce and "UPDATE_APPENDING" for the appending mode.
      - offset_width (int): The width to represent the offset of records.
      - align_pow (int): The power to align records.
      - num_buckets (int): The number of buckets for hashing.
      - fbp_capacity (int): The capacity of the free block pool.
      - lock_mem_buckets (bool): True to lock the memory for the hash buckets.

    For TreeDBM, all optional parameters for HashDBM are available.  In addition, these optional parameters are supported.
      - max_page_size (int): The maximum size of a page.
      - max_branches (int): The maximum number of branches each inner node can have.
      - max_cached_pages (int): The maximum number of cached pages.
      - key_comparator (string): The comparator of record keys: "LexicalKeyComparator" for the lexical order, "LexicalCaseKeyComparator" for the lexical order ignoring case, "DecimalKeyComparator" for the order of the decimal integer numeric expressions, "HexadecimalKeyComparato" for the order of the hexadecimal integer numeric expressions, "RealNumberKeyComparator" for the order of the decimal real number expressions.

    For SkipDBM, these optional parameters are supported.
      - offset_width (int): The width to represent the offset of records.
      - step_unit (int): The step unit of the skip list.
      - max_level (int): The maximum level of the skip list.
      - sort_mem_size (int): The memory size used for sorting to build the database in the at-random mode.
      - insert_in_order (bool): If true, records are assumed to be inserted in ascending order of the key.
      - max_cached_records (int): The maximum number of cached records.

    For TinyDBM, these optional parameters are supported.
      - num_buckets (int): The number of buckets for hashing.

    For BabyDBM, these optional parameters are supported.
      - key_comparator (string): The comparator of record keys. The same ones as TreeDBM.

    For CacheDBM, these optional parameters are supported.
      - cap_rec_num (int): The maximum number of records.
      - cap_mem_size (int): The total memory size to use.

    If the optional parameter "num_shards" is set, the database is sharded into multiple shard files.  Each file has a suffix like "-00003-of-00015".  If the value is 0, the number of shards is set by patterns of the existing files, or 1 if they doesn't exist.
    """
    pass  # native code

  def Close(self):
    """
    Closes the database file.

    :return: The result status.
    """
    pass  # native code

  def Get(self, key, status=None):
    """
    Gets the value of a record of a key.

    :param key: The key of the record.
    :param status: A status object to which the result status is assigned.  It can be omitted.
    :return: The bytes value of the matching record or None on failure.
    """
    pass  # native code

  def GetStr(self, key, status=None):
    """
    Gets the value of a record of a key, as a string.

    :param key: The key of the record.
    :param status: A status object to which the result status is assigned.  It can be omitted.
    :return: The string value of the matching record or None on failure.
    """
    pass  # native code

  def GetMulti(self, *keys):
    """
    Gets the values of multiple records of keys.
    :param keys: The keys of records to retrieve.
    :return: A map of retrieved records.  Keys which don't match existing records are ignored.
    """
    pass  # native code

  def GetMultiStr(self, *keys):
    """
    Gets the values of multiple records of keys, as strings.
    :param keys: The keys of records to retrieve.
    :return: A map of retrieved records.  Keys which don't match existing records are ignored.
    """
    pass  # native code

  def Set(self, key, value, overwrite=False):
    """
    Sets a record of a key and a value.

    :param key: The key of the record.
    :param value: The value of the record.
    :param overwrite: Whether to overwrite the existing value.  It can be omitted and then false is set.
    :return: The result status.
    """
    pass  # native code

  def SetMulti(self, **records):
    """
    Sets multiple records of the keyword arguments.
    :param records: Records to store.  Existing records with the same keys are overwritten.
    :return: The result status.
    """
    pass  # native code
  
  def Remove(self, key):
    """
    Removes a record of a key.

    :param key: The key of the record.
    :return: The result status.
    """
    pass  # native code

  def Append(self, key, value, delim=""):
    """
    Appends data at the end of a record of a key.

    :param key: The key of the record.
    :param value: The value to append.
    :param delim: The delimiter to put after the existing record.
    :return: The result status.

    If there's no existing record, the value is set without the delimiter.
    """
    pass  # native code

  def CompareExchange(self, key, expected, desired):
    """
    Compares the value of a record and exchanges if the condition meets.

    :param key: The key of the record.
    :param expected: The expected value.
    :param desired: The desired value.  If it is None, the record is to be removed.
    :return: The result status.

    If the record doesn't exist, NOT_FOUND_ERROR is returned.  If the existing value is different from the expected value, DUPLICATION_ERROR is returned.  Otherwise, the desired value is set.
    """
    pass  # native code

  def Increment(self, key, inc=1, init=0, status=None):
    """
    Increments the numeric value of a record.

    :param key: The key of the record.
    :param inc: The incremental value.  If it is Utility.INT64MIN, the current value is not changed and a new record is not created.
    :param init: The initial value.
    :param status: A status object to which the result status is assigned.  It can be omitted.
    :return: The current value, or None on failure.

    The record value is stored as an 8-byte big-endian integer.  Negative is also supported.
    """
    pass  # native code
  
  def Count(self):
    """
    Gets the number of records.

    :return: The number of records on success, or None on failure.
    """
    pass  # native code
  
  def GetFileSize(self):
    """
    Gets the current file size of the database.

    :return: The current file size of the database, or None on failure.
    """
    pass  # native code

  def GetFilePath(self):
    """
    Gets the path of the database file.

    :return: The file path of the database, or None on failure.
    """
    pass  # native code

  def Clear(self):
    """
    Removes all records.

    :return: The result status.
    """
    pass  # native code
  
  def Rebuild(self, **params):
    """
    Rebuilds the entire database.

    :param params: Optional parameters.
    :return: The result status.

    The optional parameters are the same as the Open method.  Omitted tuning parameters are kept the same or implicitly optimized.
    """
    pass  # native code

  def ShouldBeRebuilt(self):
    """
    Checks whether the database should be rebuilt.

    :return: True to be optimized or false with no necessity.
    """
    pass  # native code

  def Synchronize(self, hard, **params):
    """
    Synchronizes the content of the database to the file system.

    :param hard: True to do physical synchronization with the hardware or false to do only logical synchronization with the file system.
    :param params: Optional parameters.

    Only SkipDBM uses the optional parameters.  The "merge" parameter specifies paths of databases to merge, separated by colon.  The "reducer" parameter specifies the reducer to apply to records of the same key.  "ReduceToFirst", "ReduceToSecond", "ReduceToLast", etc are supported.
    """
    pass  # native code

  def CopyFile(self, dest_path):
    """
    Copies the content of the database file to another file.

    :param dest_path: A path to the destination file.
    :return: The result status.
    """
    pass  # native code
  
  def Export(self, dest_dbm):
    """
    Exports all records to another database.

    :param dest_dbm: The destination database.
    :return: The result status.
    """
    pass  # native code

  def ExportKeysAsLines(self, dest_path):
    """
    Exports the keys of all records as lines to a text file.

    :param dest_path: A path of the output text file.
    :return: The result status.
    """
    pass  # native code

  def Inspect(self):
    """
    Inspects the database.

    :return: A map of property names and their values.
    """
    pass  # native code
  
  def IsOpen(self):
    """
    Checks whether the database is open.

    :return: True if the database is open, or false if not.
    """
    pass  # native code

  def IsHealthy(self):
    """
    Checks whether the database condition is healthy.

    :return: True if the database condition is healthy, or false if not.
    """
    pass  # native code

  def IsOrdered(self):
    """
    Checks whether ordered operations are supported.

    :return: True if ordered operations are supported, or false if not.
    """
    pass  # native code

  def Search(self, mode, pattern, capacity=0, utf=False):
    """
    Searches the database and get keys which match a pattern.

    :param mode: The search mode.  "contain" extracts keys containing the pattern.  "begin" extracts keys beginning with the pattern.  "end" extracts keys ending with the pattern.  "regex" extracts keys partially matches the pattern of a regular expression.  "edit" extracts keys whose edit distance to the pattern is the least.
    :param pattern: The pattern for matching.
    :param capacity: The maximum records to obtain.  0 means unlimited.
    :param utf: If true, text is treated as UTF-8, which affects "regex" and "edit".
    :return: A list of keys matching the condition.
    """
    pass  # native code
  
  def MakeIterator(self):
    """
    Makes an iterator for each record.

    :return: The iterator for each record.
    """
    pass  # native code


class Iterator:
  """
  Iterator for each record.
  """

  def __init__(self, dbm):
    """
    Initializes the iterator.

    :param dbm: The database to scan.
    """
    pass  # native code

  def __repr__(self):
    """
    Returns A string representation of the object.

    :return: The string representation of the object.
    """
    pass  # native code

  def __str__(self):
    """
    Returns A string representation of the content.

    :return: The string representation of the content.
    """
    pass  # native code

  def __next__(self):
    """
    Moves the iterator to the next record, to comply to the iterator protocol.

    :return: A tuple of The key and the value of the current record.
    """
    pass  # native code

  def First(self):
    """
    Initializes the iterator to indicate the first record.

    :return: The result status.

    Even if there's no record, the operation doesn't fail.
    """
    pass  # native code
  
  def Last(self):
    """
    Initializes the iterator to indicate the last record.

    :return: The result status.

    Even if there's no record, the operation doesn't fail.  This method is suppoerted only by ordered databases.
    """
    pass  # native code
  
  def Jump(self, key):
    """
    Initializes the iterator to indicate a specific record.

    :param key: The key of the record to look for.
    :return: The result status.

    Ordered databases can support "lower bound" jump; If there's no record with the same key, the iterator refers to the first record whose key is greater than the given key.  The operation fails with unordered databases if there's no record with the same key.
    """
    pass  # native code

  def JumpLower(self, key, inclusive=False):
    """
    Initializes the iterator to indicate the last record whose key is lower than a given key.
    :param key: The key to compare with.
    :param inclusive: If true, the considtion is inclusive: equal to or lower than the key.
    :return: The result status.

    Even if there's no matching record, the operation doesn't fail.  This method is suppoerted only by ordered databases.
    """
    pass  # native code

  def JumpUpper(self, key, inclusive=False):
    """
    Initializes the iterator to indicate the first record whose key is upper than a given key.
    :param key: The key to compare with.
    :param inclusive: If true, the considtion is inclusive: equal to or upper than the key.
    :return: The result status.

    Even if there's no matching record, the operation doesn't fail.  This method is suppoerted only by ordered databases.
    """
    pass  # native code
  
  def Next(self):
    """
    Moves the iterator to the next record.

    :return: The result status.

    If the current record is missing, the operation fails.  Even if there's no next record, the operation doesn't fail.
    """
    pass  # native code

  def Previous(self):
    """
    Moves the iterator to the previous record.

    :return: The result status.

    If the current record is missing, the operation fails.  Even if there's no previous record, the operation doesn't fail.  This method is suppoerted only by ordered databases.
    """
    pass  # native code

  def Get(self, status=None):
    """
    Gets the key and the value of the current record of the iterator.

    :param status: A status object to which the result status is assigned.  It can be omitted.
    :return: A tuple of the bytes key and the bytes value of the current record.  On failure, None is returned.
    """
    pass  # native code

  def GetStr(self, status=None):
    """
    Gets the key and the value of the current record of the iterator, as strings.

    :param status: A status object to which the result status is assigned.  It can be omitted.
    :return: A tuple of the string key and the string value of the current record.  On failure, None is returned.
    """
    pass  # native code

  def GetKey(self, status=None):
    """
    Gets the key of the current record.

    :param status: A status object to which the result status is assigned.  It can be omitted.
    :return: The bytes key of the current record or None on failure.
    """
    pass  # native code

  def GetKeyStr(self, status=None):
    """
    Gets the key of the current record, as a string.

    :param status: A status object to which the result status is assigned.  It can be omitted.
    :return: The string key of the current record or None on failure.
    """
    pass  # native code

  def GetValue(self, status=None):
    """
    Gets the value of the current record.

    :param status: A status object to which the result status is assigned.  It can be omitted.
    :return: The bytes value of the current record or None on failure.
    """
    pass  # native code

  def GetValueStr(self, status=None):
    """
    Gets the value of the current record, as a string.

    :param status: A status object to which the result status is assigned.  It can be omitted.
    :return: The string value of the current record or None on failure.
    """
    pass  # native code

  def Set(self, value):
    """
    Sets the value of the current record.

    :param value: The value of the record.
    :return: The result status.
    """
    pass  # native code

  def Remove(self):
    """
    Removes the current record.

    :return: The result status.
    """
    pass  # native code


class TextFile:
  """
  Text file of line data.

  DBM#ExportKeysAsLines outputs keys of the database into a text file.  Scanning the text file is more efficient than scanning the whole database.
  """

  def __init__(self):
    """
    Initializes the text file object.
    """
    pass  # native code

  def __repr__(self):
    """
    Returns A string representation of the object.

    :return: The string representation of the object.
    """
    pass  # native code

  def __str__(self):
    """
    Returns A string representation of the content.

    :return: The string representation of the content.
    """
    pass  # native code

  def Open(self, path):
    """
    Opens a text file.

    :param path: A path of the file.
    :return: The result status.
    """
    pass  # native code

  def Close(self):
    """
    Closes the text file.

    :return: The result status.
    """
    pass  # native code

  def Search(self, mode, pattern, capacity=0, utf=False):
    """
    Searches the text file and get lines which match a pattern.

    :param mode: The search mode.  "contain" extracts lines containing the pattern.  "begin" extracts lines beginning with the pattern.  "end" extracts lines ending with the pattern.  "regex" extracts lines partially matches the pattern of a regular expression.  "edit" extracts lines whose edit distance to the pattern is the least.
    :param pattern: The pattern for matching.
    :param capacity: The maximum records to obtain.  0 means unlimited.
    :param utf: If true, text is treated as UTF-8, which affects "regex" and "edit".
    :return: A list of lines matching the condition.
    """
    pass  # native code


# END OF FILE
