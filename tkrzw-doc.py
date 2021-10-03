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


class Utility:
  """
  Library utilities.
  """

  VERSION = "0.0.0"
  """The package version numbers."""
  OS_NAME = "unknown"
  """The recognized OS name."""
  PAGE_SIZE = 4096
  """The size of a memory page on the OS."""
  INT32MIN = -2 ** 31
  """The minimum value of int32."""
  INT32MAX = 2 ** 31 - 1
  """The maximum value of int32."""
  UINT32MAX = 2 ** 32 - 1
  """The maximum value of uint32."""
  INT64MIN = -2 ** 63
  """The minimum value of int64."""
  INT64MAX = 2 ** 63 - 1
  """The maximum value of int64."""
  UINT64MAX = 2 ** 64 - 1
  """The maximum value of uint64."""

  @classmethod
  def GetMemoryCapacity(cls):
    """
    Gets the memory capacity of the platform.

    :return: The memory capacity of the platform in bytes, or -1 on failure.
    """
    pass  # native code

  @classmethod
  def GetMemoryUsage(cls):
    """
    Gets the current memory usage of the process.

    :return: The current memory usage of the process in bytes, or -1 on failure.
    """
    pass  # native code

  @classmethod
  def PrimaryHash(cls, data, num_buckets=None):
    """
    Primary hash function for the hash database.

    :param data: The data to calculate the hash value for.
    :param num_buckets: The number of buckets of the hash table.  If it is omitted, UINT64MAX is set.
    :return: The hash value.
    """
    pass  # native code

  @classmethod
  def SecondaryHash(cls, data, num_shards=None):
    """
    Secondary hash function for sharding.

    :param data: The data to calculate the hash value for.
    :param num_shards: The number of shards.  If it is omitted, UINT64MAX is set.
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
  NETWORK_ERROR = 12
  """Error caused by networking failure."""
  APPLICATION_ERROR = 13
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
    Returns a string representation of the object.

    :return: The string representation of the object.
    """
    pass  # native code

  def __str__(self):
    """
    Returns a string representation of the content.

    :return: The string representation of the content.
    """
    pass  # native code

  def __eq__(self, rhs):
    """
    Returns true if the given object is equivalent to this object.
    
    :return: True if the given object is equivalent to this object.

    This supports comparison between a status object and a status code number.
    """
    pass  # native code

  def Set(self, code=SUCCESS, message=""):
    """
    Sets the code and the message.

    :param code: The status code.  This can be omitted and then SUCCESS is set.
    :param message: An arbitrary status message.  This can be omitted and the an empty string is set.
    """
    pass  # native code

  def Join(self, rht):
    """
    Assigns the internal state from another status object only if the current state is success.

    :param rhs: The status object.
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

  @classmethod
  def CodeName(cls, code):
    """
    Gets the string name of a status code.

    :param: code The status code.
    :return: The name of the status code.
    """


class Future:
  """
  Future containing a status object and extra data.

  Future objects are made by methods of AsyncDBM.  Every future object should be destroyed by the "Destruct" method or the "Get" method to free resources.  This class implements the awaitable protocol so an instance is usable with the "await" sentence.
  """

  def __init__(self):
    """
    The constructor cannot be called directly.  Use methods of AsyncDBM.
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
    Returns a string representation of the content.

    :return: The string representation of the content.
    """
    pass  # native code

  def __await__(self):
    """
    Waits for the operation to be done and returns an iterator.

    :return: The iterator which stops immediately.
    """
    pass  # native code

  def Wait(self, timeout=-1):
    """
    Waits for the operation to be done.

    :param timeout: The waiting time in seconds.  If it is negative, no timeout is set.
    :return: True if the operation has done.  False if timeout occurs.
    """
    pass  # native code

  def Get(self):
    """
    Waits for the operation to be done and gets the result status.

    :return: The result status and extra data if any.  The existence and the type of extra data depends on the operation which makes the future.  For DBM#Get, a tuple of the status and the retrieved value is returned.  For DBM#Set and DBM#Remove, the status object itself is returned.

    The internal resource is released by this method.  "Wait" and "Get" cannot be called after calling this method.
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
  This class implements the iterable protocol so an instance is usable with "for-in" loop.
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

    :return: The number of records on success, or 0 on failure.
    """
    pass  # native code

  def __getitem__(self, key):
    """
    Gets the value of a record, to enable the [] operator.

    :param key: The key of the record.
    :return: The value of the matching record.  An exception is raised on failure.
    :raise StatusException: An exception containing the status object.
    """
    pass  # native code

  def __setitem__(self, key, value):
    """
    Sets a record of a key and a value, to enable the []= operator.

    :param key: The key of the record.
    :param value: The value of the record.
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
      - .tkh : File hash database (HashDBM)
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
      - sync_hard (bool): True to do physical synchronization when closing.

    The optional parameter "dbm" supercedes the decision of the database type by the extension.  The value is the type name: "HashDBM", "TreeDBM", "SkipDBM", "TinyDBM", "BabyDBM", "CacheDBM", "StdHashDBM", "StdTreeDBM".

    The optional parameter "file" specifies the internal file implementation class.  The default file class is "MemoryMapAtomicFile".  The other supported classes are "StdFile", "MemoryMapAtomicFile", "PositionalParallelFile", and "PositionalAtomicFile".

    For HashDBM, these optional parameters are supported.
      - update_mode (string): How to update the database file: "UPDATE_IN_PLACE" for the in-palce or "UPDATE_APPENDING" for the appending mode.
      - record_crc_mode (string): How to add the CRC data to the record: "RECORD_CRC_NONE" to add no CRC to each record, "RECORD_CRC_8" to add CRC-8 to each record, "RECORD_CRC_16" to add CRC-16 to each record, or "RECORD_CRC_32" to add CRC-32 to each record.
      - record_comp_mode (string): How to compress the record data: "RECORD_COMP_NONE" to do no compression, "RECORD_COMP_ZLIB" to compress with ZLib, "RECORD_COMP_ZSTD" to compress with ZStd, "RECORD_COMP_LZ4" to compress with LZ4, "RECORD_COMP_LZMA" to compress with LZMA.
      - offset_width (int): The width to represent the offset of records.
      - align_pow (int): The power to align records.
      - num_buckets (int): The number of buckets for hashing.
      - restore_mode (string): How to restore the database file: "RESTORE_SYNC" to restore to the last synchronized state, "RESTORE_READ_ONLY" to make the database read-only, or "RESTORE_NOOP" to do nothing.  By default, as many records as possible are restored.
      - fbp_capacity (int): The capacity of the free block pool.
      - min_read_size (int): The minimum reading size to read a record.
      - lock_mem_buckets (bool): True to lock the memory for the hash buckets.
      - cache_buckets (bool): True to cache the hash buckets on memory.

    For TreeDBM, all optional parameters for HashDBM are available.  In addition, these optional parameters are supported.
      - max_page_size (int): The maximum size of a page.
      - max_branches (int): The maximum number of branches each inner node can have.
      - max_cached_pages (int): The maximum number of cached pages.
      - key_comparator (string): The comparator of record keys: "LexicalKeyComparator" for the lexical order, "LexicalCaseKeyComparator" for the lexical order ignoring case, "DecimalKeyComparator" for the order of the decimal integer numeric expressions, "HexadecimalKeyComparator" for the order of the hexadecimal integer numeric expressions, "RealNumberKeyComparator" for the order of the decimal real number expressions.

    For SkipDBM, these optional parameters are supported.
      - offset_width (int): The width to represent the offset of records.
      - step_unit (int): The step unit of the skip list.
      - max_level (int): The maximum level of the skip list.
      - restore_mode (string): How to restore the database file: "RESTORE_SYNC" to restore to the last synchronized state or "RESTORE_NOOP" to do nothing make the database read-only.  By default, as many records as possible are restored.
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

    All databases support taking update logs into files.  It is enabled by setting the prefix of update log files.
      - ulog_prefix (str): The prefix of the update log files.
      - ulog_max_file_size (num): The maximum file size of each update log file.  By default, it is 1GiB.
      - ulog_server_id (num): The server ID attached to each log.  By default, it is 0.
      - ulog_dbm_index (num): The DBM index attached to each log.  By default, it is 0.

    For the file "PositionalParallelFile" and "PositionalAtomicFile", these optional parameters are supported.
      - block_size (int): The block size to which all blocks should be aligned.
      - access_options (str): Values separated by colon.  "direct" for direct I/O.  "sync" for synchrnizing I/O, "padding" for file size alignment by padding, "pagecache" for the mini page cache in the process.

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

  def Set(self, key, value, overwrite=True):
    """
    Sets a record of a key and a value.

    :param key: The key of the record.
    :param value: The value of the record.
    :param overwrite: Whether to overwrite the existing value.  It can be omitted and then false is set.
    :return: The result status.  If overwriting is abandoned, DUPLICATION_ERROR is returned.
    """
    pass  # native code

  def SetMulti(self, overwrite=True, **records):
    """
    Sets multiple records of the keyword arguments.

    :param overwrite: Whether to overwrite the existing value if there's a record with the same key.  If true, the existing value is overwritten by the new value.  If false, the operation is given up and an error status is returned.
    :param records: Records to store.
    :return: The result status.  If there are records avoiding overwriting, DUPLICATION_ERROR is returned.
    """
    pass  # native code

  def SetAndGet(self, key, value, overwrite=True):
    """
    Sets a record and get the old value.

    :param key: The key of the record.
    :param value: The value of the record.
    :param overwrite: Whether to overwrite the existing value if there's a record with the same key.  If true, the existing value is overwritten by the new value.  If false, the operation is given up and an error status is returned.
    :return: A pair of the result status and the old value.  If the record has not existed when inserting the new record, None is assigned as the value.  If not None, the type of the returned old value is the same as the parameter value.
    """
    pass  # native code

  def Remove(self, key):
    """
    Removes a record of a key.

    :param key: The key of the record.
    :return: The result status.  If there's no matching record, NOT_FOUND_ERROR is returned.
    """
    pass  # native code

  def RemoveMulti(self, keys):
    """
    Removes records of keys.

    :param key: The keys of the records.
    :return: The result status.  If there are missing records, NOT_FOUND_ERROR is returned.
    """
    pass  # native code

  def RemoveAndGet(self, key):
    """
    Removes a record and get the value.

    :param key: The key of the record.
    :return: A pair of the result status and the record value.  If the record does not exist, None is assigned as the value.  If not None, the type of the returned value is the same as the parameter key.
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

  def AppendMulti(self, delim="", **records):
    """
    Appends data to multiple records of the keyword arguments.

    :param delim: The delimiter to put after the existing record.
    :param records: Records to append.  Existing records with the same keys are overwritten.
    :return: The result status.

    If there's no existing record, the value is set without the delimiter.
    """
    pass  # native code

  def CompareExchange(self, key, expected, desired):
    """
    Compares the value of a record and exchanges if the condition meets.

    :param key: The key of the record.
    :param expected: The expected value.  If it is None, no existing record is expected.
    :param desired: The desired value.  If it is None, the record is to be removed.
    :return: The result status.  If the condition doesn't meet, INFEASIBLE_ERROR is returned.
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

  def CompareExchangeMulti(self, expected, desired):
    """
    Compares the values of records and exchanges if the condition meets.

    :param expected: A sequence of pairs of the record keys and their expected values.  If the value is None, no existing record is expected.
    :param desired: A sequence of pairs of the record keys and their desired values.  If the value is None, the record is to be removed.
    :return: The result status.  If the condition doesn't meet, INFEASIBLE_ERROR is returned.
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

  def GetTimestamp(self):
    """
    Gets the timestamp in seconds of the last modified time.

    :return: The timestamp of the last modified time, or None on failure.
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

    In addition, HashDBM, TreeDBM, and SkipDBM supports the following parameters.
      - skip_broken_records (bool): If true, the operation continues even if there are broken records which can be skipped.
      - sync_hard (bool): If true, physical synchronization with the hardware is done before finishing the rebuilt file.
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
    :return: The result status.

    Only SkipDBM uses the optional parameters.  The "merge" parameter specifies paths of databases to merge, separated by colon.  The "reducer" parameter specifies the reducer to apply to records of the same key.  "ReduceToFirst", "ReduceToSecond", "ReduceToLast", etc are supported.
    """
    pass  # native code

  def CopyFileData(self, dest_path, sync_hard=False):
    """
    Copies the content of the database file to another file.

    :param dest_path: A path to the destination file.
    :param sync_hard: True to do physical synchronization with the hardware.
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

  def ExportToFlatRecords(self, dest_file):
    """
    Exports all records of a database to a flat record file.

    :param dest_file: The file object to write records in.
    :return: The result status.

    A flat record file contains a sequence of binary records without any high level structure so it is useful as a intermediate file for data migration.
    """
    pass  # native code
    
  def ImportFromFlatRecords(self, src_file):
    """
    Imports records to a database from a flat record file.

    :param src_file: The file object to read records from.
    :return: The result status.
    """
    pass  # native code

  def ExportKeysAsLines(self, dest_file):
    """
    Exports the keys of all records as lines to a text file.

    :param dest_file: The file object to write keys in.
    :return: The result status.

    As the exported text file is smaller than the database file, scanning the text file by the search method is often faster than scanning the whole database.
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

  def IsWritable(self):
    """
    Checks whether the database is writable.

    :return: True if the database is writable, or false if not.
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

  def Search(self, mode, pattern, capacity=0):
    """
    Searches the database and get keys which match a pattern.

    :param mode: The search mode.  "contain" extracts keys containing the pattern.  "begin" extracts keys beginning with the pattern.  "end" extracts keys ending with the pattern.  "regex" extracts keys partially matches the pattern of a regular expression.  "edit" extracts keys whose edit distance to the UTF-8 pattern is the least.  "editbin" extracts keys whose edit distance to the binary pattern is the least.
    :param pattern: The pattern for matching.
    :param capacity: The maximum records to obtain.  0 means unlimited.
    :return: A list of string keys matching the condition.
    """
    pass  # native code

  def MakeIterator(self):
    """
    Makes an iterator for each record.

    :return: The iterator for each record.
    """
    pass  # native code

  @classmethod
  def RestoreDatabase(cls, old_file_path, new_file_path, class_name="", end_offset=-1):
    """
    Restores a broken database as a new healthy database.

    :param old_file_path: The path of the broken database.
    :param new_file_path: The path of the new database to be created.
    :param class_name: The name of the database class.  If it is None or empty, the class is guessed from the file extension.
    :param end_offset: The exclusive end offset of records to read.  Negative means unlimited.  0 means the size when the database is synched or closed properly.  Using a positive value is not meaningful if the number of shards is more than one.
    :return: The result status.
    """


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


class AsyncDBM:
  """
  Asynchronous database manager adapter.

  This class is a wrapper of DBM for asynchronous operations.  A task queue with a thread pool is used inside.  Every method except for the constructor and the destructor is run by a thread in the thread pool and the result is set in the future oject of the return value.  The caller can ignore the future object if it is not necessary.  The Destruct method waits for all tasks to be done.  Therefore, the destructor should be called before the database is closed.
  """
  
  def __init__(self, dbm, num_worker_threads):
    """
    Sets up the task queue.

    :param dbm: A database object which has been opened.
    :param num_worker_threads: The number of threads in the internal thread pool.
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
    Returns a string representation of the content.

    :return: The string representation of the content.
    """
    pass  # native code

  def Destruct():
    """
    Destructs the asynchronous database adapter.

    This method waits for all tasks to be done.
    """

  def Get(self, key):
    """
    Gets the value of a record of a key.

    :param key: The key of the record.
    :return: The future for the result status and the bytes value of the matching record.
    """
    pass  # native code

  def GetStr(self, key):
    """
    Gets the value of a record of a key, as a string.

    :param key: The key of the record.
    :return: The future for the result status and the string value of the matching record.
    """
    pass  # native code

  def GetMulti(self, *keys):
    """
    Gets the values of multiple records of keys.

    :param keys: The keys of records to retrieve.
    :return: The future for the result status and a map of retrieved records.  Keys which don't match existing records are ignored.
    """
    pass  # native code

  def GetMultiStr(self, *keys):
    """
    Gets the values of multiple records of keys, as strings.

    :param keys: The keys of records to retrieve.
    :return: The future for the result status and a map of retrieved records.  Keys which don't match existing records are ignored.
    """
    pass  # native code

  def Set(self, key, value, overwrite=True):
    """
    Sets a record of a key and a value.

    :param key: The key of the record.
    :param value: The value of the record.
    :param overwrite: Whether to overwrite the existing value.  It can be omitted and then false is set.
    :return: The future for the result status.  If overwriting is abandoned, DUPLICATION_ERROR is set.
    """
    pass  # native code

  def SetMulti(self, overwrite=True, **records):
    """
    Sets multiple records of the keyword arguments.

    :param overwrite: Whether to overwrite the existing value if there's a record with the same key.  If true, the existing value is overwritten by the new value.  If false, the operation is given up and an error status is returned.
    :param records: Records to store.
    :return: The future for the result status.  If overwriting is abandoned, DUPLICATION_ERROR is set.
    """
    pass  # native code

  def Append(self, key, value, delim=""):
    """
    Appends data at the end of a record of a key.

    :param key: The key of the record.
    :param value: The value to append.
    :param delim: The delimiter to put after the existing record.
    :return: The future for the result status.

    If there's no existing record, the value is set without the delimiter.
    """
    pass  # native code

  def AppendMulti(self, delim="", **records):
    """
    Appends data to multiple records of the keyword arguments.

    :param delim: The delimiter to put after the existing record.
    :param records: Records to append.  Existing records with the same keys are overwritten.
    :return: The future for the result status.

    If there's no existing record, the value is set without the delimiter.
    """
    pass  # native code

  def CompareExchange(self, key, expected, desired):
    """
    Compares the value of a record and exchanges if the condition meets.

    :param key: The key of the record.
    :param expected: The expected value.  If it is None, no existing record is expected.
    :param desired: The desired value.  If it is None, the record is to be removed.
    :return: The future for the result status.  If the condition doesn't meet, INFEASIBLE_ERROR is set.
    """
    pass  # native code

  def Increment(self, key, inc=1, init=0):
    """
    Increments the numeric value of a record.

    :param key: The key of the record.
    :param inc: The incremental value.  If it is Utility.INT64MIN, the current value is not changed and a new record is not created.
    :param init: The initial value.
    :return: The future for the result status and the current value.

    The record value is stored as an 8-byte big-endian integer.  Negative is also supported.
    """
    pass  # native code

  def CompareExchangeMulti(self, expected, desired):
    """
    Compares the values of records and exchanges if the condition meets.

    :param expected: A sequence of pairs of the record keys and their expected values.  If the value is None, no existing record is expected.
    :param desired: A sequence of pairs of the record keys and their desired values.  If the value is None, the record is to be removed.
    :return: The future for the result status.  If the condition doesn't meet, INFEASIBLE_ERROR is returned.
    """
    pass  # native code

  def Clear(self):
    """
    Removes all records.

    :return: The future for the result status.
    """
    pass  # native code

  def Rebuild(self, **params):
    """
    Rebuilds the entire database.

    :param params: Optional parameters.
    :return: The future for the result status.

    The parameters work in the same way as with DBM::Rebuild.
    """
    pass  # native code

  def Synchronize(self, hard, **params):
    """
    Synchronizes the content of the database to the file system.

    :param hard: True to do physical synchronization with the hardware or false to do only logical synchronization with the file system.
    :param params: Optional parameters.
    :return: The future for the result status.

    The parameters work in the same way as with DBM::Synchronize.
    """
    pass  # native code

  def CopyFileData(self, dest_path, sync_hard=False):
    """
    Copies the content of the database file to another file.

    :param dest_path: A path to the destination file.
    :param sync_hard: True to do physical synchronization with the hardware.
    :return: The future for the result status.
    """
    pass  # native code

  def Export(self, dest_dbm):
    """
    Exports all records to another database.

    :param dest_dbm: The destination database.  The lefetime of the database object must last until the task finishes.
    :return: The future for the result status.
    """
    pass  # native code

  def ExportToFlatRecords(self, dest_file):
    """
    Exports all records of a database to a flat record file.

    :param dest_file: The file object to write records in.  The lefetime of the file object must last until the task finishes.
    :return: The future for the result status.

    A flat record file contains a sequence of binary records without any high level structure so it is useful as a intermediate file for data migration.
    """
    pass  # native code
    
  def ImportFromFlatRecords(self, src_file):
    """
    Imports records to a database from a flat record file.

    :param src_file: The file object to read records from.  The lefetime of the file object must last until the task finishes.
    :return: The future for the result status.
    """
    pass  # native code

  def Search(self, mode, pattern, capacity=0):
    """
    Searches the database and get keys which match a pattern.

    :param mode: The search mode.  "contain" extracts keys containing the pattern.  "begin" extracts keys beginning with the pattern.  "end" extracts keys ending with the pattern.  "regex" extracts keys partially matches the pattern of a regular expression.  "edit" extracts keys whose edit distance to the UTF-8 pattern is the least.  "editbin" extracts keys whose edit distance to the binary pattern is the least.
    :param pattern: The pattern for matching.
    :param capacity: The maximum records to obtain.  0 means unlimited.
    :return: The future for the result status and a list of keys matching the condition.
    """
    pass  # native code


class File:
  """
  Generic file implementation.

  All operations except for "open" and "close" are thread-safe; Multiple threads can access the same file concurrently.  You can specify a concrete class when you call the "open" method.  Every opened file must be closed explicitly by the "close" method to avoid data corruption.
  """

  def __init__(self):
    """
    Initializes the file object.
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

  def Open(self, path, writable, **params):
    """
    Opens a file.

    :param path: A path of the file.
    :param writable: If true, the file is writable.  If false, it is read-only.
    :param params: Optional parameters.
    :return: The result status.

    The optional parameters can include an option for the concurrency tuning.  By default, database operatins are done under the GIL (Global Interpreter Lock), which means that database operations are not done concurrently even if you use multiple threads.  If the "concurrent" parameter is true, database operations are done outside the GIL, which means that database operations can be done concurrently if you use multiple threads.  However, the downside is that swapping thread data is costly so the actual throughput is often worse in the concurrent mode than in the normal mode.  Therefore, the concurrent mode should be used only if the database is huge and it can cause blocking of threads in multi-thread usage.

    The optional parameters can include options for the file opening operation.
      - truncate (bool): True to truncate the file.
      - no_create (bool): True to omit file creation.
      - no_wait (bool): True to fail if the file is locked by another process.
      - no_lock (bool): True to omit file locking.
      - sync_hard (bool): True to do physical synchronization when closing.

    The optional parameter "file" specifies the internal file implementation class.  The default file class is "MemoryMapAtomicFile".  The other supported classes are "StdFile", "MemoryMapAtomicFile", "PositionalParallelFile", and "PositionalAtomicFile".

    For the file "PositionalParallelFile" and "PositionalAtomicFile", these optional parameters are supported.
      - block_size (int): The block size to which all blocks should be aligned.
      - access_options (str): Values separated by colon.  "direct" for direct I/O.  "sync" for synchrnizing I/O, "padding" for file size alignment by padding, "pagecache" for the mini page cache in the process.
    """
    pass  # native code

  def Close(self):
    """
    Closes the file.

    :return: The result status.
    """
    pass  # native code

  def Read(self, off, size, status=None):
    """
    Reads data.
    
    :param off: The offset of a source region.
    :param size: The size to be read.
    :param status: A status object to which the result status is assigned.  It can be omitted.
    :return: The bytes value of the read data or None on failure.
    """
    pass  # native code

  def ReadStr(self, off, size, status=None):
    """
    Reads data as a string.
    
    :param off: The offset of a source region.
    :param size: The size to be read.
    :param status: A status object to which the result status is assigned.  It can be omitted.
    :return: The string value of the read data or None on failure.
    """
    pass  # native code

  def Write(self, off, data):
    """
    Writes data.

    :param off: The offset of the destination region.
    :param data: The data to write.
    :return: The result status.
    """
    pass  # native code

  def Append(self, data, status=None):
    """
    Appends data at the end of the file.

    :param data: The data to write.
    :param status: A status object to which the result status is assigned.  It can be omitted.
    :return: The offset at which the data has been put, or None on failure.
    """
    pass  # native code

  def Truncate(self, size):
    """
    Truncates the file.

    :param size: The new size of the file.
    :return: The result status.

    If the file is shrunk, data after the new file end is discarded.  If the file is expanded, null codes are filled after the old file end.
    """
    pass  # native code

  def Synchronize(self, hard, off=0, size=0):
    """
    Synchronizes the content of the file to the file system.

    :param hard: True to do physical synchronization with the hardware or false to do only logical synchronization with the file system.
    :param off: The offset of the region to be synchronized.
    :param size: The size of the region to be synchronized.  If it is zero, the length to the end of file is specified.
    :return: The result status.

    The pysical file size can be larger than the logical size in order to improve performance by reducing frequency of allocation.  Thus, you should call this function before accessing the file with external tools.
    """
    pass  # native code

  def GetSize(self):
    """
    Gets the size of the file.

    :return: The size of the file or None on failure.
    """
    pass  # native code

  def GetPath(self):
    """
    Gets the path of the file.

    :return: The path of the file or None on failure.
    """
    pass  # native code

  def Search(self, mode, pattern, capacity=0):
    """
    Searches the file and get lines which match a pattern.

    :param mode: The search mode.  "contain" extracts lines containing the pattern.  "begin" extracts lines beginning with the pattern.  "end" extracts lines ending with the pattern.  "regex" extracts lines partially matches the pattern of a regular expression.  "edit" extracts lines whose edit distance to the UTF-8 pattern is the least.  "editbin" extracts lines whose edit distance to the binary pattern is the least.
    :param pattern: The pattern for matching.
    :param capacity: The maximum records to obtain.  0 means unlimited.
    :return: A list of lines matching the condition.
    """
    pass  # native code


# END OF FILE
