Module and Classes
==================

.. autosummary::

   tkrzw
   tkrzw.Utility
   tkrzw.Status
   tkrzw.StatusException
   tkrzw.Future
   tkrzw.DBM
   tkrzw.Iterator
   tkrzw.AsyncDBM
   tkrzw.File

Introduction
============

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
============

Install the latest version of Tkrzw beforehand and get the package of the Python binding of Tkrzw.  Python 3.6 or later is required to use this package.

Enter the directory of the extracted package then perform installation.  If your system has the another command except for the "python3" command, edit the Makefile beforehand.::

 make
 make check
 sudo make install

Example
=======

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

The following code is a typical example of coroutine usage.  The AsyncDBM class manages a thread pool and handles database operations in the background in parallel.  Each Method of AsyncDBM returns a Future object to monitor the result.  The Future class implements the awaitable protocol so that the instance is usable with the "await" sentence to await the operation while yielding the execution ownership.::

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
     # Awaits the operation while yielding the execution ownership.
     await future
     status, value = future.Get()
     if status == tkrzw.Status.SUCCESS:
         print("VALUE: " + value)

     # Releases the asynchronous adapter.
     adbm.Destruct()

     # Closes the database.
     dbm.Close()

 asyncio.run(main())

Indices and tables
==================

.. toctree::
   :maxdepth: 4
   :caption: Contents:

   tkrzw

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
