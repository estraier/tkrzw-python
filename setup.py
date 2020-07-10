#! /usr/bin/python3
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Building configurations
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

from distutils.core import *
from subprocess import *

package_name = 'Tkrzw'
package_version = '0.1'
package_description = 'a set of implementations of DBM'
package_author = 'Mikio Hirabayashi'
package_author_email = 'mikio@dbmx.net'
package_url = 'http://dbmx.net/tkrzw/'
module_name = 'tkrzw'

def getcmdout(cmdargs):
    try:
        pipe = Popen(cmdargs, stdout=PIPE)
        output = pipe.communicate()[0].decode('utf-8')
    except:
        output = ""
    return output.strip()

include_dirs = []
myincopts = getcmdout(['tkrzw_build_util', 'config', '-i']).split()
for incopt in myincopts:
    if incopt.startswith('-I'):
        incdir = incopt[2:]
        include_dirs.append(incdir)
if len(include_dirs) < 1:
    include_dirs = ['/usr/local/include']

extra_compile_args = ["-std=c++17", "-Wall"]
sources = ['tkrzw.cc']

library_dirs = []
libraries = []
mylibopts = getcmdout(['tkrzw_build_util', 'config', '-l']).split()
for libopt in mylibopts:
    if libopt.startswith('-L'):
        libdir = libopt[2:]
        library_dirs.append(libdir)
    elif libopt.startswith('-l'):
        libname = libopt[2:]
        libraries.append(libname)
if len(library_dirs) < 1:
    library_dirs = ['/usr/local/lib']
if len(libraries) < 1:
    if (os.uname()[0] == "Darwin"):
        libraries = ['tkrzw', 'stdc++', 'pthread', 'm', 'c']
    else:
        libraries = ['tkrzw', 'stdc++', 'rt', 'pthread', 'm', 'c']

module = Extension(module_name,
                   include_dirs = include_dirs,
                   extra_compile_args = extra_compile_args,
                   sources = sources,
                   library_dirs = library_dirs,
                   libraries = libraries)

setup(name = package_name,
      version = package_version,
      description = package_description,
      author = package_author,
      author_email = package_author_email,
      url = package_url,
      ext_modules = [module])
