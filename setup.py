#! /usr/bin/python3
# -*- coding: utf-8 -*-
# --------------------------------------------------------------------------------------------------
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
# --------------------------------------------------------------------------------------------------

import os
import platform
import subprocess

from setuptools import Extension, setup

package_name = 'tkrzw'
package_version = '0.1'
package_description = 'a set of implementations of DBM'
package_author = 'Mikio Hirabayashi'
package_author_email = 'hirarin@gmail.com'
package_url = 'http://dbmx.net/tkrzw/'
module_name = 'tkrzw'

extra_compile_args = ["-std=c++17", "-Wall"]
sources = ['tkrzw.cc']

ld_paths = [
    '/usr/local/lib'
]
os.environ['LD_LIBRARY_PATH'] = os.environ.get('LD_LIBRARY_PATH', '') + ':'.join(ld_paths)


def parse_build_flags(cmd_args):
    result = {}
    output = subprocess.check_output(cmd_args).strip().decode()
    for item in output.split():
        flag = item[0:2]
        result.setdefault(flag, []).append(item[2:])
    return result


# Parse include dirs
flags = parse_build_flags(['tkrzw_build_util', 'config', '-i'])
include_dirs = flags.get('-I', []) or ['/usr/local/include']

# Parse library dirs and libraries
flags = parse_build_flags(['tkrzw_build_util', 'config', '-l'])
library_dirs = flags.get('-L', []) or ['/usr/local/lib']
libraries = flags.get('-l', [])
if not libraries:
    libraries = ['tkrzw', 'stdc++', 'pthread', 'm', 'c']
    if platform.system() != "Darwin":
        libraries.merge(['rt', 'atomic'])

setup(
    name=package_name,
    version=package_version,
    description=package_description,
    author=package_author,
    author_email=package_author_email,
    url=package_url,
    ext_modules=[
        Extension(
            module_name,
            include_dirs=include_dirs,
            extra_compile_args=extra_compile_args,
            sources=sources,
            library_dirs=library_dirs,
            libraries=libraries
        ),
    ]
)
