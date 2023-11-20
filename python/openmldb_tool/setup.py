#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright 2021 4Paradigm
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from setuptools import setup, find_packages

setup(
    name="openmldb-tool",
    version='0.8.3a0',
    author="OpenMLDB Team",
    author_email=" ",
    url="https://github.com/4paradigm/OpenMLDB",
    description="OpenMLDB Tool",
    license="copyright 4paradigm.com",
    classifiers=[
        "Programming Language :: Python :: 3",
    ],
    install_requires=[
        "openmldb >= 0.8.1",
        "absl-py",
        "pyyaml",
        "paramiko",
        "termplotlib",
        "requests",
    ],
    extras_require={
        "pb": [
            "protobuf==3.6.1",
            "beautifulsoup4",
        ],
        "test": [
            "openmldb-tool[pb]",
            "pytest",
        ],
    },
    packages=find_packages(exclude=["tests"]),
    exclude_package_data={"openmldb-tool": ["diagnostic_tool/common_err.yml"]},
    entry_points={
        "console_scripts": ["openmldb_tool = diagnostic_tool.diagnose:run"],
    },
    zip_safe=False,
)
