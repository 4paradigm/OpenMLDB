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
    name='openmldb',
    version='0.8.3a0',
    author='OpenMLDB Team',
    author_email=' ',
    url='https://github.com/4paradigm/OpenMLDB',
    description='OpenMLDB Python SDK',
    license="copyright 4paradigm.com",
    classifiers=[
        'Programming Language :: Python :: 3',
    ],
    install_requires=[
        "importlib-metadata < 5.0",
        "sqlalchemy <= 1.4.50",
        "IPython <= 7.30.1",
        "prettytable <= 3.1.0",
    ],
    extras_require={'test': [
        "pytest",
        "tox",
    ]},
    include_package_data=True,
    package_data={'': ['*.so']},
    packages=find_packages(exclude=['tests']),
    entry_points={
        'sqlalchemy.dialects': [
            'openmldb = openmldb.sqlalchemy_openmldb.openmldb_dialect:OpenmldbDialect',
        ],
    },
    zip_safe=False,
)
