# test_dbms.py
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

#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
#

"""

"""

from test_base import *
import time
import fesql

def run_db_check(dbms, ns, result):
    assert dbms
    status = fesql.Status()
    dbms.CreateDatabase(ns, status)
    assert status.code == result

def test_dbms_create_db():
    dbms = fesql.CreateDBMSSdk(dbms_endpoint)
    cases= [("ns1" + str(time.time()) , 0), ('ns2' + str(time.time()), 0)]
    for case in cases:
        yield run_db_check, dbms, case[0], case[1]

def test_dbms_valid():
    dbms = fesql.CreateDBMSSdk("xxxxx")
    assert not dbms


