# python/test/test_base.py
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

"""
"""

import fesql

dbms_endpoint="127.0.0.1:9211"

def exec_query(dbms, db, sql):
    if not dbms:
        return False, None, None
    status = fesql.Status();
    rs = dbms.ExecuteQuery(db, sql, status)
    if status.code == 0:
        return True, None, rs
    return False, status.msg, None

