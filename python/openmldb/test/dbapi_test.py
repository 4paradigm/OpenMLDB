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

import openmldb
import pytest
import logging

from case_conf import OpenMLDB_ZK_CLUSTER, OpenMLDB_ZK_PATH

logging.basicConfig(level=logging.WARNING)

class TestOpenmldbDBAPI:

    def setup_class(self):
        self.db = openmldb.dbapi.connect('db_test', OpenMLDB_ZK_CLUSTER, OpenMLDB_ZK_PATH)
        self.cursor = self.db.cursor()

    def execute(self, sql):
        try:
            self.cursor.execute(sql)
            return True
        except Exception as e:
            return

    def test_create_table(self):
        self.cursor.execute('create table new_table (x string, y int);')
        assert "new_table" in self.cursor.get_all_tables()
        with pytest.raises(Exception):
            assert self.execute("create table ")

    def test_insert(self):
        try:
            self.cursor.execute("insert into new_table values('first', 100);")
        except Exception as e:
            assert False
        result = self.cursor.execute("select * from new_table;").fetchone()
        assert 'first' in result
        assert 100 in result

        with pytest.raises(Exception):
            assert self.execute("insert into new_table values(100, 'first');")
        with pytest.raises(Exception):
            assert self.execute(
                "insert into new_table values({'x':100, 'y':'first'});")

    def test_select_conditioned(self):
        self.cursor.execute("insert into new_table values('second', 200);")
        result = self.cursor.execute(
            "select * from new_table where x = 'second';").fetchone()
        assert 'second' in result
        assert 200 in result

    def test_drop_table(self):
        try:
            self.cursor.execute("drop table new_table;")
        except Exception as e:
            assert False
        assert "new_table" not in self.cursor.get_all_tables()

        with pytest.raises(Exception):
            assert self.execute("drop table new_table;")

    def teardown_class(self):
        self.cursor.close()
