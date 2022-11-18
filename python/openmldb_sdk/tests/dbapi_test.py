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

import logging
import sys
import os
from openmldb.dbapi import connect
from openmldb.dbapi import DatabaseError
import pytest

from .case_conf import OpenMLDB_ZK_CLUSTER, OpenMLDB_ZK_PATH

logging.basicConfig(level=logging.WARNING)


class TestOpenmldbDBAPI:
    cursor = None

    @classmethod
    def setup_class(cls):
        db = connect(database='db_test', zk=OpenMLDB_ZK_CLUSTER,
                     zkPath=OpenMLDB_ZK_PATH)
        cls.cursor = db.cursor()
        cls.cursor.execute("create database if not exists db_test;")
        cls.cursor.execute('create table new_table (x string, y int);')
        assert "new_table" in cls.cursor.get_all_tables()

    @classmethod
    def teardown_class(cls):
        cls.cursor.execute("drop table new_table;")
        assert "new_table" not in cls.cursor.get_all_tables()

        with pytest.raises(DatabaseError):
            cls.cursor.execute("drop table new_table;")
        cls.cursor.close()

    def test_invalid_create(self):
        with pytest.raises(DatabaseError):
            self.cursor.execute("create table ")

    def test_simple_insert_select(self):
        self.cursor.execute("insert into new_table values('first', 100);")
        result = self.cursor.execute("select * from new_table;").fetchone()
        assert 'first' in result
        assert 100 in result

        with pytest.raises(DatabaseError):
            self.cursor.execute(
                "insert into new_table values(1001, 'first1');")
        with pytest.raises(DatabaseError):
            self.cursor.execute(
                "insert into new_table values({'x':1001, 'y':'first1'});")
        
        # TODO: won't fail
        with pytest.raises(DatabaseError) as e:
            self.cursor.execute("insert into new_table values('aaa', 1002),('foo', 999999999999999),('bar', 99999999999999)")
        # row 1 and 2 failed
        print(e.value)
        assert '1,2' in str(e.value)

    def test_select_conditioned(self):
        self.cursor.execute("insert into new_table values('second', 200);")
        result = self.cursor.execute(
            "select * from new_table where x = 'second';").fetchone()
        assert 'second' in result
        assert 200 in result

    def test_custom_order_insert(self):
        self.cursor.execute(
            "insert into new_table (y, x) values(300, 'third');")
        self.cursor.execute("insert into new_table (y, x) values(?, ?);",
                            (300, 'third'))
        self.cursor.execute("insert into new_table (y, x) values(?, ?);", {
            'x': 'third',
            'y': 300
        })

    @pytest.mark.skip(reason="test may fail on mac")
    def test_request_timeout(self):
        """
        Note: this test works now(select > 0ms). If you can't reach the timeout, redesign the test.
        """
        # requestTimeout -1 means wait indefinitely
        db = connect(database='db_test',
                     zk=OpenMLDB_ZK_CLUSTER,
                     zkPath=OpenMLDB_ZK_PATH,
                     requestTimeout=0)
        cursor = db.cursor()
        rs = cursor.execute(
            "insert into new_table (y, x) values(400, 'a'),(401,'b'),(402, 'c');"
        )
        # insert no result
        assert not rs

        with pytest.raises(DatabaseError) as e:
            cursor.execute("select * from new_table where y=402;").fetchall()
        assert 'execute select fail' in str(e.value)

    def test_connect_options(self):
        db = connect(database='db_test',
                     zk=OpenMLDB_ZK_CLUSTER,
                     zkPath=OpenMLDB_ZK_PATH,
                     requestTimeout=100000,
                     maxSqlCacheSize=100)


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", os.path.abspath(__file__)]))
