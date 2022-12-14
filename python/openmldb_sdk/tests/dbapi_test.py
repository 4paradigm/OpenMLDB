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
    cursor_without_db = None
    cursor = None
    db_name = "dbapi_test"

    @classmethod
    def setup_class(cls):
        # connect without db to create it
        db = connect(zk=OpenMLDB_ZK_CLUSTER, zkPath=OpenMLDB_ZK_PATH)
        cls.cursor_without_db = db.cursor()
        cls.cursor_without_db.execute(
            f"create database if not exists {cls.db_name};")
        # then we can use the db to connect
        db = connect(database=cls.db_name,
                     zk=OpenMLDB_ZK_CLUSTER,
                     zkPath=OpenMLDB_ZK_PATH)
        cls.cursor = db.cursor()
        cls.cursor.execute("create database if not exists db_test;")
        cls.cursor.execute("create table new_table (x string, y int16);")
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
        assert "first" in result
        assert 100 in result

        with pytest.raises(DatabaseError):
            self.cursor.execute("insert into new_table values(1001, 'first1');")
        with pytest.raises(DatabaseError):
            self.cursor.execute(
                "insert into new_table values({'x':1001, 'y':'first1'});"
            )

        # row 1&2 have the invalid value of y(int16)
        with pytest.raises(DatabaseError) as e:
            self.cursor.execute(
                "insert into new_table values('aaa', 1002),('foo', 999999999999999),('bar', 99999999999999)"
            )
        print(e.value)
        assert "fail to parse row[1]" in str(e.value)

    def test_select_conditioned(self):
        self.cursor.execute("insert into new_table values('second', 200);")
        result = self.cursor.execute(
            "select * from new_table where x = 'second';"
        ).fetchone()
        assert "second" in result
        assert 200 in result

    def test_custom_order_insert(self):
        self.cursor.execute("insert into new_table (y, x) values(300, 'third');")
        self.cursor.execute(
            "insert into new_table (y, x) values(?, ?);", (300, "third")
        )
        self.cursor.execute(
            "insert into new_table (y, x) values(?, ?);", {"x": "third", "y": 300}
        )

    @pytest.mark.skip(reason="test may fail on mac")
    def test_request_timeout(self):
        """
        Note: this test works now(select > 0ms). If you can't reach the timeout, redesign the test.
        """
        # requestTimeout -1 means wait indefinitely
        db = connect(database=self.db_name,
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
        assert "execute select fail" in str(e.value)

    def test_connect_options(self):
        connect(database=self.db_name,
                zk=OpenMLDB_ZK_CLUSTER,
                zkPath=OpenMLDB_ZK_PATH,
                requestTimeout=100000,
                maxSqlCacheSize=100)

    def test_invalid_database_arg(self):
        with pytest.raises(DatabaseError) as e:
            connect(database='db_test_876371',
                    zk=OpenMLDB_ZK_CLUSTER,
                    zkPath=OpenMLDB_ZK_PATH)
        assert 'database not found' in str(e.value)

    def test_cursor_without_db(self):
        self.cursor_without_db.execute(f"use {self.db_name}")
        self.cursor_without_db.execute(
            "insert into new_table values('abcd', 500);")
        result = self.cursor_without_db.execute(
            "select * from new_table where y = 500;").fetchall()
        assert len(result) == 1
        assert 'abcd' in result[0]
        assert 500 in result[0]
        self.cursor_without_db.executemany(
            "insert into new_table values(?, ?);", [('abcde', 501)])
        result = self.cursor_without_db.execute(
            "select * from new_table where y = 501;").fetchall()
        assert len(result) == 1
        assert 'abcde' in result[0]
        assert 501 in result[0]


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", os.path.abspath(__file__)]))
