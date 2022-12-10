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
import sys
import os
import openmldb
import pytest
import logging

from .case_conf import OpenMLDB_ZK_CLUSTER, OpenMLDB_ZK_PATH

logging.basicConfig(level=logging.WARNING)


class TestSQLMagicOpenMLDB:

    def setup_class(self):
        self.db = openmldb.dbapi.connect(database='db_test',
                                         zk=OpenMLDB_ZK_CLUSTER,
                                         zkPath=OpenMLDB_ZK_PATH)
        self.ip = openmldb.sql_magic.register(self.db, test=True)

    def execute(self, magic_name, sql):
        try:
            self.ip.run_line_magic(magic_name, sql)
            return True
        except Exception as e:
            return

    def test_create_table(self):
        try:
            self.ip.run_cell_magic(
                'sql', '', "create table magic_table (x string, y int);")
        except Exception as e:
            assert False
        assert "magic_table" in self.db.cursor().get_all_tables()

        with pytest.raises(Exception):
            assert self.execute('sql', "create table magic_table;")

    def test_insert(self):
        try:
            self.ip.run_line_magic(
                'sql', "insert into magic_table values('first', 100);")
        except Exception as e:
            assert False

        with pytest.raises(Exception):
            assert self.execute(
                'sql', "insert into magic_table values(200, 'second');")

        with pytest.raises(Exception):
            assert self.execute(
                'sql', "insert into magic_table values({x: 'first', y:100});")

    def test_select(self):
        try:
            self.ip.run_line_magic('sql', "select * from magic_table;")
        except Exception as e:
            assert False

    def test_drop(self):
        try:
            self.ip.run_line_magic('sql', "drop table magic_table;")
        except Exception as e:
            assert False

        assert "magic_table" not in self.db.cursor().get_all_tables()


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", os.path.abspath(__file__)]))
