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
from openmldb.dbapi import connect
import pytest
from datetime import datetime

from .case_conf import OpenMLDB_ZK_CLUSTER, OpenMLDB_ZK_PATH


class TestOpenmldbDBAPI:
    cursor_without_db = None
    cursor = None
    db_name = "dbapi_test"
    table = "dbapi_param_query"
    schema_str = "col0 bigint, col1 date, col2 string, col3 string, col4 timestamp, col5 int, " \
                 "index(key=col2, ts=col0), index(key=col2, ts=col4)"

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
        cls.cursor.execute("create database if not exists {};".format(cls.db_name))
        assert cls.db_name in cls.cursor.get_databases()

    def recreate_table(self, table, schema):
        if table in self.cursor.get_tables(self.db_name):
            self.cursor.execute("drop table {}".format(table))
        assert table not in self.cursor.get_tables(self.db_name)

        self.cursor.execute("create table {}({}) OPTIONS(partitionnum=1);".format(table, schema))
        assert table in self.cursor.get_tables(self.db_name)

    @staticmethod
    def build_par_sql(test_row, mask_idx=None):
        if not mask_idx:
            mask_idx = range(0, len(test_row))
        if type(test_row) is tuple:
            stringify = [
                '\'' + x + '\'' if type(x) is str else str(x) for x in test_row
            ]
            if type(mask_idx) is list:
                for idx in mask_idx:
                    stringify[idx] = '?'
            return ', '.join(stringify)

    @staticmethod
    def build_par_data(test_rows, mask_idx=[], is_dict=False):
        if not mask_idx:
            mask_idx = range(0, len(test_rows[0]))
        data_list = []
        for row in test_rows:
            if is_dict:
                data = {}
                for i in mask_idx:
                    data[f"col{i}"] = row[i]
            else:
                data = []
                for i in mask_idx:
                    data.append(row[i])
                data = tuple(data)
            data_list.append(data)
        return data_list

    def test_more_parameterized_insert(self):
        self.recreate_table(self.table, self.schema_str)
        test_rows = [
            (1000 + i, '2022-05-0' + str(i), 'province' + str(i % 4), 'city' + str(i), (1590738990 + i) * 1000, i)
            for i in range(1, 10)]
        # executemany: values(?,?,?,?,?,?), [(#,#,#,#,#,#)...]
        self.cursor.executemany(f"insert into {self.table} values (?, ?, ?, ?, ?, ?);", test_rows[0:4])
        # executemany: values(?,?,?,#,#,#), [(#,#,#)]
        self.cursor.executemany(f"insert into {self.table} values ({self.build_par_sql(test_rows[4], [0, 1, 2])});",
                                self.build_par_data([test_rows[4]], [0, 1, 2]))
        # executemany: values(#,#,#,?,?,?), [{#:#,#:#,#:#}]
        self.cursor.executemany(f"insert into {self.table} values ({self.build_par_sql(test_rows[5], [3, 4, 5])});",
                                self.build_par_data([test_rows[5]], [3, 4, 5], True))
        # executemany: values(?,#,?,?,#,?), [(#,#)]
        self.cursor.executemany(f"insert into {self.table} values ({self.build_par_sql(test_rows[6], [1, 4])});",
                                self.build_par_data([test_rows[6]], [1, 4]))
        # executemany: values(?,?,?,?,?,?), (#,#,#,#,#,#)
        self.cursor.execute(f"insert into {self.table} values (?, ?, ?, ?, ?, ?);", test_rows[7])
        # executemany: values(?,?,?,#,#,#), (#,#,#)
        self.cursor.execute(f"insert into {self.table} values ({self.build_par_sql(test_rows[8], [0, 1, 2])});",
                            self.build_par_data([test_rows[8]], [0, 1, 2])[0])
        result = self.cursor.execute("select * from {};".format(self.table)).fetchall()
        result = sorted(list(result), key=lambda x: x[0])
        assert result == test_rows

    def test_more_parameterized_query(self):
        self.recreate_table(self.table, self.schema_str)
        test_rows = [
            (1000 + i, '2022-05-0' + str(i), 'province' + str(i % 4), 'city' + str(i), (1590738990 + i) * 1000, i)
            for i in range(1, 5)]
        self.cursor.executemany(f'insert into {self.table} values (?, ?, ?, ?, ?, ?);', test_rows)
        # the data type of the timestamp field in the query conditions needs to be consistent
        new_test_rows = []
        for i in range(0, len(test_rows)):
            row_list = list(test_rows[i])
            row_list[4] = datetime.fromtimestamp(row_list[4] / 1000)
            new_test_rows.append(tuple(row_list))
        # test full-field query conditions
        result = self.cursor.execute(
            f"select * from {self.table} where col0 = ? and col1 = ? and col2 = ? and col3 = ? and col4 = ? and col5 = ?",
            self.build_par_data(new_test_rows[0:1])[0]).fetchall()
        assert result == [test_rows[0]]


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", os.path.abspath(__file__)]))
