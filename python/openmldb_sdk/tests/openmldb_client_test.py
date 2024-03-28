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

import os
import logging
import sys
from datetime import date
from datetime import datetime
import pytest

# fmt:off
import openmldb
import sqlalchemy as db
from sqlalchemy.exc import DatabaseError
# fmt:on
from .case_conf import OpenMLDB_ZK_CLUSTER, OpenMLDB_ZK_PATH

logging.basicConfig(level=logging.DEBUG)


class TestOpenMLDBClient:
    """
    sqlalchemy connection test
    """
    engine = None
    connection = None
    db = "openmldb_client_test"

    @classmethod
    def setup_class(cls):
        cls.engine = db.create_engine('openmldb:///?zk={}&zkPath={}'.format(OpenMLDB_ZK_CLUSTER, OpenMLDB_ZK_PATH))
        cls.connection = cls.engine.connect()
        cls.connection.exec_driver_sql("create database if not exists {};".format(cls.db))
        cls.connection.exec_driver_sql(f"use {cls.db}")

    @staticmethod
    def has_table(connection, table_name):
        return connection.dialect.has_table(connection, table_name, schema=None)

    def recreate_table(self, table, schema):
        if self.has_table(self.connection, table):
            self.connection.exec_driver_sql("drop table {}".format(table))
        assert not self.has_table(self.connection, table)
        # key is col3, partitionnum==1
        self.connection.exec_driver_sql("create table {}({}) OPTIONS(partitionnum=1);".format(table, schema))
        assert self.has_table(self.connection, table)

    @staticmethod
    def convert_to_dicts(column_names, tuple_rows):
        return [
            TestOpenMLDBClient.convert_to_dict(column_names, tu)
            for tu in tuple_rows
        ]

    @staticmethod
    def convert_to_dict(column_names, test_row):
        return dict(zip(column_names, test_row))

    @staticmethod
    def stringify_join(sth, mask_idx=None):
        if type(sth) is tuple:
            stringify = [
                '\'' + x + '\'' if type(x) is str else str(x) for x in sth
            ]
            if type(mask_idx) is list:
                for idx in mask_idx:
                    stringify[idx] = '?'
            return ', '.join(stringify)

    @staticmethod
    def schema_dict(schema_str):
        d = {}
        for part in schema_str.split(","):
            part = part.strip()
            if part.lower().startswith("index("):
                break
            kv = part.split(" ")
            d[kv[0]] = kv[1]
        return d

    def test_basic(self):
        table = "test_basic"
        schema_str = "col1 bigint, col2 date, col3 string, col4 string, col5 int, index(key=col3, ts=col1)"
        column_names = ['col' + str(i + 1) for i in range(5)]

        self.recreate_table(table, schema_str)

        test_rows = [(1000, '2020-12-25', 'guangdong', '广州', 1),
                     (1001, '2020-12-26', 'hefei', 'anhui', 2),
                     (1002, '2020-12-27', 'fujian', 'fuzhou', 3),
                     (1003, '2020-12-28', 'jiangxi', 'nanchang', 4),
                     (1004, '2020-12-29', 'hubei', 'wuhan', 5)]

        insert0 = "insert into {} values({});".format(
            table, self.stringify_join(test_rows[0]))
        # 1001, '2020-12-26', 'hefei', ?, ? - anhui 2
        insert1 = "insert into {} values({});".format(
            table, self.stringify_join(test_rows[1], [3, 4]))
        # 1002, '2020-12-27', ?, ?, 3 - fujian fuzhou
        insert2 = "insert into {} values({});".format(
            table, self.stringify_join(test_rows[2], [2, 3]))
        # all ? - 1003 2020-11-28 jiangxi nanchang 4
        insert3 = "insert into {} values(?, ?, ?, ?, ?);".format(table)
        # 1004, ?, 'hubei', 'wuhan', 5 - 2020-11-29
        insert4 = "insert into {} values({});".format(
            table, self.stringify_join(test_rows[4], [1]))
        self.connection.exec_driver_sql(insert0)
        self.connection.exec_driver_sql(insert1, ({
            "col4": test_rows[1][3],
            "col5": test_rows[1][4]
        }))
        self.connection.exec_driver_sql(insert2, ({
            "col3": test_rows[2][2],
            "col4": test_rows[2][3]
        }))
        self.connection.exec_driver_sql(insert3, self.convert_to_dicts(column_names, [test_rows[3]]))
        self.connection.exec_driver_sql(insert4, [{"col2": test_rows[4][1]}])

        # order by is not supported now
        result = self.connection.exec_driver_sql("select * from {};".format(table))
        # fetch many times
        result_list = result.fetchmany()
        assert len(result_list) == 1
        result_list += result.fetchmany(size=2)
        assert len(result_list) == 3
        result_list += result.fetchmany(size=4)
        assert len(result_list) == 5
        # use col1 to sort
        result_list.sort(key=lambda y: y[0])
        assert result_list == test_rows

        # insert invalid type
        with pytest.raises(TypeError):
            self.connection.exec_driver_sql(insert1, (2, 2))
        # insert many
        new_rows = [(1005, "2020-12-29", "shandong", 'jinan', 6),
                    (1006, "2020-12-30", "fujian", 'fuzhou', 7)]
        # insert3 is all ?
        self.connection.exec_driver_sql(insert3, self.convert_to_dicts(column_names, new_rows))
        test_rows += new_rows

        # test fetch all
        rs = self.connection.exec_driver_sql("select * from {};".format(table))
        result = sorted(rs.fetchall(), key=lambda x: x[0])
        assert result == test_rows

        # test convert to list
        rs = self.connection.exec_driver_sql("select * from {};".format(table))
        rs = sorted(list(rs), key=lambda x: x[0])
        assert rs == test_rows

        # test condition select
        rs = self.connection.exec_driver_sql("select * from {} where col3 = 'hefei';".format(table))
        # hefei row idx == 1
        assert list(rs) == [test_rows[1]]

        # test request mode, select sql with dict parameters(one dict or list of dict)
        request_row = (9999, "2020-12-27", "zhejiang", "hangzhou", 100)
        rs = self.connection.exec_driver_sql("select * from {};".format(table),
                                             self.convert_to_dict(column_names, request_row))
        assert list(rs) == [request_row]
        rs = self.connection.exec_driver_sql("select * from {};".format(table),
                                             self.convert_to_dicts(column_names, [request_row]))
        assert list(rs) == [request_row]

        # test parameterized query in batch mode, select sql with tuple or tuple list
        rs = self.connection.exec_driver_sql("select * from {} where col3 = ?;".format(table), tuple(['hefei']))
        assert list(rs) == [test_rows[1]]
        # rs = self.connection.exec_driver_sql("select * from {} where col3 = ?;".format(table), ['hefei'])
        # assert list(rs) == [test_rows[1]]
        # rs = self.connection.exec_driver_sql("select * from {} where col3 = ?;".format(table), [('hefei')])
        # assert list(rs) == [test_rows[1]]

    def test_procedure(self):
        # TODO(hw): creating procedure is not recommended, test deploy
        table = "test_procedure"
        schema_str = "col1 bigint, col2 date, col3 string, col4 string, col5 int, index(key=col3, ts=col1)"

        column_names = ['col' + str(i + 1) for i in range(5)]

        # try to delete sp before recreate, cause table may have associated deployment 'sp'
        try:
            self.connection.exec_driver_sql("drop procedure sp;")
        except DatabaseError as e:
            assert "not found" in str(e)

        self.recreate_table(table, schema_str)

        self.connection.exec_driver_sql(
            "create procedure sp (col1 bigint, col2 date, col3 string, col4 string, col5 int) "
            "begin select * from {}; end;".format(table))

        mouse = self.engine.raw_connection().cursor()
        test_rows = [(1002, '2020-12-27', 'fujian', 'fuzhou', 3)]
        # parameters should be one dict, not list
        rs = mouse.callproc("sp",
                            self.convert_to_dict(column_names, test_rows[0]))
        assert list(rs.fetchall()) == test_rows

        self.connection.exec_driver_sql("drop procedure sp;")

        # test batch request mode
        mouse2 = self.engine.raw_connection().cursor()
        rs = mouse2.batch_row_request(
            "select * from {};".format(table), (),
            (self.convert_to_dict(column_names, test_rows[0])))
        assert list(rs.fetchall()) == test_rows

        test_rows += [(1003, "2020-12-28", "jiangxi", "nanchang", 4)]
        mouse3 = self.engine.raw_connection().cursor()
        rs = mouse3.batch_row_request(
            "select * from {};".format(table), (),
            self.convert_to_dicts(column_names, test_rows))
        assert list(rs.fetchall()) == test_rows

    def test_more_parameterized_query(self):
        table = "test_param_query"
        schema_str = "col1 bigint, col2 date, col3 string, col4 string, col5 int, col6 timestamp, " \
                     "index(key=col3, ts=col1), index(key=col3, ts=col6)"
        schema = self.schema_dict(schema_str)
        self.recreate_table(table, schema_str)
        # 1-9
        test_rows = [(1000 + i, '2022-05-0' + str(i), 'province' + str(i % 4),
                      'city' + str(i), i, (1590738990 + i) * 1000)
                     for i in range(1, 10)]

        self.connection.exec_driver_sql(
            "insert into {} values (?, ?, ?, ?, ?, ?);".format(table),
            self.convert_to_dicts(schema, test_rows))

        rs = self.connection.exec_driver_sql(
            "select * from {} where col3 = ?;".format(table), tuple(['province1']))
        rs = sorted(list(rs), key=lambda x: x[0])
        assert rs == test_rows[0::4]

        # test parameterized query in batch mode case 2
        rs = self.connection.exec_driver_sql(
            "select * from {} where col3 = ?;".format(table), tuple(['province2']))
        rs = sorted(list(rs), key=lambda x: x[0])
        assert rs == test_rows[1::4]

        # test parameterized query in batch mode case 3
        rs = self.connection.exec_driver_sql(
            "select * from {} where col3 = ?;".format(table), tuple(['province3']))
        rs = sorted(list(rs), key=lambda x: x[0])
        assert rs == test_rows[2::4]

        # test parameterized query in batch mode case 3 and col1 < 1004, only one row[2]
        rs = self.connection.exec_driver_sql("select * from {} where col3 = ? and col1 < ?;".format(table),
                                             ('province3', 1004))
        assert list(rs) == [test_rows[2]]

        # test parameterized query in batch mode case 4
        rs = self.connection.exec_driver_sql("select * from {} where col3 = ? and col1 < ? and col2 < ?;".format(table),
                                             ('province3', 2000, date.fromisoformat('2022-05-04')))
        assert list(rs) == [test_rows[2]]

        # test parameterized query in batch mode case 5
        rs = self.connection.exec_driver_sql("select * from {} where col3 = ? and col6 < ?;".format(table),
                                             ('province3', datetime.fromtimestamp(1590738997.000)))
        assert list(rs) == [test_rows[2]]


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", os.path.abspath(__file__)]))
