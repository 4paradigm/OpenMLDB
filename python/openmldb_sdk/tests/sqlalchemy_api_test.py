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
import pytest
# fmt: off
import openmldb
import sqlalchemy as db
from sqlalchemy import Table, Column, Integer, String, MetaData
from sqlalchemy.sql import select
from sqlalchemy.exc import DatabaseError
# fmt:on

from .case_conf import OpenMLDB_ZK_CLUSTER, OpenMLDB_ZK_PATH

logging.basicConfig(level=logging.WARNING)


class TestSqlalchemyAPI:

    def setup_class(self):
        self.engine = db.create_engine(
            'openmldb:///db_test?zk={}&zkPath={}'.format(
                OpenMLDB_ZK_CLUSTER, OpenMLDB_ZK_PATH))
        self.connection = self.engine.connect()
        self.connection.execute('create database if not exists db_test')
        self.metadata = MetaData()
        self.test_table = Table('test_table', self.metadata,
                                Column('x', String), Column('y', Integer))
        self.metadata.create_all(self.engine)

    def test_create_table(self):
        assert self.connection.dialect.has_table(self.connection, 'test_table')

    def test_insert(self):
        self.connection.execute(self.test_table.insert().values(x='first',
                                                                y=100))

    def test_select(self):
        for row in self.connection.execute(select([self.test_table])):
            assert 'first' in list(row)
            assert 100 in list(row)

    def test_request_timeout(self):
        self.connection.execute(
            "insert into test_table (y, x) values(400, 'a'),(401,'b'),(402, 'c');"
        )

        engine = db.create_engine(
            'openmldb:///db_test?zk={}&zkPath={}&requestTimeout=0'.format(
                OpenMLDB_ZK_CLUSTER, OpenMLDB_ZK_PATH))
        connection = engine.connect()

        with pytest.raises(DatabaseError) as e:
            connection.execute(
                "select * from test_table where x='b'").fetchall()
        assert 'select fail' in str(e.value)

    def test_zk_log(self):
        # disable zk log
        engine = db.create_engine(
            'openmldb:///db_test?zk={}&zkPath={}&zkLogLevel=0'.format(
                OpenMLDB_ZK_CLUSTER, OpenMLDB_ZK_PATH))
        connection = engine.connect()
        connection.execute("select 1;")

        # redirect to /tmp/test_openmldb_zk.log, may core dump when client close
        # engine = db.create_engine(
        #     'openmldb:///db_test?zk={}&zkPath={}&zkLogFile=/tmp/test_openmldb_zk.log'.format(
        #         OpenMLDB_ZK_CLUSTER, OpenMLDB_ZK_PATH))
        # connection = engine.connect()
        # connection.execute("select 1;")

    def teardown_class(self):
        self.connection.execute("drop table test_table;")
        self.connection.close()


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", os.path.abspath(__file__)]))
