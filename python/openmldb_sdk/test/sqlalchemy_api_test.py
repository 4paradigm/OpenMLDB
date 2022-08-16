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
# fmt:on

from case_conf import OpenMLDB_ZK_CLUSTER, OpenMLDB_ZK_PATH

logging.basicConfig(level=logging.WARNING)


class TestSqlalchemyAPI:

    def setup_class(self):
        self.engine = db.create_engine(
            'openmldb:///db_test?zk={}&zkPath={}'.format(OpenMLDB_ZK_CLUSTER, OpenMLDB_ZK_PATH))
        self.connection = self.engine.connect()
        self.metadata = MetaData()
        self.test_table = Table('test_table', self.metadata,
                                Column('x', String),
                                Column('y', Integer))
        self.metadata.create_all(self.engine)

    def test_create_table(self):
        assert self.connection.dialect.has_table(self.connection, 'test_table')

    def test_insert(self):
        self.connection.execute(self.test_table.insert().values(x='first', y=100))

    def test_select(self):
        for row in self.connection.execute(select([self.test_table])):
            assert 'first' in list(row)
            assert 100 in list(row)

    def teardown_class(self):
        self.connection.execute("drop table test_table;")
        self.connection.close()


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", os.path.abspath(__file__)]))
