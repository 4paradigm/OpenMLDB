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

import sqlalchemy as db
from nb_log import LogManager
import openmldb

log = LogManager('fedb-sdk-test').get_logger_and_add_handlers()


class FedbClient:

    def __init__(self, zkCluster, zkRootPath, dbName='test_fedb'):
        self.zkCluster = zkCluster
        self.zkRootPath = zkRootPath
        self.dbName = dbName

    def getConnect(self):
        # engine = db.create_engine('openmldb://@/{}?zk={}&zkPath={}'.format(self.dbName, self.zkCluster, self.zkRootPath))
        # connect = engine.connect()
        # return connect

        db = openmldb.dbapi.connect(database=self.dbName, zk=self.zkCluster, zkPath=self.zkRootPath)
        cursor = db.cursor()
        return cursor
