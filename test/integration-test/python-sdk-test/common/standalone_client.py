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
import sqlalchemy as db
from nb_log import LogManager
from common import standalone_config

log = LogManager('fedb-sdk-test').get_logger_and_add_handlers()


class StandaloneClient:

    def __init__(self, host, port, dbName="db1"):
        self.host = host
        self.port = port
        self.dbName = dbName

    def getConnect(self):
        engine = db.create_engine('openmldb:///{}?host={}&port={}'.format(self.dbName, self.host, self.port))
        connect = engine.connect()
        return connect

        # db = openmldb.dbapi.connect(self.dbName, self.host, int(self.port))
        # cursor = db.cursor()
        # return cursor

if __name__ == "__main__":
    s = StandaloneClient(standalone_config.host,standalone_config.port,standalone_config.default_db_name)
    cursor = s.getConnect()
    rs = cursor.execute("select db3.auto_avelWUr0.c1,db3.auto_avelWUr0.c2,db4.auto_jF8Dp3W1.c3,db4.auto_jF8Dp3W1.c4 from db3.auto_avelWUr0 last join db4.auto_jF8Dp3W1 ORDER BY db4.auto_jF8Dp3W1.c3 on db3.auto_avelWUr0.c1=db4.auto_jF8Dp3W1.c1")
    print(rs.fetchall())
