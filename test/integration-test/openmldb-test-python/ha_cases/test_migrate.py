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
# limitations under the License

import pytest
import sys
import random
import time
sys.path.append("../util")
from cluster_manager import ClusterManager
from tool import Executor
from tool import Status
import openmldb.dbapi

class TestMigrate:
    manager = None
    db = None
    cursor = None
    conf = None

    @classmethod
    def setup_class(cls):
        cls.manager = ClusterManager("../openmldb/conf/hosts")
        cls.conf = cls.manager.GetConf()
        cls.db = openmldb.dbapi.connect(zk=cls.conf["zk_cluster"], zkPath=cls.conf["zk_root_path"])
        cls.cursor = cls.db.cursor()
        cls.executor = Executor(cls.manager.GetBinPath(), cls.conf["zk_cluster"], cls.conf["zk_root_path"])

    @pytest.mark.parametrize("storage_mode, snapshot", [("memory", True), ("hdd", True), ("memory", False), ("hdd", False)])
    def test_migrate(self, storage_mode, snapshot):
        database = "test"
        tablets = self.manager.GetComponents("tablet")
        self.cursor.execute(f"create database if not exists {database}")
        self.cursor.execute(f"use {database}")
        table_name = "table" + str(random.randint(0, 10000))
        partition_num = 1
        ddl = f"create table if not exists {table_name} (col1 string, col2 string) OPTIONS (storage_mode='{storage_mode}', DISTRIBUTION=[('{tablets[0]}', ['{tablets[1]}'])]);"
        self.cursor.execute(ddl)
        key_num = 100
        for i in range(key_num):
            key = "key" + str(i)
            self.cursor.execute(f"insert into {table_name} values (\'{key}\', \'col2\')");
        result = self.cursor.execute(f"select * from {table_name}")
        data = result.fetchall()
        assert len(data) == key_num
        if snapshot:
            for pid in range(partition_num):
                self.executor.MakeSnashot(database, table_name, pid)
            assert self.executor.WaitingTableOP(database, table_name, partition_num).OK()
            for i in range(key_num):
                key = "key" + str(i)
                self.cursor.execute(f"insert into {table_name} values (\'{key}\', \'col2\')");

        assert self.executor.SetAutofailover("false").OK()
        assert self.executor.Migrate(database, table_name, 0, tablets[1], tablets[2], True).OK()
        assert self.executor.SetAutofailover("true").OK()

        result = self.cursor.execute(f"select * from {table_name}")
        data = result.fetchall()
        assert len(data) == key_num * 2 if snapshot else key_num
        status, table_info = self.executor.GetTableInfo(database, table_name)
        assert status.OK()
        assert len(table_info) == 2
        assert tablets[0] in [table_info[0][3], table_info[1][3]]
        assert tablets[2] in [table_info[0][3], table_info[1][3]]

        self.cursor.execute(f"drop table {table_name}")
