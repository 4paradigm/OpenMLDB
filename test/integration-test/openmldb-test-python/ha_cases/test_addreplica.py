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

import os
import pytest
import sys
import random
import time
cur_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(f"{cur_path}/../util")
from cluster_manager import ClusterManager
from tool import Executor
from tool import Status
import openmldb.dbapi

class TestAddReplica:
    manager = None
    db = None
    cursor = None
    conf = None

    @classmethod
    def setup_class(cls):
        cls.manager = ClusterManager(f"{cur_path}/../openmldb/conf/hosts")
        cls.conf = cls.manager.GetConf()
        cls.db = openmldb.dbapi.connect(zk=cls.conf["zk_cluster"], zkPath=cls.conf["zk_root_path"])
        cls.cursor = cls.db.cursor()
        cls.executor = Executor(cls.manager.GetBinPath(), cls.conf["zk_cluster"], cls.conf["zk_root_path"])

    @pytest.mark.parametrize("storage_mode, snapshot", [("memory", True), ("hdd", True), ("memory", False), ("hdd", False)])
    def test_add_replica(self, storage_mode, snapshot):
        database = "test"
        tablets = self.manager.GetComponents("tablet")
        self.cursor.execute(f"create database if not exists {database}")
        self.cursor.execute(f"use {database}")
        table_name = "table" + str(random.randint(0, 10000))
        ddl = f"create table if not exists {table_name} (col1 string, col2 string) OPTIONS (storage_mode='{storage_mode}', DISTRIBUTION=[('{tablets[0]}')]);"
        self.cursor.execute(ddl)
        status, table_info = self.executor.GetTableInfo(database, table_name)
        assert status.OK()
        assert len(table_info) == 1
        key_num = 100
        partition_num = 1
        for i in range(key_num):
            key = "key" + str(i)
            self.cursor.execute(f"insert into {table_name} values (\'{key}\', \'col2\')");
        if snapshot:
            for pid in range(partition_num):
                self.executor.MakeSnashot(database, table_name, pid)
            assert self.executor.WaitingTableOP(database, table_name, partition_num).OK()
            for i in range(key_num):
                key = "key" + str(i)
                self.cursor.execute(f"insert into {table_name} values (\'{key}\', \'col2\')");
        result = self.cursor.execute(f"select * from {table_name}")
        data = result.fetchall()
        assert len(data) == key_num * 2 if snapshot else key_num

        assert self.executor.AddReplica(database, table_name, 0, tablets[1], True).OK()
        status, table_info = self.executor.GetTableInfo(database, table_name)
        assert status.OK()
        assert len(table_info) == 2
        check_cnt = 10
        while check_cnt > 0:
            if table_info[0][6] != '-' and table_info[1][6] != '-':
                break
            time.sleep(1)
            status, table_info = self.executor.GetTableInfo(database, table_name)
            assert status.OK()
            check_cnt -= 1
        # assert offset
        assert table_info[0][6] == table_info[1][6]

        self.cursor.execute(f"drop table {table_name}")

    @pytest.mark.parametrize("storage_mode, snapshot", [("memory", True), ("hdd", True), ("memory", False), ("hdd", False)])
    def test_del_replica(self, storage_mode, snapshot):
        database = "test"
        tablets = self.manager.GetComponents("tablet")
        self.cursor.execute(f"create database if not exists {database}")
        self.cursor.execute(f"use {database}")
        table_name = "table" + str(random.randint(0, 10000))
        ddl = f"create table if not exists {table_name} (col1 string, col2 string) OPTIONS (storage_mode='{storage_mode}', DISTRIBUTION=[('{tablets[0]}', ['{tablets[1]}'])]);"
        self.cursor.execute(ddl)
        status, table_info = self.executor.GetTableInfo(database, table_name)
        assert status.OK()
        assert len(table_info) == 2
        key_num = 100
        partition_num = 1
        for i in range(key_num):
            key = "key" + str(i)
            self.cursor.execute(f"insert into {table_name} values (\'{key}\', \'col2\')");
        if snapshot:
            for pid in range(partition_num):
                self.executor.MakeSnashot(database, table_name, pid)
            assert self.executor.WaitingTableOP(database, table_name, partition_num).OK()
            for i in range(key_num):
                key = "key" + str(i)
                self.cursor.execute(f"insert into {table_name} values (\'{key}\', \'col2\')");
        result = self.cursor.execute(f"select * from {table_name}")
        data = result.fetchall()
        assert len(data) == key_num * 2 if snapshot else key_num

        assert self.executor.DelReplica(database, table_name, 0, tablets[1], True).OK()
        status, table_info = self.executor.GetTableInfo(database, table_name)
        assert status.OK()
        assert len(table_info) == 1

        self.cursor.execute(f"drop table {table_name}")
