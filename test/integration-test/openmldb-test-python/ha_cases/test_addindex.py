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
import os
import time
cur_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(f"{cur_path}/../util")
from cluster_manager import ClusterManager
from tool import Executor
from tool import Status
import openmldb.dbapi

class TestAddIndex:
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

    @pytest.mark.parametrize("storage_mode, snapshot", [("memory", True), ("memory", False)])
    def test_addindex(self, storage_mode, snapshot):
        database = "test"
        tablets = self.manager.GetComponents("tablet")
        self.cursor.execute(f"create database if not exists {database}")
        self.cursor.execute(f"use {database}")
        table_name = "table" + str(random.randint(0, 10000))
        partition_num = 8
        ddl = f"create table if not exists {table_name} (col1 string, col2 string, col3 bigint, index(key=col1, ts=col3));"
        self.cursor.execute(ddl)
        status, indexs = self.executor.GetIndexs(database, table_name)
        assert status.OK() and len(indexs) == 1
        key_num = 1000
        ts = 1682510327000
        for i in range(key_num):
            key1 = "key1" + str(i)
            key2 = "key2" + str(i)
            self.cursor.execute(f"insert into {table_name} values (\'{key1}\', \'{key2}\', {ts})");
        result = self.cursor.execute(f"select * from {table_name}")
        data = result.fetchall()
        assert len(data) == key_num
        if snapshot:
            for pid in range(partition_num):
                self.executor.MakeSnashot(database, table_name, pid)
            assert self.executor.WaitingTableOP(database, table_name, partition_num).OK()
            ts += 1000
            for i in range(key_num):
                key1 = "key1" + str(i)
                key2 = "key2" + str(i)
                self.cursor.execute(f"insert into {table_name} values (\'{key1}\', \'{key2}\', {ts})");

        time.sleep(2)
        result = self.cursor.execute(f"create index index2 on {table_name} (col2) options (ts=col3)")
        time.sleep(1)
        assert self.executor.WaitingOP(database, table_name, 0).OK()
        status, indexs = self.executor.GetIndexs(database, table_name)
        assert status.OK() and len(indexs) == 2
        assert indexs[1].GetName() == "index2" and indexs[1].GetTsCol() == "col3"
        assert len(indexs[1].GetKeys()) == 1 and indexs[1].GetKeys()[0] == "col2"
        for i in range(key_num):
            key2 = "key2" + str(i)
            status, result = self.executor.Scan(database, table_name, key2, "index2", 0, 0)
            if snapshot:
                assert status.OK() and len(result) == 2 and result[0][0] == "key1" + str(i)
            else:
                assert status.OK() and len(result) == 1 and result[0][0] == "key1" + str(i)
        self.cursor.execute(f"drop table {table_name}")
