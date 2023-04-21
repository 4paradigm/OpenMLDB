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

class TestMakesnapshot:
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

    def parse_manifest(self, file_path: str) -> (Status, dict):
        if not os.path.exists(file_path):
            return Status(-1, "file is not exist"), None
        result = {}
        with open(file_path, 'r') as f:
            line = f.readline()
            while line:
                arr = line.strip().split(":")
                result[arr[0]] = arr[1].strip()
                if arr[0] == "name":
                    result["name"] = result["name"][1:-1]
                line = f.readline()
        return Status(), result

    @pytest.mark.parametrize("storage_mode", ["memory", "hdd"])
    def test_makesnapshot(self, storage_mode):
        database = "test"
        tablets = self.manager.GetComponents("tablet")
        self.cursor.execute(f"create database if not exists {database}")
        self.cursor.execute(f"use {database}")
        table_name = "table" + str(random.randint(0, 10000))
        partition_num = 1
        ddl = f"create table if not exists {table_name} (col1 string, col2 string) OPTIONS (storage_mode='{storage_mode}', DISTRIBUTION=[('{tablets[0]}')]);"
        self.cursor.execute(ddl)
        status, table_info = self.executor.GetTableInfo(database, table_name)
        assert status.OK()
        assert len(table_info) == 1
        tid = table_info[0][1]
        key_num = 100
        for i in range(key_num):
            key = "key" + str(i)
            self.cursor.execute(f"insert into {table_name} values (\'{key}\', \'col2\')");
        result = self.cursor.execute(f"select * from {table_name}")
        data = result.fetchall()
        assert len(data) == key_num
        self.executor.MakeSnashot(database, table_name, 0)
        assert self.executor.WaitingTableOP(database, table_name, partition_num).OK()
        db_root_dir = "db" if storage_mode == "memory" else "db_hdd"
        snapshot_path = self.conf["tablet"][tablets[0]] + f"/{db_root_dir}/{tid}_0/snapshot"
        status, manifest = self.parse_manifest(snapshot_path + "/MANIFEST")
        assert status.OK()
        print(manifest)
        assert int(manifest["offset"]) == key_num
        assert int(manifest["count"]) == key_num
        snapshot_file = snapshot_path + "/"+ manifest["name"]
        assert os.path.exists(snapshot_file)

        for i in range(key_num):
            key = "key" + str(i)
            self.cursor.execute(f"insert into {table_name} values (\'{key}\', \'col2\')");
        result = self.cursor.execute(f"select * from {table_name}")
        data = result.fetchall()
        assert len(data) == key_num * 2
        if storage_mode == "hdd":
            time.sleep(60)
        self.executor.MakeSnashot(database, table_name, 0)
        assert self.executor.WaitingTableOP(database, table_name, partition_num).OK()
        status, manifest = self.parse_manifest(snapshot_path + "/MANIFEST")
        assert status.OK()
        assert int(manifest["offset"]) == key_num * 2
        assert int(manifest["count"]) == key_num * 2
        new_snapshot_file = snapshot_path + "/"+ manifest["name"]
        assert os.path.exists(new_snapshot_file)

        self.cursor.execute(f"drop table {table_name}")
