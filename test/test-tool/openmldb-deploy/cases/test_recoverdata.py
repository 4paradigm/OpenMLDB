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
import case_conf
import openmldb.dbapi
import random
from tool import Executor
from tool import Partition
from tool import Status

class TestRecoverData:
    db = None
    cursor = None

    @classmethod
    def setup_class(cls):
        cls.db = openmldb.dbapi.connect(zk=case_conf.conf["zk_cluster"], zkPath=case_conf.conf["zk_root_path"])
        cls.cursor = cls.db.cursor()
        cls.base_dir = case_conf.conf["base_dir"]
        cls.openmldb_path = cls.base_dir + "/openmldb"
        cls.tool_path = cls.base_dir + "/tools"
        cls.bin_path = cls.base_dir + "/bin/openmldb"
        cls.executor = Executor(cls.bin_path, case_conf.conf["zk_cluster"], case_conf.conf["zk_root_path"])

    def start_openmldb(self):
        cmd = ["bash"]
        cmd.append(f"{self.openmldb_path}/sbin/start-all.sh")
        status, output = self.executor.RunWithRetuncode(cmd)
        return status

    def stop_openmldb(self):
        cmd = ["bash"]
        cmd.append(f"{self.openmldb_path}/sbin/stop-all.sh")
        status, output = self.executor.RunWithRetuncode(cmd)
        return status

    def test_recoverdata(self):
        self.cursor.execute("create database if not exists test")
        self.cursor.execute("use test")
        table_name = "table" + str(random.randint(0, 10000))
        self.cursor.execute(f"create table if not exists {table_name} (col1 string, col2 string);")
        key_num = 50000
        for i in range(key_num):
            key = "key" + str(i)
            self.cursor.execute(f"insert into {table_name} values (\'{key}\', \'col2\')");
        result = self.cursor.execute(f"select * from {table_name}")
        data = result.fetchall()
        assert len(data) == key_num
        assert self.stop_openmldb().OK()
        # will call recover data in start-all.sh
        assert self.start_openmldb().OK()
        result = self.cursor.execute(f"select * from {table_name}")
        data = result.fetchall()
        assert len(data) == key_num
        data = self.cursor.execute("show table status").fetchall()
        assert len(data) > 0
        for item in data:
            # Partition_unalive num
            assert int(item[8]) == 0
        
        self.cursor.execute(f"drop table {table_name}")

if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", os.path.abspath(__file__)]))
