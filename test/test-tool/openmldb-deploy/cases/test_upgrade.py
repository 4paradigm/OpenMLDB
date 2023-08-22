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
from tool import Util

class TestUpgrade:
    db = None
    cursor = None

    @classmethod
    def setup_class(cls):
        cls.db = openmldb.dbapi.connect(zk=case_conf.conf["zk_cluster"], zkPath=case_conf.conf["zk_root_path"])
        cls.cursor = cls.db.cursor()
        cls.base_dir = case_conf.conf["base_dir"]
        cls.openmldb_path = cls.base_dir + "/openmldb"
        cls.tool_path = cls.base_dir + "/tools"
        cls.bin_path = cls.openmldb_path + "/bin/openmldb"
        cls.executor = Executor(cls.bin_path, case_conf.conf["zk_cluster"], case_conf.conf["zk_root_path"])

    def execute_upgrade(self, python_bin, upgrade_cmd : str, endpoint : str):
        cmd = [python_bin]
        cmd.append(f"{self.base_dir}/tools/openmldb_ops.py")
        cmd.append(f"--openmldb_bin_path={self.bin_path}")
        cmd.append("--zk_cluster=" + case_conf.conf["zk_cluster"])
        cmd.append("--zk_root_path=" + case_conf.conf["zk_root_path"])
        cmd.append(f"--cmd={upgrade_cmd}")
        cmd.append(f"--endpoint={endpoint}")
        cmd.append(f"--statfile={self.base_dir}/.stat")
        status, output = self.executor.RunWithRetuncode(cmd)
        return status

    def get_leader_cnt(self, db, table_name, endpoint) -> (Status, int):
        status, result = self.executor.GetTableInfo(db, table_name)
        if not status.OK():
            return status, 0
        leader_cnt = 0
        for item in result:
            if item[3] == endpoint and item[4] == "leader":
                leader_cnt += 1
        return Status(), leader_cnt

    def get_unalive_cnt(self, db, table_name) -> (Status, int):
        status, result = self.executor.GetTableInfo(db, table_name)
        if not status.OK():
            return status, 0
        unalive_cnt = 0
        for item in result:
            if item[5] == "no":
                unalive_cnt += 1
        return Status(), unalive_cnt


    @pytest.mark.parametrize("replica_num, partition_num", [(3, 10), (2, 10), (1, 10)])
    @pytest.mark.parametrize("python_bin", ["python2", "python3"])
    def test_upgrade(self, python_bin, replica_num, partition_num):
        assert len(case_conf.conf["components"]["tablet"]) > 2
        status, distribution = Util.gen_distribution(case_conf.conf["components"]["tablet"], replica_num, partition_num)
        assert status.OK()
        self.cursor.execute("create database if not exists test")
        self.cursor.execute("use test")
        table_name = "table" + str(random.randint(0, 10000))
        self.cursor.execute(f"create table if not exists {table_name} (col1 string, col2 string) options (DISTRIBUTION={distribution});")
        key_num = 50000
        for i in range(key_num):
            key = "key" + str(i)
            self.cursor.execute(f"insert into {table_name} values (\'{key}\', \'col2\');")
        result = self.cursor.execute(f"select * from {table_name}")
        data = result.fetchall()
        assert len(data) == key_num

        status, cnt = self.get_leader_cnt("test", table_name, case_conf.conf["components"]["tablet"][0])
        assert status.OK() 
        status, unalive_cnt = self.get_unalive_cnt("test", table_name)
        assert status.OK() and unalive_cnt == 0

        assert self.execute_upgrade(python_bin, "pre-upgrade", case_conf.conf["components"]["tablet"][0]).OK()

        status, cnt1 = self.get_leader_cnt("test", table_name, case_conf.conf["components"]["tablet"][0])
        assert status.OK() and cnt1 == 0
        result = self.cursor.execute(f"select * from {table_name}")
        status, unalive_cnt = self.get_unalive_cnt("test", table_name)
        assert status.OK() and unalive_cnt == 0
        data = result.fetchall()
        assert len(data) == key_num

        assert self.execute_upgrade(python_bin, "post-upgrade", case_conf.conf["components"]["tablet"][0]).OK()

        status, cnt2 = self.get_leader_cnt("test", table_name, case_conf.conf["components"]["tablet"][0])
        assert status.OK() and cnt2 == cnt
        status, unalive_cnt = self.get_unalive_cnt("test", table_name)
        assert status.OK() and unalive_cnt == 0
        result = self.cursor.execute(f"select * from {table_name}")
        data = result.fetchall()
        assert len(data) == key_num

        self.cursor.execute(f"drop table {table_name}")

if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", os.path.abspath(__file__)]))
