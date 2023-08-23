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
from util import Util

class TestScale:
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

    def execute_scale(self, python_bin, scale_cmd : str, endpoint : str = ""):
        cmd = [python_bin]
        cmd.append(f"{self.base_dir}/tools/openmldb_ops.py")
        cmd.append(f"--openmldb_bin_path={self.bin_path}")
        cmd.append("--zk_cluster=" + case_conf.conf["zk_cluster"])
        cmd.append("--zk_root_path=" + case_conf.conf["zk_root_path"])
        cmd.append(f"--cmd={scale_cmd}")
        if scale_cmd == "scalein":
            cmd.append(f"--endpoint={endpoint}")
        status, output = self.executor.RunWithRetuncode(cmd)
        return status

    @pytest.mark.parametrize("replica_num, partition_num", [(2, 8), (1, 10)])
    @pytest.mark.parametrize("python_bin", ["python2", "python3"])
    def test_scalein(self, python_bin, replica_num, partition_num):
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
            self.cursor.execute(f"insert into {table_name} values (\'{key}\', \'col2\')");
        result = self.cursor.execute(f"select * from {table_name}")
        data = result.fetchall()
        assert len(data) == key_num

        status, result = self.executor.GetTableInfo("test", "")
        assert status.OK()
        endpoints = set()
        for item in result:
            endpoints.add(item[3])
            assert item[5] == "yes"
        assert len(endpoints) == len(case_conf.conf["components"]["tablet"])

        assert self.execute_scale(python_bin, "scalein", case_conf.conf["components"]["tablet"][0]).OK()

        status, result = self.executor.GetTableInfo("test", "")
        assert status.OK()
        endpoints = set()
        for item in result:
            endpoints.add(item[3])
            assert item[5] == "yes"
        assert len(endpoints) == len(case_conf.conf["components"]["tablet"]) - 1
        assert case_conf.conf["components"]["tablet"][0] not in endpoints

        self.cursor.execute(f"drop table {table_name}")

    @pytest.mark.parametrize("replica_num, partition_num", [(2, 8), (1, 10)])
    @pytest.mark.parametrize("python_bin", ["python2", "python3"])
    def test_scaleout(self, python_bin, replica_num, partition_num):
        assert len(case_conf.conf["components"]["tablet"]) > 2
        status, distribution = Util.gen_distribution(case_conf.conf["components"]["tablet"][1:], replica_num, partition_num)
        assert status.OK()
        self.cursor.execute("create database if not exists test")
        self.cursor.execute("use test")
        table_name = "table" + str(random.randint(0, 10000))
        self.cursor.execute(f"create table if not exists {table_name} (col1 string, col2 string) options (DISTRIBUTION={distribution});")
        key_num = 50000
        for i in range(key_num):
            key = "key" + str(i)
            self.cursor.execute(f"insert into {table_name} values (\'{key}\', \'col2\')");
        result = self.cursor.execute(f"select * from {table_name}")
        data = result.fetchall()
        assert len(data) == key_num

        status, result = self.executor.GetTableInfo("test", "")
        assert status.OK()
        endpoints = set()
        for item in result:
            endpoints.add(item[3])
            assert item[5] == "yes"
        assert len(endpoints) == len(case_conf.conf["components"]["tablet"]) - 1
        assert case_conf.conf["components"]["tablet"][0] not in endpoints

        assert self.execute_scale(python_bin, "scaleout").OK()

        status, result = self.executor.GetTableInfo("test", "")
        assert status.OK()
        endpoints = set()
        for item in result:
            endpoints.add(item[3])
            assert item[5] == "yes"
        assert len(endpoints) == len(case_conf.conf["components"]["tablet"])

        self.cursor.execute(f"drop table {table_name}")

if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", os.path.abspath(__file__)]))
