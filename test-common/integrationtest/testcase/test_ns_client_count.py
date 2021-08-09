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

# -*- coding: utf-8 -*-
from testcasebase import TestCaseBase
import threading
import time
from libs.deco import multi_dimension
from libs.logger import infoLogger
import libs.ddt as ddt
from libs.test_loader import load
import libs.utils as utils


@ddt.ddt
class TestCount(TestCaseBase):

    def test_count_kv(self):
        """
        kv表统计pk下的条数
        :return:
        """
        name = 'tname{}'.format(time.time())
        rs = self.ns_create_cmd(self.ns_leader, name, '0', str(8), str(3), '')
        self.assertIn('Create table ok', rs)

        rs1 = self.ns_put_kv(self.ns_leader, name, 'testkey0', '11', 'testvalue0')
        self.assertIn('Put ok', rs1)
        rs2 = self.ns_put_kv(self.ns_leader, name, 'testkey0', '22', 'testvalue1')
        self.assertIn('Put ok', rs2)
        rs3 = self.ns_put_kv(self.ns_leader, name, 'testkey1', '33', 'testvalue2')
        self.assertIn('Put ok', rs3)

        rs4 = self.ns_count(self.ns_leader, name, 'testkey0', '')
        self.assertIn('count: 2', rs4)
        rs5 = self.ns_count(self.ns_leader, name, 'testkey1', '')
        self.assertIn('count: 1', rs5)
        rs6 = self.ns_count(self.ns_leader, name, 'testkeyx', '')
        self.assertIn('count: 0', rs6)
        rs7 = self.ns_count(self.ns_leader, name, 'testkey0', '', 'true')
        self.assertIn('count: 2', rs7)
        rs8 = self.ns_count(self.ns_leader, name, 'testkeyx', '', 'true')
        self.assertIn('count: 0', rs8)

    def test_count_kv_latest(self):
        """
        kv表统计pk下的条数
        :return:
        """
        name = 'tname{}'.format(time.time())
        rs = self.ns_create_cmd(self.ns_leader, name, 'latest:2', str(8), str(3), '')
        self.assertIn('Create table ok', rs)

        rs1 = self.ns_put_kv(self.ns_leader, name, 'testkey0', '11', 'testvalue0')
        self.assertIn('Put ok', rs1)
        rs2 = self.ns_put_kv(self.ns_leader, name, 'testkey0', '22', 'testvalue1')
        self.assertIn('Put ok', rs2)
        rs3 = self.ns_put_kv(self.ns_leader, name, 'testkey0', '33', 'testvalue2')
        self.assertIn('Put ok', rs3)
        rs4 = self.ns_put_kv(self.ns_leader, name, 'testkey1', '44', 'testvalue3')
        self.assertIn('Put ok', rs4)

        rs5 = self.ns_count(self.ns_leader, name, 'testkey0', '')
        self.assertIn('count: 3', rs5)
        rs6 = self.ns_count(self.ns_leader, name, 'testkey0', '', 'true')
        self.assertIn('count: 2', rs6)

    def test_count_schema(self):
        """
        schema表统计pk下的条数
        :return:
        """
        name = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        m = utils.gen_table_metadata(
            '"{}"'.format(name), '"kAbsoluteTime"', 0, 8,
            ('table_partition', '"{}"'.format(self.leader), '"0-2"', 'true'),
            ('table_partition', '"{}"'.format(self.slave1), '"0-1"', 'false'),
            ('table_partition', '"{}"'.format(self.slave2), '"1-2"', 'false'),
            ('column_desc', '"k1"', '"string"', 'true'),
            ('column_desc', '"k2"', '"string"', 'true'),
            ('column_desc', '"k3"', '"uint16"', 'false'),
        )
        utils.gen_table_metadata_file(m, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs)

        rs1 = self.ns_put_multi(self.ns_leader, name, 11, ['card0', 'mcc0', '15'])
        self.assertIn('Put ok', rs1)
        rs2 = self.ns_put_multi(self.ns_leader, name, 22, ['card0', 'mcc1', '16'])
        self.assertIn('Put ok', rs2)
        rs3 = self.ns_put_multi(self.ns_leader, name, 33, ['card1', 'mcc2', '20'])
        self.assertIn('Put ok', rs3)

        rs4 = self.ns_count(self.ns_leader, name, 'card0', 'k1')
        self.assertIn('count: 2', rs4)
        rs5 = self.ns_count(self.ns_leader, name, 'mcc1', 'k2')
        self.assertIn('count: 1', rs5)
        rs6 = self.ns_count(self.ns_leader, name, 'mcc1', 'k1')
        self.assertIn('count: 0', rs6)
        rs7 = self.ns_count(self.ns_leader, name, 'mcc1', 'card')
        self.assertIn('idx name not found', rs7)

if __name__ == "__main__":
    load(TestCount)

