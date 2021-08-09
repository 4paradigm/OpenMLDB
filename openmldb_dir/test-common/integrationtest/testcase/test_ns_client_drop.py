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
import time
import os
from libs.test_loader import load
import libs.utils as utils
from libs.logger import infoLogger
from libs.deco import multi_dimension
import libs.ddt as ddt
import collections

@ddt.ddt
class TestNsDropTable(TestCaseBase):


    def test_ns_drop_table(self):
        """
        通过ns建的表，可以通过ns全部drop掉，drop后gettablestatus为空
        :return:
        """
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        name = 'tname{}'.format(time.time())
        m = utils.gen_table_metadata(
            '"{}"'.format(name), None, 144000, 2,
            ('table_partition', '"{}"'.format(self.leader), '"0-7"', 'true'),
            ('table_partition', '"{}"'.format(self.slave1), '"0-7"', 'false'),
            ('table_partition', '"{}"'.format(self.slave2), '"0-7"', 'false'),
            ('column_desc', '"k1"', '"string"', 'true'),
            ('column_desc', '"k2"', '"string"', 'false'),
            ('column_desc', '"k3"', '"string"', 'true'),
        )
        utils.gen_table_metadata_file(m, metadata_path)
        rs0 = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs0)

        rs1 = self.showtable(self.ns_leader, name)
        tid = rs1.keys()[0][1]
        rs3 = self.get_table_status(self.leader, tid, 0)
        infoLogger.info(rs3)
        self.assertNotEqual(rs3, None)
        self.ns_drop(self.ns_leader, name)
        time.sleep(2)
        rs2 = self.showtable(self.ns_leader, name)
        rs3 = self.get_table_status(self.leader, tid, 0)

        self.assertEqual(len(rs1), 24)
        self.assertEqual(rs2, {})
        self.assertEqual(rs3, None)
        


if __name__ == "__main__":
    load(TestNsDropTable)
