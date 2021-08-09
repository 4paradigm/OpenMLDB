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
class TestDelete(TestCaseBase):

    def test_kv_delete(self):
        """
        kv表delete pk
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
        rs4 = self.ns_scan_kv(self.ns_leader, name, 'testkey0', '1111', '0')
        self.assertEqual(2, len(rs4))
        rs5 = self.ns_delete(self.ns_leader, name, 'testkey0')
        self.assertIn('delete ok', rs5)
        rs6 = self.ns_scan_kv(self.ns_leader, name, 'testkey0', '1111', '0')
        self.assertEqual(0, len(rs6))


    def test_schema_delete(self):
        """
        schema表 delete pk
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
        rs4 = self.ns_scan_kv(self.ns_leader, name, 'card0', 'k1', '1111', '0')
        self.assertEqual(2, len(rs4))
        rs5 = self.ns_delete(self.ns_leader, name, 'card0', 'k1')
        self.assertIn('delete ok', rs5)
        rs6 = self.ns_scan_kv(self.ns_leader, name, 'card0', 'k1', '1111', '0')
        self.assertEqual(0, len(rs6))

    def test_delete_column_key(self):
        name = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
                "name": name,
                "ttl": 14400,
                "column_desc":[
                    {"name": "card", "type": "string", "add_ts_idx": "false"},
                    {"name": "mcc", "type": "string", "add_ts_idx": "false"},
                    {"name": "amt", "type": "double", "add_ts_idx": "false"},
                    {"name": "col1", "type": "string", "add_ts_idx": "false"},
                    {"name": "col2", "type": "string", "add_ts_idx": "false"},
                    ],
                "column_key":[
                    {"index_name":"card_x", "col_name":"card"},
                    {"index_name":"col", "col_name":["col1", "col2"]}
                    ]
                }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs)
        (schema, column_key) = self.ns_showschema(self.ns_leader, name)
        self.assertEqual(len(schema), 5)
        self.assertEqual(schema[0], ['0', 'card', 'string'])
        self.assertEqual(schema[2], ['2', 'amt', 'double'])
        self.assertEqual(len(column_key), 2)
        self.assertEqual(column_key[0], ["0", "card_x", "card", "-", "14400min"])
        self.assertEqual(column_key[1], ["1", "col", "col1|col2", "-", "14400min"])
        row = ['card0', 'mcc0', '1.3', 'col1x', 'col2x']
        self.ns_put_multi(self.ns_leader, name, self.now(), row)
        row = ['card0', 'mcc1', '1.4', 'col1m', 'col2m']
        self.ns_put_multi(self.ns_leader, name, self.now(), row)
        row = ['card1', 'mcc1', '1.5', 'col11', 'col22']
        self.ns_put_multi(self.ns_leader, name, self.now(), row)
        rs = self.ns_scan_multi(self.ns_leader, name, 'card0', 'card_x', self.now() + 100, 0)
        self.assertEqual(len(rs), 2)
        rs5 = self.ns_delete(self.ns_leader, name, 'card0', 'card_x')
        self.assertIn('delete ok', rs5)
        rs = self.ns_scan_multi(self.ns_leader, name, 'card0', 'card_x', self.now() + 100, 0)
        self.assertEqual(len(rs), 0)
        rs = self.ns_scan_multi(self.ns_leader, name, 'col1m|col2m', 'col', self.now() + 100, 0)
        self.assertEqual(len(rs), 1)
        rs5 = self.ns_delete(self.ns_leader, name, 'col1m|col2m', 'col')
        self.assertIn('delete ok', rs5)
        rs = self.ns_scan_multi(self.ns_leader, name, 'col1m|col2m', 'col', self.now() + 100, 0)
        self.assertEqual(len(rs), 0)

if __name__ == "__main__":
    load(TestDelete)

