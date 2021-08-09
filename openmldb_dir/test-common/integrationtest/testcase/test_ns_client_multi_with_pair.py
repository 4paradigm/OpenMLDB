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
@multi_dimension(False)
class TestHasTsCol(TestCaseBase):

    def test_count_schema_has_ts_col(self):
        """
        指定时间列的schema表统计pk下的条数
        :return:
        """
        name = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
                "name": name,
                "ttl": 0,
               "column_desc":[
                   {"name": "card", "type": "string", "add_ts_idx": "true"},
                   {"name": "mcc", "type": "string", "add_ts_idx": "true"},
                   {"name": "amt", "type": "double", "add_ts_idx": "false"},
                   {"name": "ts1", "type": "int64", "add_ts_idx": "false", "is_ts_col": "true"},
                   {"name": "ts2", "type": "int64", "add_ts_idx": "false", "is_ts_col": "true"},
                   ],
               "column_key":[
                   {"index_name":"card", "ts_name":["ts1", "ts2"]},
                   {"index_name":"mcc", "ts_name":["ts2"]},
                   ]
               }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs)

        rs1 = self.ns_put_multi_with_pair(self.ns_leader,name, ['card0', 'mcc0', '1', '15', '18'])
        self.assertIn('Put ok', rs1)
        rs2 = self.ns_put_multi_with_pair(self.ns_leader, name, ['card0', 'mcc1', '2', '16', '19'])
        self.assertIn('Put ok', rs2)
        rs3 = self.ns_put_multi_with_pair(self.ns_leader, name, ['card1', 'mcc2', '3', '20', '20'])
        self.assertIn('Put ok', rs3)

        rs4 = self.ns_count_with_pair(self.ns_leader, name, 'card0', 'card', 'ts1')
        self.assertIn('count: 2', rs4)
        rs5 = self.ns_count_with_pair(self.ns_leader, name, 'card0', 'card', 'ts2')
        self.assertIn('count: 2', rs5)
        rs6 = self.ns_count_with_pair(self.ns_leader, name, 'card1', 'card', 'ts1')
        self.assertIn('count: 1', rs6)
        rs7 = self.ns_count_with_pair(self.ns_leader, name, 'card1', 'card', 'ts2')
        self.assertIn('count: 1', rs7)
        rs8 = self.ns_count_with_pair(self.ns_leader, name, 'mcc1', 'mcc', 'ts1')
        self.assertIn('idx name not found', rs8)
        rs9 = self.ns_count_with_pair(self.ns_leader, name, 'mcc1', 'card', 'ts1')
        self.assertIn('count: 0', rs9)
        rs10 = self.ns_count_with_pair(self.ns_leader, name, 'mcc1', 'k1', 'ts1')
        self.assertIn('idx name not found', rs10)
        rs11 = self.ns_count_with_pair(self.ns_leader, name, 'mcc1', 'mcc', 'ts3')
        self.assertIn('ts name not found', rs11)

    def test_scan_schema_has_ts_col(self):
        """
        指定时间列的schema表查询pk下的多条数据
        :return:
        """
        name = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
                "name": name,
                "ttl": 0,
               "column_desc":[
                   {"name": "card", "type": "string", "add_ts_idx": "true"},
                   {"name": "mcc", "type": "string", "add_ts_idx": "true"},
                   {"name": "amt", "type": "double", "add_ts_idx": "false"},
                   {"name": "ts1", "type": "int64", "add_ts_idx": "false", "is_ts_col": "true"},
                   {"name": "ts2", "type": "int64", "add_ts_idx": "false", "is_ts_col": "true"},
                   ],
               "column_key":[
                   {"index_name":"card", "ts_name":["ts1", "ts2"]},
                   {"index_name":"mcc", "ts_name":["ts2"]},
                   ]
               }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs)

        rs1 = self.ns_put_multi_with_pair(self.ns_leader,name, ['card0', 'mcc0', '1', '15', '18'])
        self.assertIn('Put ok', rs1)
        rs2 = self.ns_put_multi_with_pair(self.ns_leader, name, ['card0', 'mcc1', '2', '16', '19'])
        self.assertIn('Put ok', rs2)
        rs3 = self.ns_put_multi_with_pair(self.ns_leader, name, ['card1', 'mcc2', '3', '20', '20'])
        self.assertIn('Put ok', rs3)
        
        rs4 = self.ns_scan_multi_with_pair(self.ns_leader, name, 'card0', 'card',  '25', '0', 'ts1')
        self.assertEqual(len(rs4),2)
        self.assertEqual(rs4[0]['card'], 'card0')
        self.assertEqual(rs4[0]['mcc'], 'mcc1')
        self.assertEqual(rs4[0]['amt'], '2')
        self.assertEqual(rs4[0]['ts1'], '16')
        self.assertEqual(rs4[0]['ts2'], '19')
        self.assertEqual(rs4[1]['card'], 'card0')
        self.assertEqual(rs4[1]['mcc'], 'mcc0')
        self.assertEqual(rs4[1]['amt'], '1')
        self.assertEqual(rs4[1]['ts1'], '15')
        self.assertEqual(rs4[1]['ts2'], '18')
        rs5 = self.ns_scan_multi_with_pair(self.ns_leader, name, 'card0', 'card',  '25', '0', 'ts1', '1')
        self.assertEqual(len(rs5),1)
        self.assertEqual(rs5[0]['card'], 'card0')
        self.assertEqual(rs5[0]['mcc'], 'mcc1')
        self.assertEqual(rs5[0]['amt'], '2')
        self.assertEqual(rs5[0]['ts1'], '16')
        self.assertEqual(rs5[0]['ts2'], '19')
        rs6 = self.ns_scan_multi_with_pair(self.ns_leader, name, 'mcc1', 'mcc',  '25', '0', 'ts1', '1')
        self.assertEqual(len(rs6), 0)
        rs7 = self.ns_scan_multi_with_pair(self.ns_leader, name, 'mcc1', 'mcc',  '25', '0', 'ts2', '1')
        self.assertEqual(len(rs7), 1)

    def test_get_schema_has_ts_col(self):
        """
        指定时间列的schema表查询一条数据·
        :return:
        """
        name = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
                "name": name,
                "ttl": 0,
               "column_desc":[
                   {"name": "card", "type": "string", "add_ts_idx": "true"},
                   {"name": "mcc", "type": "string", "add_ts_idx": "true"},
                   {"name": "amt", "type": "double", "add_ts_idx": "false"},
                   {"name": "ts1", "type": "int64", "add_ts_idx": "false", "is_ts_col": "true"},
                   {"name": "ts2", "type": "int64", "add_ts_idx": "false", "is_ts_col": "true", "ttl": 0},
                   ],
               "column_key":[
                   {"index_name":"card", "ts_name":["ts1", "ts2"]},
                   {"index_name":"mcc", "ts_name":["ts2"]},
                   ]
               }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs)

        rs1 = self.ns_put_multi_with_pair(self.ns_leader,name, ['card0', 'mcc0', '1', '15', '18'])
        self.assertIn('Put ok', rs1)
        rs2 = self.ns_put_multi_with_pair(self.ns_leader, name, ['card0', 'mcc1', '2', '16', '19'])
        self.assertIn('Put ok', rs2)
        rs3 = self.ns_put_multi_with_pair(self.ns_leader, name, ['card1', 'mcc2', '3', '20', '20'])
        self.assertIn('Put ok', rs3)

        rs4 = self.ns_get_multi_with_pair(self.ns_leader, name, 'card0', 'card', '16', 'ts1')
        self.assertEqual(len(rs4),5)
        self.assertEqual(rs4['card'], 'card0')
        self.assertEqual(rs4['mcc'], 'mcc1')
        self.assertEqual(rs4['amt'], '2')
        self.assertEqual(rs4['ts1'], '16')
        self.assertEqual(rs4['ts2'], '19')
        rs5 = self.ns_get_multi_with_pair(self.ns_leader, name, 'card0', 'card', '18', 'ts2')
        self.assertEqual(len(rs5),5)
        self.assertEqual(rs5['card'], 'card0')
        self.assertEqual(rs5['mcc'], 'mcc0')
        self.assertEqual(rs5['amt'], '1')
        self.assertEqual(rs5['ts1'], '15')
        self.assertEqual(rs5['ts2'], '18')

        rs6 = self.ns_get_multi_with_pair(self.ns_leader, name, 'mcc1', 'mcc',  '0', 'ts1')
        self.assertEqual(len(rs6), 0)
        rs7 = self.ns_get_multi_with_pair(self.ns_leader, name, 'mcc1', 'mcc',  '0', 'ts2')
        self.assertEqual(len(rs7), 5)

    def test_scan_atleast(self):
        """
        指定时间列的schema表查询pk下的多条数据
        :return:
        """
        name = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
                "name": name,
                "ttl": 0,
               "column_desc":[
                   {"name": "card", "type": "string", "add_ts_idx": "true"},
                   {"name": "ts1", "type": "int64", "add_ts_idx": "false", "is_ts_col": "true"},
                   {"name": "ts2", "type": "int64", "add_ts_idx": "false", "is_ts_col": "true"},
                   {"name": "ts3", "type": "int64", "add_ts_idx": "false", "is_ts_col": "true"},
                   ],
                "column_key":[
                   {"index_name":"card", "ts_name":["ts1", "ts2", "ts3"]},
                   ]
               }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs)
        
        for i in range(100):
            rs = self.ns_put_multi_with_pair(self.ns_leader,name, ['card' + str(i%10), str(i+1), str(i+1), str(i+1)])

            self.assertIn('Put ok', rs)
        for i in range(10):
            prs = self.ns_scan_multi_with_pair(self.ns_leader, name, 'card'+str(i), 'card', '100', '0', 'ts1', '0', '8')
            self.assertEqual(len(prs), 10)
        for i in range(10):
            prs = self.ns_scan_multi_with_pair(self.ns_leader, name, 'card'+str(i), 'card', '100', '50', 'ts1', '0', '0')
            self.assertEqual(len(prs), 5)
        for i in range(10):
            prs = self.ns_scan_multi_with_pair(self.ns_leader, name, 'card'+str(i), 'card', '100', '50', 'ts1', '0', '8')
            self.assertEqual(len(prs), 8)
        for i in range(10):
            prs = self.ns_scan_multi_with_pair(self.ns_leader, name, 'card'+str(i), 'card', '50', '0', 'ts1', '0', '8')
            self.assertEqual(len(prs), 5)
        for i in range(10):
            prs = self.ns_scan_multi_with_pair(self.ns_leader, name, 'card'+str(i), 'card', '100', '10', 'ts1', '8', '3')
            self.assertEqual(len(prs), 8)
        prs = self.ns_scan_multi_with_pair(self.ns_leader, name, 'card'+str(i), 'card', '90', '0', 'ts1', '4', '5')
        self.assertEqual(len(prs), 0)

if __name__ == "__main__":
    load(TestHasTsCol)

