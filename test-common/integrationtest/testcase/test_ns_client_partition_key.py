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
import time
from testcasebase import TestCaseBase
from libs.deco import *
import libs.conf as conf
from libs.test_loader import load
import libs.ddt as ddt
import libs.utils as utils
from libs.logger import infoLogger
import collections
import libs.conf as conf


@ddt.ddt
@multi_dimension(True)
class TestPartitionKey(TestCaseBase):

    leader, slave1, slave2 = (i for i in conf.tb_endpoints)

    def convert_partition_key(self, raw_key):
        partition_key = raw_key.split(",")
        if len(partition_key) == 1:
            partition_key = raw_key
        return partition_key

    @ddt.data(
            ("col1", "Create table ok"),
            ("col2", "Create table ok"),
            ("col3", "Create table ok"),
            ("col1,col2", "Create table ok"),
            ("col1,col2,col3", "Create table ok"),
            ("", "Fail to create table"),
            ("xxx", "Fail to create table"),
            ("col1,xxx", "Fail to create table"),
            ("xxx,col1", "Fail to create table"),
            )
    def test_partitionkey_create(self, args):
        name = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
                "name": name,
                "ttl": 0,
                "column_desc":[
                    {"name": "col1", "type": "string", "add_ts_idx": "true"},
                    {"name": "col2", "type": "string", "add_ts_idx": "false"},
                    {"name": "col3", "type": "double", "add_ts_idx": "false"},
                    ],
                "partition_key": self.convert_partition_key(args[0])
                }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn(args[1], rs);
        if args[1] == "Create table ok":
            result = self.ns_info(self.ns_leader, name)
            self.assertEqual(result["partition_key"].replace(" ", ""), args[0]);
        self.ns_drop(self.ns_leader, name)

    @ddt.data(
            ("col1", "Create table ok"),
            ("col2", "Create table ok"),
            ("col3", "Create table ok"),
            ("col1,col2", "Create table ok"),
            ("col1,col2,col3", "Create table ok"),
            ("", "Fail to create table"),
            ("xxx", "Fail to create table"),
            ("col1,xxx", "Fail to create table"),
            ("xxx,col1", "Fail to create table"),
            )
    def test_partitionkey_create_by_column_key(self, args):
        name = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
                "name": name,
                "ttl": 0,
                "column_desc":[
                    {"name": "col1", "type": "string", "add_ts_idx": "false"},
                    {"name": "col2", "type": "string", "add_ts_idx": "false"},
                    {"name": "col3", "type": "int64", "add_ts_idx": "false", "is_ts_col": "true"},
                    ],
                "partition_key": self.convert_partition_key(args[0]),
                "column_key": [
                    {"index_name":"col1", "ts_name":["col3"]},
                    ]
                }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn(args[1], rs);
        if args[1] == "Create table ok":
            result = self.ns_info(self.ns_leader, name)
            self.assertEqual(result["partition_key"].replace(" ", ""), args[0]);
        self.ns_drop(self.ns_leader, name)

    @ddt.data(
            (0, "card"),
            (1, "card"),
            (0, "mcc"),
            (1, "mcc"),
            (0, "amt"),
            (1, "amt"),
            (0, "card,mcc"),
            (1, "card,mcc"),
            )
    def test_partitionkey_all(self, args):
        name = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
                "name": name,
                "ttl": 0,
                "format_version": args[0],
                "column_desc":[
                    {"name": "card", "type": "string", "add_ts_idx": "true"},
                    {"name": "mcc", "type": "string", "add_ts_idx": "true"},
                    {"name": "amt", "type": "double", "add_ts_idx": "false"},
                    ],
                "partition_key": self.convert_partition_key(args[1])
                }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs)

        row = ['card0', 'mcc0', '1.3']
        self.ns_put_multi(self.ns_leader, name, 11, row)
        row = ['card0', 'mcc1', '1.4']
        self.ns_put_multi(self.ns_leader, name, 22, row)
        row = ['card1', 'mcc1', '1.5']
        self.ns_put_multi(self.ns_leader, name, 33, row)

        rs = self.ns_scan_multi(self.ns_leader, name, 'card0', 'card', 0, 0)
        self.assertEqual(len(rs), 2)
        rs = self.ns_scan_multi(self.ns_leader, name, 'mcc0', 'mcc', 0, 0)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0]['card'], 'card0')
        self.assertEqual(rs[0]['mcc'], 'mcc0')
        rs = self.ns_scan_multi(self.ns_leader, name, 'mcc1', 'mcc', 0, 0)
        self.assertEqual(len(rs), 2)

        rs = self.ns_get_multi(self.ns_leader, name, 'mcc1', 'mcc', 0)
        self.assertEqual(rs['card'], 'card1')
        self.assertEqual(rs['mcc'], 'mcc1')
        rs = self.ns_get_multi(self.ns_leader, name, 'card0', 'card', 0)
        self.assertEqual(rs['card'], 'card0')
        self.assertEqual(rs['mcc'], 'mcc1')

        rs = self.ns_count(self.ns_leader, name, 'card0', 'card')
        self.assertEqual("count: 2", rs)
        rs = self.ns_count(self.ns_leader, name, 'mcc1', 'mcc')
        self.assertEqual("count: 2", rs)
        rs = self.ns_count(self.ns_leader, name, 'mcc0', 'mcc')
        self.assertEqual("count: 1", rs)

        rs = self.ns_preview(self.ns_leader, name)
        self.assertEqual(len(rs), 3)

        rs = self.ns_delete(self.ns_leader, name, "mcc1", "mcc") 
        rs = self.ns_count(self.ns_leader, name, 'mcc1', 'mcc')
        self.assertEqual("count: 0", rs)

        self.ns_drop(self.ns_leader, name)


if __name__ == "__main__":
    load(TestPartitionKey)
