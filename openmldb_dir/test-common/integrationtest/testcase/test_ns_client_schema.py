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
class TestSchema(TestCaseBase):

    leader, slave1, slave2 = (i for i in conf.tb_endpoints)

    def test_schema(self):
        name = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        m = utils.gen_table_metadata(
            '"{}"'.format(name), '"kAbsoluteTime"', 144000, 8,
            ('table_partition', '"{}"'.format(self.leader), '"0-2"', 'true'),
            ('table_partition', '"{}"'.format(self.slave1), '"0-1"', 'false'),
            ('table_partition', '"{}"'.format(self.slave2), '"1-2"', 'false'),
            ('column_desc', '"k1"', '"string"', 'true'),
            ('column_desc', '"k2"', '"int16"', 'true'),
            ('column_desc', '"k3"', '"uint16"', 'false'),
            ('column_desc', '"k4"', '"int32"', 'false'),
            ('column_desc', '"k5"', '"uint32"', 'false'),
            ('column_desc', '"k6"', '"int64"', 'false'),
            ('column_desc', '"k7"', '"uint64"', 'false'),
            ('column_desc', '"k8"', '"bool"', 'false'),
            ('column_desc', '"k9"', '"float"', 'false'),
            ('column_desc', '"k10"', '"double"', 'false'),
            ('column_desc', '"k11"', '"timestamp"', 'false'),
            ('column_desc', '"k12"', '"date"', 'false'),
        )
        utils.gen_table_metadata_file(m, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs)

        row = ['testvalue0', '-100', '100','-1000', '1000', '-10000', '10000', 'true', '1.5', '1123.65', '1545724145000', '2018-12-25']
        self.ns_put_multi(self.ns_leader, name, self.now(), row)
        time.sleep(0.5)
        rs = self.ns_scan_multi(self.ns_leader, name, 'testvalue0', 'k1', self.now() + 100, 0)
        self.assertEqual(rs[0]['k1'], 'testvalue0')
        self.assertEqual(rs[0]['k2'], '-100')
        self.assertEqual(rs[0]['k3'], '100')
        self.assertEqual(rs[0]['k4'], '-1000')
        self.assertEqual(rs[0]['k5'], '1000')
        self.assertEqual(rs[0]['k6'], '-10000')
        self.assertEqual(rs[0]['k7'], '10000')
        self.assertEqual(rs[0]['k8'], 'true')
        self.assertAlmostEqual(float(rs[0]['k9']), float('1.5'))
        self.assertAlmostEqual(float(rs[0]['k10']), float('1123.65'))
        self.assertEqual(rs[0]['k11'], '1545724145000')
        self.assertEqual(rs[0]['k12'], '2018-12-25')

        rs1 = self.ns_get_multi(self.ns_leader, name, 'testvalue0', 'k1', 0)
        self.assertEqual(rs1['k1'], 'testvalue0')
        self.assertEqual(rs1['k2'], '-100')
        self.assertEqual(rs1['k3'], '100')
        self.assertEqual(rs1['k4'], '-1000')
        self.assertEqual(rs1['k5'], '1000')
        self.assertEqual(rs1['k6'], '-10000')
        self.assertEqual(rs1['k7'], '10000')
        self.assertEqual(rs1['k8'], 'true')
        self.assertAlmostEqual(float(rs1['k9']), float('1.5'))
        self.assertAlmostEqual(float(rs1['k10']), float('1123.65'))
        self.assertEqual(rs1['k11'], '1545724145000')
        self.assertEqual(rs1['k12'], '2018-12-25')
        self.ns_drop(self.ns_leader, name)

    def test_showschema(self):
        name = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
                "name": name,
                "ttl": 14400,
                "column_desc":[
                    {"name": "card", "type": "string", "add_ts_idx": "true"},
                    {"name": "mcc", "type": "string", "add_ts_idx": "true"},
                    {"name": "amt", "type": "double", "add_ts_idx": "false"},
                    {"name": "ts1", "type": "int64", "add_ts_idx": "false", "is_ts_col": "true"},
                    {"name": "ts2", "type": "int64", "add_ts_idx": "false", "is_ts_col": "true", "ttl": 100},
                    ],
                "column_key":[
                    {"index_name":"card", "ts_name":["ts1", "ts2"]},
                    {"index_name":"mcc", "ts_name":["ts2"]},
                    ]
                }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs)
        (schema, column_key) = self.ns_showschema(self.ns_leader, name)
        self.assertEqual(len(schema), 5)
        self.assertEqual(schema[0], ['0', 'card', 'string'])
        self.assertEqual(schema[2], ['2', 'amt', 'double'])
        self.assertEqual(schema[3], ['3', 'ts1', 'int64'])
        self.assertEqual(len(column_key), 3)
        self.assertEqual(column_key[0], ["0", "card", "card", "ts1", "14400min"])
        self.assertEqual(column_key[1], ["1", "card", "card", "ts2", "100min"])
        self.assertEqual(column_key[2], ["2", "mcc", "mcc", "ts2", "100min"])

        rs = self.ns_deleteindex(self.ns_leader, name, "card")
        self.assertIn('Fail to delete index', rs)
        rs = self.ns_deleteindex(self.ns_leader, name, "mcc")
        self.assertIn('delete index ok', rs)

        (schema, column_key) = self.ns_showschema(self.ns_leader, name)
        self.assertEqual(len(schema), 5)
        self.assertEqual(schema[0], ['0', 'card', 'string'])
        self.assertEqual(schema[2], ['2', 'amt', 'double'])
        self.assertEqual(schema[3], ['3', 'ts1', 'int64'])
        self.assertEqual(len(column_key), 2)
        self.assertEqual(column_key[0], ["0", "card", "card", "ts1", "14400min"])
        self.assertEqual(column_key[1], ["1", "card", "card", "ts2", "100min"])

        self.ns_drop(self.ns_leader, name)

    def test_showschema_no_columnkey(self):
        name = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
                "name": name,
                "ttl": 14400,
                "column_desc":[
                    {"name": "card", "type": "string", "add_ts_idx": "true"},
                    {"name": "mcc", "type": "string", "add_ts_idx": "true"},
                    {"name": "amt", "type": "double", "add_ts_idx": "false"},
                    {"name": "ts1", "type": "int64", "add_ts_idx": "false", "is_ts_col": "true"},
                    ]
                }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs)
        (schema, column_key) = self.ns_showschema(self.ns_leader, name)
        self.assertEqual(len(schema), 4)
        self.assertEqual(schema[0], ["0", "card", "string"])
        self.assertEqual(schema[2], ["2", "amt", "double"])
        self.assertEqual(schema[3], ["3", "ts1", "int64"])
        self.assertEqual(len(column_key), 2)
        self.assertEqual(column_key[0], ["0", "card", "card", "ts1", "14400min"])
        self.assertEqual(column_key[1], ["1", "mcc", "mcc", "ts1", "14400min"])

        rs = self.ns_deleteindex(self.ns_leader, name, "card")
        self.assertIn('Fail to delete index', rs)
        rs = self.ns_deleteindex(self.ns_leader, name, "mcc")
        self.assertIn('delete index ok', rs)

        (schema, column_key) = self.ns_showschema(self.ns_leader, name)
        self.assertEqual(len(schema), 4)
        self.assertEqual(schema[0], ["0", "card", "string"])
        self.assertEqual(schema[2], ["2", "amt", "double"])
        self.assertEqual(schema[3], ["3", "ts1", "int64"])
        self.assertEqual(len(column_key), 1)
        self.assertEqual(column_key[0], ["0", "card", "card", "ts1", "14400min"])

        self.ns_drop(self.ns_leader, name)

    def test_showschema_no_columnkey_no_tskey(self):
        name = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
                "name": name,
                "ttl": 14400,
                "column_desc":[
                    {"name": "card", "type": "string", "add_ts_idx": "true"},
                    {"name": "mcc", "type": "string", "add_ts_idx": "true"},
                    {"name": "amt", "type": "double", "add_ts_idx": "false"},
                    {"name": "ts1", "type": "int64", "add_ts_idx": "false"}
                    ]
                }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs)
        (schema, column_key) = self.ns_showschema(self.ns_leader, name)
        self.assertEqual(len(schema), 4)
        self.assertEqual(schema[0], ["0", "card", "string", "yes"])
        self.assertEqual(schema[1], ["1", "mcc", "string", "yes"])
        self.assertEqual(schema[2], ["2", "amt", "double", "no"])
        self.assertEqual(schema[3], ["3", "ts1", "int64", "no"])
        self.ns_drop(self.ns_leader, name)

    def test_showschema_no_schema(self):
        name = 'tname{}'.format(time.time())
        rs = self.ns_create_cmd(self.ns_leader, name, '0', '8', '3')
        self.assertIn('Create table ok', rs)
        (schema_map, column_key) = self.ns_showschema(self.ns_leader, name)
        self.assertEqual(len(schema_map), 0)
        self.assertEqual(len(column_key), 0)

    @ddt.data(0, 1)
    def test_addindex(self, format_version):
        name = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
                "name": name,
                "ttl": 14400,
                "format_version": format_version,
                "column_desc":[
                    {"name": "card", "type": "string", "add_ts_idx": "false"},
                    {"name": "mcc", "type": "string", "add_ts_idx": "false"},
                    {"name": "amt", "type": "double", "add_ts_idx": "false"},
                    ],
                "column_key":[
                    {"index_name":"card"},
                    ]
                }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs)
        (schema, column_key) = self.ns_showschema(self.ns_leader, name)
        self.assertEqual(len(schema), 3)
        self.assertEqual(schema[0], ['0', 'card', 'string'])
        self.assertEqual(schema[2], ['2', 'amt', 'double'])
        self.assertEqual(len(column_key), 1)
        self.assertEqual(column_key[0], ["0", "card", "card", "-", "14400min"])
        row = ['card0', 'mcc0', '1.3']
        self.ns_put_multi(self.ns_leader, name, self.now(), row)
        row = ['card0', 'mcc1', '1.4']
        self.ns_put_multi(self.ns_leader, name, self.now(), row)
        row = ['card1', 'mcc1', '1.5']
        self.ns_put_multi(self.ns_leader, name, self.now(), row)

        rs = self.ns_scan_multi(self.ns_leader, name, 'mcc0', 'mcc', self.now() + 100, 0)
        self.assertEqual(len(rs), 0)

        rs = self.ns_addindex(self.ns_leader, name, "mcc")
        self.assertIn('addindex ok', rs)

        time.sleep(2)
        self.wait_op_done(name)

        (schema, column_key) = self.ns_showschema(self.ns_leader, name)
        self.assertEqual(len(schema), 3)
        self.assertEqual(schema[0], ['0', 'card', 'string'])
        self.assertEqual(schema[2], ['2', 'amt', 'double'])
        self.assertEqual(len(column_key), 2)
        self.assertEqual(column_key[0], ["0", "card", "card", "-", "14400min"])
        self.assertEqual(column_key[1], ["1", "mcc", "mcc", "-", "14400min"])

        rs = self.ns_scan_multi(self.ns_leader, name, 'mcc0', 'mcc', self.now() + 100, 0)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0]['card'], 'card0')
        self.assertEqual(rs[0]['mcc'], 'mcc0')

        self.ns_drop(self.ns_leader, name)

    @ddt.data(0, 1)
    def test_addindex_multi(self, format_version):
        name = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
                "name": name,
                "ttl": 14400,
                "format_version": format_version,
                "partition_num": 8, 
                "column_desc":[
                    {"name": "card", "type": "string", "add_ts_idx": "false"},
                    {"name": "mcc", "type": "string", "add_ts_idx": "false"},
                    {"name": "amt", "type": "string", "add_ts_idx": "false"},
                    {"name": "num", "type": "int64", "add_ts_idx": "false"},
                    {"name": "coll", "type": "int64", "add_ts_idx": "false"},
                    {"name": "loc", "type": "string", "add_ts_idx": "false"},
                    ],
                "column_key":[
                    {"index_name":"card"},
                    {"index_name":"mcc"},
                    {"index_name":"amt"},
                    {"index_name":"num"},
                    {"index_name":"coll"},
                    ]
                }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs)
        (schema, column_key) = self.ns_showschema(self.ns_leader, name)
        self.assertEqual(len(schema), 6)
        self.assertEqual(schema[0], ['0', 'card', 'string'])
        self.assertEqual(schema[4], ['4', 'coll', 'int64'])
        self.assertEqual(len(column_key), 5)
        self.assertEqual(column_key[0], ["0", "card", "card", "-", "14400min"])
        self.assertEqual(column_key[4], ["4", "coll", "coll", "-", "14400min"])
        for i in range (20):
            row = ["card"+str(i), "mcc"+str(i), str(2.33 + i), str(i + 123), str(i+666), "loc" + str(i)]
            self.ns_put_multi(self.ns_leader, name, self.now(), row)
        
        rs = self.ns_scan_multi(self.ns_leader, name, 'loc0', 'loc', self.now() + 100, 0)
        self.assertEqual(len(rs), 0)

        rs = self.ns_addindex(self.ns_leader, name, "loc")
        self.assertIn('addindex ok', rs)

        time.sleep(2)
        self.wait_op_done(name)

        (schema, column_key) = self.ns_showschema(self.ns_leader, name)
        self.assertEqual(len(schema), 6)
        self.assertEqual(schema[0], ['0', 'card', 'string'])
        self.assertEqual(schema[4], ['4', 'coll', 'int64'])

        self.assertEqual(len(column_key), 6)
        self.assertEqual(column_key[0], ["0", "card", "card", "-", "14400min"])
        self.assertEqual(column_key[4], ["4", "coll", "coll", "-", "14400min"])
        self.assertEqual(column_key[5], ["5", "loc", "loc", "-", "14400min"])
        for i in range(20):
            rs = self.ns_scan_multi(self.ns_leader, name, 'loc'+str(i), 'loc', self.now() + 100, 0)
            self.assertEqual(len(rs), 1)
            self.assertEqual(rs[0]['card'], 'card'+str(i))
            self.assertEqual(rs[0]['mcc'], 'mcc'+str(i))
            self.assertEqual(rs[0]['loc'], 'loc'+str(i))

        self.ns_drop(self.ns_leader, name)

    @ddt.data(0, 1)
    def test_addColAndIndex_multi(self, format_version):
        name = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
                "name": name,
                "ttl": 14400,
                "format_version": format_version,
                "partition_num": 8, 
                "replica_num": 3, 
                "column_desc":[
                    {"name": "card", "type": "string", "add_ts_idx": "false"},
                    {"name": "mcc", "type": "string", "add_ts_idx": "false"},
                    {"name": "amt", "type": "string", "add_ts_idx": "false"},
                    {"name": "num", "type": "int64", "add_ts_idx": "false"},
                    {"name": "coll", "type": "int64", "add_ts_idx": "false"},
                    {"name": "loc", "type": "string", "add_ts_idx": "false"},
                    ],
                "column_key":[
                    {"index_name":"card"},
                    {"index_name":"mcc"},
                    {"index_name":"amt"},
                    {"index_name":"num"},
                    {"index_name":"coll"},
                    ]
                }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs)
        (schema, column_key) = self.ns_showschema(self.ns_leader, name)
        self.assertEqual(len(schema), 6)
        self.assertEqual(schema[0], ['0', 'card', 'string'])
        self.assertEqual(schema[4], ['4', 'coll', 'int64'])
        self.assertEqual(len(column_key), 5)
        self.assertEqual(column_key[0], ["0", "card", "card", "-", "14400min"])
        self.assertEqual(column_key[4], ["4", "coll", "coll", "-", "14400min"])
        for i in range (20):
            row = ["card"+str(i), "mcc"+str(i), str(2.33 + i), str(i + 123), str(i+666), "loc" + str(i)]
            self.ns_put_multi(self.ns_leader, name, self.now(), row)
        
        rs = self.ns_scan_multi(self.ns_leader, name, 'loc0', 'loc', self.now() + 100, 0)
        self.assertEqual(len(rs), 0)

        rs = self.ns_addindex(self.ns_leader, name, "add", "add:int32,add2:string")
        self.assertIn('addindex ok', rs)
        time.sleep(2)
        self.wait_op_done(name)

        rs = self.ns_addindex(self.ns_leader, name, "loc")
        self.assertIn('addindex ok', rs)

        time.sleep(2)
        self.wait_op_done(name)

        (schema, column_key) = self.ns_showschema(self.ns_leader, name)
        self.assertEqual(len(schema), 8)
        self.assertEqual(schema[0], ['0', 'card', 'string'])
        self.assertEqual(schema[4], ['4', 'coll', 'int64'])
        self.assertEqual(schema[6], ['6', 'add', 'int32'])
        self.assertEqual(schema[7], ['7', 'add2', 'string'])

        self.assertEqual(len(column_key), 7)
        self.assertEqual(column_key[0], ["0", "card", "card", "-", "14400min"])
        self.assertEqual(column_key[4], ["4", "coll", "coll", "-", "14400min"])
        self.assertEqual(column_key[5], ["5", "add", "add|add2", "-", "14400min"])
        self.assertEqual(column_key[6], ["6", "loc", "loc", "-", "14400min"])
        for i in range(20):
            rs = self.ns_scan_multi(self.ns_leader, name, 'loc'+str(i), 'loc', self.now() + 100, 0)
            self.assertEqual(len(rs), 1)
            self.assertEqual(rs[0]['card'], 'card'+str(i))
            self.assertEqual(rs[0]['mcc'], 'mcc'+str(i))
            self.assertEqual(rs[0]['loc'], 'loc'+str(i))
        for i in range (20):
            row = ["card"+str(i), "mcc"+str(i), str(2.33 + i), str(i + 123), str(i+666), "loc{}".format(i), "{}".format(i), "2add{}".format(i)]
            self.ns_put_multi(self.ns_leader, name, self.now(), row)
        for i in range(20):
            rs = self.ns_scan_multi(self.ns_leader, name, '{}|2add{}'.format(i, i), 'add', self.now() + 100, 0)
            self.assertEqual(len(rs), 1)
            self.assertEqual(rs[0]['card'], 'card'+str(i))
            self.assertEqual(rs[0]['mcc'], 'mcc'+str(i))
            self.assertEqual(rs[0]['add'], str(i))
        for i in range(20):
            rs = self.ns_scan_multi(self.ns_leader, name, 'loc'+str(i), 'loc', self.now() + 100, 0)
            self.assertEqual(len(rs), 2)
            self.assertEqual(rs[0]['card'], 'card'+str(i))
            self.assertEqual(rs[0]['mcc'], 'mcc'+str(i))
            self.assertEqual(rs[0]['loc'], 'loc'+str(i))
            self.assertEqual(rs[0]['add2'], "2add{}".format(i))
        rs = self.ns_deleteindex(self.ns_leader, name, "loc")
        self.assertIn('delete index ok', rs)
        time.sleep(2)

        rs = self.ns_addindex(self.ns_leader, name, "loc2", "loc")
        self.assertIn('addindex ok', rs)

        time.sleep(2)
        self.wait_op_done(name)
        time.sleep(2)
        for i in range(20):
            rs = self.ns_scan_multi(self.ns_leader, name, 'loc'+str(i), 'loc2', 0, 0)
            self.assertEqual(len(rs), 2)
            self.assertEqual(rs[0]['card'], 'card'+str(i))
            self.assertEqual(rs[0]['mcc'], 'mcc'+str(i))
            self.assertEqual(rs[0]['loc'], 'loc'+str(i))
            self.assertEqual(rs[0]['add2'], "2add{}".format(i))

        self.ns_drop(self.ns_leader, name)

if __name__ == "__main__":
    load(TestSchema)
