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
    @ddt.data(
        ['kSSD'],
        ['kHDD'],
    )
    @ddt.unpack
    def test_schema(self,storage_mode):
        name = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
            "name": name,
            "ttl": 144000,
            "storage_mode": storage_mode,
            "table_partition": [
                {"endpoint": self.leader,"pid_group": "0-2","is_leader": "true"},
                {"endpoint": self.slave1,"pid_group": "0-1","is_leader": "false"},
                {"endpoint": self.slave2,"pid_group": "1-2","is_leader": "false"},
            ],
            "column_desc":[
                {"name": "k1", "type": "string", "add_ts_idx": "true"},
                {"name": "k2", "type": "int16", "add_ts_idx": "true"},
                {"name": "k3", "type": "uint16", "add_ts_idx": "false"},
                {"name": "k4", "type": "int32", "add_ts_idx": "false"},
                {"name": "k5", "type": "uint32", "add_ts_idx": "false"},
                {"name": "k6", "type": "int64", "add_ts_idx": "false"},
                {"name": "k7", "type": "uint64", "add_ts_idx": "false"},
                {"name": "k8", "type": "bool", "add_ts_idx": "false"},
                {"name": "k9", "type": "float", "add_ts_idx": "false"},
                {"name": "k10", "type": "double", "add_ts_idx": "false"},
                {"name": "k11", "type": "timestamp", "add_ts_idx": "false"},
                {"name": "k12", "type": "date", "add_ts_idx": "false"},
            ],
        }
        utils.gen_table_meta_file(table_meta, metadata_path)
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
    @ddt.data(
        ['kSSD'],
        ['kHDD'],
    )
    @ddt.unpack
    def test_showschema(self,storage_mode):
        name = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
                "name": name,
                "ttl": 14400,
            "partition_num": 1,
            "storage_mode": storage_mode,
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
        self.ns_drop(self.ns_leader, name)
    @ddt.data(
        ['kSSD'],
        ['kHDD'],
    )
    @ddt.unpack
    def test_showschema_no_columnkey(self,storage_mode):
        name = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
                "name": name,
                "ttl": 14400,
            "storage_mode": storage_mode,
            "partition_num": 1,
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
        self.ns_drop(self.ns_leader, name)

    @ddt.data(
        ['kSSD'],
        ['kHDD'],
    )
    @ddt.unpack
    def test_showschema_no_columnkey_no_tskey(self,storage_mode):
        name = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
                "name": name,
                "ttl": 14400,
            "storage_mode": storage_mode,
            "partition_num": 1,
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
        self.assertEqual(len(column_key), 0)
        # self.assertEqual(column_key[0], ["0", "card", "card", "-", "14400min"])
        # self.assertEqual(column_key[1], ["1", "mcc", "mcc", "-", "14400min"])
        self.ns_drop(self.ns_leader, name)

    @ddt.data(
        ['kSSD'],
        ['kHDD'],
    )
    @ddt.unpack
    def test_showschema_no_schema(self,storage_mode):
        name = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
            "name": name,
            "ttl": 0,
            "partition_num": 1,
            "replica_num": 3,
            "storage_mode": storage_mode,
        }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)

        self.assertIn('Create table ok', rs)
        (schema_map, column_key) = self.ns_showschema(self.ns_leader, name)
        self.assertEqual(len(schema_map), 0)
        self.assertEqual(len(column_key), 0)
        self.ns_drop(self.ns_leader, name)

if __name__ == "__main__":
    load(TestSchema)
