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
class TestSetTTL(TestCaseBase):

    def test_set_ttl_normal(self):
        """
        测试setttl函数，正常修改ttl值，查看返回值
        :return:
        """
        rs_absolute1 = self.ns_create_cmd(self.ns_leader, 't1', '10', str(8), str(3), '')
        rs_latest1 = self.ns_create_cmd(self.ns_leader, 'latest1', 'latest:10', str(8), str(3), '')
        self.assertIn('Create table ok', rs_absolute1)
        self.assertIn('Create table ok', rs_latest1)


        rs_absolute2 = self.ns_setttl(self.ns_leader, 'setttl', 't1', 'absolute', 1)
        rs_latest2 = self.ns_setttl(self.ns_leader, 'setttl', 'latest1', 'latest', 1)
        self.assertIn('Set ttl ok !', rs_absolute2)
        self.assertIn('Set ttl ok !', rs_latest2)


        rs_absolute3 = self.ns_setttl(self.ns_leader, 'setttl', 't1', 'absolute', -1)
        rs_latest3 = self.ns_setttl(self.ns_leader, 'setttl', 'latest1', 'latest', -1)
        self.assertIn('Set ttl failed! fail to update ttl from tablet', rs_absolute3)
        self.assertIn('Set ttl failed! fail to update ttl from tablet', rs_latest3)


    def test_set_ttl_expired(self):
        """
        测试修改后的ttl值，scan后的数据被删除。两种类型latest和absolute都要测试
        :return:
        """
        rs_absolute1 = self.ns_create_cmd(self.ns_leader, 't1', '10', str(8), str(3), '')
        rs_latest1 = self.ns_create_cmd(self.ns_leader, 'latest1', 'latest:10', str(8), str(3), '')
        self.assertIn('Create table ok', rs_absolute1)
        self.assertIn('Create table ok', rs_latest1)
        rs_absolute2 = self.ns_setttl(self.ns_leader, 'setttl', 't1', 'absolute', 1)
        rs_latest2 = self.ns_setttl(self.ns_leader, 'setttl', 'latest1', 'latest', 1)
        self.assertIn('Set ttl ok !', rs_absolute2)
        self.assertIn('Set ttl ok !', rs_latest2)

        rs_time = self.now()
        rs_absolute3 = self.ns_put_kv(self.ns_leader, 't1', 'testkey0', str(rs_time), 'testvalue0')
        rs_latest3 = self.ns_put_kv(self.ns_leader, 'latest1', 'testkey0', str(rs_time), 'testvalue0')
        self.assertIn('Put ok', rs_absolute3)
        self.assertIn('Put ok', rs_latest3)

        rs_absolute4 = self.ns_get_kv(self.ns_leader, 't1', 'testkey0', str(rs_time))
        rs_latest4 = self.ns_get_kv(self.ns_leader, 'latest1', 'testkey0', str(rs_time))
        self.assertIn('testvalue0', rs_absolute4)
        self.assertIn('testvalue0', rs_latest4)

        rs_latest5 = self.ns_put_kv(self.ns_leader, 'latest1', 'testkey0', str(rs_time), 'testvalue1')
        self.assertIn('Put ok', rs_latest5)
        time.sleep(1)

        rs_latest6 = self.ns_get_kv(self.ns_leader, 'latest1', 'testkey0', str(rs_time))
        self.assertFalse('testvalue0' in rs_latest6)
        rs_absolute5 = self.ns_scan_kv(self.ns_leader, 't1', 'testkey0', str(rs_time), '0', ' ')
        self.assertEqual('testvalue0', rs_absolute5[0]['data'])
        time.sleep(61)

        rs_absolute5 = self.ns_scan_kv(self.ns_leader, 't1', 'testkey0', str(rs_time), '0', ' ')
        self.assertEqual(0, len(rs_absolute5))

    def test_set_ttl_ts_name(self):
        name = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
                "name": name,
                "ttl": 14400,
                "storage_mode": "kSSD",
                "column_desc":[
                    {"name": "card", "type": "string", "add_ts_idx": "true"},
                    {"name": "mcc", "type": "string", "add_ts_idx": "true"},
                    {"name": "amt", "type": "double", "add_ts_idx": "false"},
                    {"name": "ts1", "type": "int64", "add_ts_idx": "false", "is_ts_col": "true", "ttl": 1000},
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
        self.assertEqual(len(column_key), 3)
        self.assertEqual(column_key[0], ["0", "card", "card", "ts1", "1000min"])
        self.assertEqual(column_key[1], ["1", "card", "card", "ts2", "100min"])
        self.assertEqual(column_key[2], ["2", "mcc", "mcc", "ts2", "100min"])
        rs1 = self.ns_setttl(self.ns_leader, 'setttl', name, 'absolute', 10, 'ts1')
        self.assertIn('Set ttl ok !', rs1)
        (schema, column_key) = self.ns_showschema(self.ns_leader, name)
        self.assertEqual(len(column_key), 3)
        self.assertEqual(column_key[0], ["0", "card", "card", "ts1", "10min"])
        self.assertEqual(column_key[1], ["1", "card", "card", "ts2", "100min"])
        self.assertEqual(column_key[2], ["2", "mcc", "mcc", "ts2", "100min"])
        self.ns_drop(self.ns_leader, name)

    def test_set_ttl_ts_name_latest(self):
        name = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
                "name": name,
                "ttl": 100,
                "ttl_type": "kLatestTime",
                "column_desc":[
                    {"name": "card", "type": "string", "add_ts_idx": "true"},
                    {"name": "mcc", "type": "string", "add_ts_idx": "true"},
                    {"name": "amt", "type": "double", "add_ts_idx": "false"},
                    {"name": "ts1", "type": "int64", "add_ts_idx": "false", "is_ts_col": "true", "ttl": 10},
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
        (schema, column_key) = self.ns_showschema(self.ns_leader, name)
        self.assertEqual(len(column_key), 3)
        self.assertEqual(column_key[0], ["0", "card", "card", "ts1", "10"])
        self.assertEqual(column_key[1], ["1", "card", "card", "ts2", "100"])
        self.assertEqual(column_key[2], ["2", "mcc", "mcc", "ts2", "100"])
        rs1 = self.ns_setttl(self.ns_leader, 'setttl', name, 'latest', 1, 'ts2')
        self.assertIn('Set ttl ok !', rs1)
        (schema, column_key) = self.ns_showschema(self.ns_leader, name)
        self.assertEqual(len(column_key), 3)
        self.assertEqual(column_key[0], ["0", "card", "card", "ts1", "10"])
        self.assertEqual(column_key[1], ["1", "card", "card", "ts2", "1"])
        self.assertEqual(column_key[2], ["2", "mcc", "mcc", "ts2", "1"])
        self.ns_drop(self.ns_leader, name)

if __name__ == "__main__":
    load(TestSetTTL)
