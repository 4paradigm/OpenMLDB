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
class TestPreview(TestCaseBase):
    @ddt.data(
        ['kSSD'],
        ['kHDD'],
    )
    @ddt.unpack
    def test_preview_kv(self,storage_mode):
        """
        预览kv表
        :return:
        """
        # rs = self.ns_create_cmd(self.ns_leader, 't1', '0', str(8), str(3), '')

        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
            "name": "t1",
            "ttl": 0,
            "partition_num": 1,
            "replica_num": 3,
            "storage_mode": storage_mode,
        }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)

        self.assertIn('Create table ok', rs)


        rs1 = self.ns_put_kv(self.ns_leader, 't1', 'testkey0', '11', 'testvalue0')
        self.assertIn('Put ok', rs1)
        rs2 = self.ns_put_kv(self.ns_leader, 't1', 'testkey0', '22', 'testvalue1')
        self.assertIn('Put ok', rs2)
        rs3 = self.ns_put_kv(self.ns_leader, 't1', 'testkey1', '33', 'testvalue2')
        self.assertIn('Put ok', rs3)

        rs4 = self.ns_preview(self.ns_leader, 't1', 2)
        self.assertEqual(2, len(rs4))
        self.assertEqual(rs4[0]['key'], 'testkey0')
        self.assertEqual(rs4[0]['data'], 'testvalue1')
        self.assertEqual(rs4[1]['key'], 'testkey0')
        self.assertEqual(rs4[1]['data'], 'testvalue0')
        rs5 = self.ns_preview(self.ns_leader, 't1')
        self.assertEqual(3, len(rs5))
        self.assertEqual(rs5[0]['key'], 'testkey0')
        self.assertEqual(rs5[0]['data'], 'testvalue1')
        self.assertEqual(rs5[1]['key'], 'testkey0')
        self.assertEqual(rs5[1]['data'], 'testvalue0')
        self.assertEqual(rs5[2]['key'], 'testkey1')
        self.assertEqual(rs5[2]['data'], 'testvalue2')
    @ddt.data(
        ['kSSD'],
        ['kHDD'],
    )
    @ddt.unpack
    def test_preview_schema(self,storage_mode):
        """
        预览schema表
        :return:
        """
        name = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)

        table_meta = {
            "name": name,
            "ttl": 0,
            "storage_mode": storage_mode,
            "partition_num": 1,
            "table_partition": [
                {"endpoint": self.leader,"pid_group": "0-2","is_leader": "true"},
                {"endpoint": self.slave1,"pid_group": "0-1","is_leader": "false"},
                {"endpoint": self.slave2,"pid_group": "1-2","is_leader": "false"},
            ],
            "column_desc":[
                {"name": "k1", "type": "string", "add_ts_idx": "true"},
                {"name": "k2", "type": "string", "add_ts_idx": "true"},
                {"name": "k3", "type": "uint16", "add_ts_idx": "false"},
            ],
        }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs)

        rs1 = self.ns_put_multi(self.ns_leader, name, 11, ['card0', 'mcc0', '15'])
        self.assertIn('Put ok', rs1)
        rs2 = self.ns_put_multi(self.ns_leader, name, 22, ['card0', 'mcc1', '16'])
        self.assertIn('Put ok', rs2)
        rs3 = self.ns_put_multi(self.ns_leader, name, 33, ['card1', 'mcc2', '20'])
        self.assertIn('Put ok', rs3)

        rs4 = self.ns_preview(self.ns_leader, name, 2)
        self.assertEqual(2, len(rs4))
        self.assertEqual(rs4[0]['k1'], 'card1')
        self.assertEqual(rs4[0]['k2'], 'mcc2')
        self.assertEqual(rs4[0]['k3'], '20')
        self.assertEqual(rs4[1]['k1'], 'card0')
        self.assertEqual(rs4[1]['k2'], 'mcc1')
        rs5 = self.ns_preview(self.ns_leader, name)
        self.assertEqual(3, len(rs5))
        self.assertEqual(rs5[0]['k1'], 'card1')
        self.assertEqual(rs5[0]['k2'], 'mcc2')
        self.assertEqual(rs5[0]['k3'], '20')
        self.assertEqual(rs5[1]['k1'], 'card0')
        self.assertEqual(rs5[1]['k2'], 'mcc1')
        self.assertEqual(rs5[2]['k1'], 'card0')
        self.assertEqual(rs5[2]['k2'], 'mcc0')

if __name__ == "__main__":
    load(TestPreview)

