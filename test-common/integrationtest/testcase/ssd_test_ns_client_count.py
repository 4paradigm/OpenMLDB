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
        # rs = self.ns_create_cmd(self.ns_leader, name, '0', str(8), str(3), '')
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
            "name": name,
            "ttl": 0,
            "partition_num": 8,
            "replica_num": 3,
            "storage_mode": "kSSD",
        }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
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
        # rs = self.ns_create_cmd(self.ns_leader, name, 'latest:2', str(8), str(3), '')
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
            "name": name,
            "ttl_type": "kLatestTime",
            "ttl": 2,
            "partition_num": 8,
            "replica_num": 3,
            "storage_mode": "kSSD",
        }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
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
        self.assertIn('count: 2', rs5)
        rs6 = self.ns_count(self.ns_leader, name, 'testkey0', '', 'true')
        self.assertIn('count: 2', rs6)

    def test_count_schema(self):
        """
        schema表统计pk下的条数
        :return:
        """
        name = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        # m = utils.gen_table_metadata(
        #     '"{}"'.format(name), '"kAbsoluteTime"', 0, 8,
        #     ('table_partition', '"{}"'.format(self.leader), '"0-2"', 'true'),
        #     ('table_partition', '"{}"'.format(self.slave1), '"0-1"', 'false'),
        #     ('table_partition', '"{}"'.format(self.slave2), '"1-2"', 'false'),
        #     ('column_desc', '"k1"', '"string"', 'true'),
        #     ('column_desc', '"k2"', '"string"', 'true'),
        #     ('column_desc', '"k3"', '"uint16"', 'false'),
        # )
        # utils.gen_table_metadata_file(m, metadata_path)

        table_meta = {
            "name": name,
            "ttl": 0,
            "partition_num": 8,
            "storage_mode": "kSSD",
            "table_partition": [
                {"endpoint": self.leader,"pid_group": "0-2","is_leader": "true"},
                {"endpoint": self.slave1,"pid_group": "0-1","is_leader": "false"},
                {"endpoint": self.slave2,"pid_group": "1-2","is_leader": "false"},
            ],
            "column_desc":[
                {"name": "k1", "type": "string", "add_ts_idx": "true"},
                {"name": "k2", "type": "string", "add_ts_idx": "true"},
                {"name": "k3", "type": "string", "add_ts_idx": "false"},
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

