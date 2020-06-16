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

    @ddt.data(0, 1)
    def test_partitionkey(self, format_version):
        name = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
                "name": name,
                "ttl": 0,
                "format_version": format_version,
                "column_desc":[
                    {"name": "card", "type": "string", "add_ts_idx": "true"},
                    {"name": "mcc", "type": "string", "add_ts_idx": "true"},
                    {"name": "amt", "type": "double", "add_ts_idx": "false"},
                    ],
                "partition_key": "card"
                }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs)

        row = ['card0', 'mcc0', '1.3']
        self.ns_put_multi(self.ns_leader, name, self.now(), row)
        row = ['card0', 'mcc1', '1.4']
        self.ns_put_multi(self.ns_leader, name, self.now(), row)
        row = ['card1', 'mcc1', '1.5']
        self.ns_put_multi(self.ns_leader, name, self.now(), row)

        rs = self.ns_scan_multi(self.ns_leader, name, 'card0', 'card', 0, 0)
        self.assertEqual(len(rs), 2)
        rs = self.ns_scan_multi(self.ns_leader, name, 'mcc0', 'mcc', 0, 0)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0]['card'], 'card0')
        self.assertEqual(rs[0]['mcc'], 'mcc0')
        rs = self.ns_scan_multi(self.ns_leader, name, 'mcc1', 'mcc', 0, 0)
        self.assertEqual(len(rs), 2)

        self.ns_drop(self.ns_leader, name)


if __name__ == "__main__":
    load(TestPartitionKey)
