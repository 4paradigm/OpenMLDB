# -*- coding: utf-8 -*-
from testcasebase import TestCaseBase
from libs.test_loader import load
from libs.logger import infoLogger
import time
import libs.ddt as ddt
import libs.conf as conf
from libs.deco import multi_dimension
import libs.utils as utils
import collections

@ddt.ddt
class TestMultiCluster(TestCaseBase):

    @ddt.data(
        ['kMemory'],
        #['kSSD'],
        #['kHDD'],
    )
    @multi_dimension(False)
    @ddt.unpack
    def test_create_after_add_rep_cluster(self, storage_mode):
        """
        指定时间列的schema表建表删表与数据同步测试
        """
        zk_root_path = '/remote'
        msg = self.ns_switch_mode(self.ns_leader, 'leader') 
        self.assertIn('switchmode ok', msg)
        msg = self.add_replica_cluster(self.ns_leader, conf.zk_endpoint, zk_root_path, self.alias)
        self.assertIn('adrepcluster ok', msg)
        time.sleep(3)

        name = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
                "name": name,
                "ttl": 0,
                "storage_mode": storage_mode,
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

        (schema, column_key) = self.ns_showschema(self.ns_leader, name)
        self.assertEqual(len(schema), 5)
        self.assertEqual(schema[0], ['0', 'card', 'string'])
        self.assertEqual(schema[1], ['1', 'mcc', 'string'])
        self.assertEqual(schema[2], ['2', 'amt', 'double'])
        self.assertEqual(schema[3], ['3', 'ts1', 'int64'])
        self.assertEqual(schema[4], ['4', 'ts2', 'int64'])
        self.assertEqual(len(column_key), 3)
        self.assertEqual(column_key[0], ["0", "card", "card", "ts1", "0min"])
        self.assertEqual(column_key[1], ["1", "card", "card", "ts2", "0min"])
        self.assertEqual(column_key[2], ["2", "mcc", "mcc", "ts2", "0min"])
        time.sleep(3)

        (schema, column_key) = self.ns_showschema(self.ns_leader_r, name)
        self.assertEqual(len(schema), 5)
        self.assertEqual(schema[0], ['0', 'card', 'string'])
        self.assertEqual(schema[1], ['1', 'mcc', 'string'])
        self.assertEqual(schema[2], ['2', 'amt', 'double'])
        self.assertEqual(schema[3], ['3', 'ts1', 'int64'])
        self.assertEqual(schema[4], ['4', 'ts2', 'int64'])
        self.assertEqual(len(column_key), 3)
        self.assertEqual(column_key[0], ["0", "card", "card", "ts1", "0min"])
        self.assertEqual(column_key[1], ["1", "card", "card", "ts2", "0min"])
        self.assertEqual(column_key[2], ["2", "mcc", "mcc", "ts2", "0min"])

        
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
        time.sleep(3)

        rs4 = self.ns_scan_multi_with_pair(self.ns_leader_r, name, 'card0', 'card',  '25', '0', 'ts1')
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
        rs5 = self.ns_scan_multi_with_pair(self.ns_leader_r, name, 'card0', 'card',  '25', '0', 'ts1', '1')
        self.assertEqual(len(rs5),1)
        self.assertEqual(rs5[0]['card'], 'card0')
        self.assertEqual(rs5[0]['mcc'], 'mcc1')
        self.assertEqual(rs5[0]['amt'], '2')
        self.assertEqual(rs5[0]['ts1'], '16')
        self.assertEqual(rs5[0]['ts2'], '19')
        rs6 = self.ns_scan_multi_with_pair(self.ns_leader_r, name, 'mcc1', 'mcc',  '25', '0', 'ts1', '1')
        self.assertEqual(len(rs6), 0)
        rs7 = self.ns_scan_multi_with_pair(self.ns_leader_r, name, 'mcc1', 'mcc',  '25', '0', 'ts2', '1')
        self.assertEqual(len(rs7), 1)

        msg = self.ns_drop(self.ns_leader, name)
        self.assertIn('drop ok', msg)
        time.sleep(3)
        rs = self.showtable(self.ns_leader_r, name)
        self.assertEqual(0, len(rs))


    @ddt.data(
        ['kMemory'],
        ['kSSD'],
        ['kHDD'],
    )
    @multi_dimension(True)
    @ddt.unpack
    def test_create_before_add_rep_cluster(self, storage_mode):
        """
        指定时间列的schema表建表删表与数据同步测试
        """
        name = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
                "name": name,
                "ttl": 0,
                "storage_mode": storage_mode,
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

        (schema, column_key) = self.ns_showschema(self.ns_leader, name)
        self.assertEqual(len(schema), 5)
        self.assertEqual(schema[0], ['0', 'card', 'string'])
        self.assertEqual(schema[1], ['1', 'mcc', 'string'])
        self.assertEqual(schema[2], ['2', 'amt', 'double'])
        self.assertEqual(schema[3], ['3', 'ts1', 'int64'])
        self.assertEqual(schema[4], ['4', 'ts2', 'int64'])
        self.assertEqual(len(column_key), 3)
        self.assertEqual(column_key[0], ["0", "card", "card", "ts1", "0min"])
        self.assertEqual(column_key[1], ["1", "card", "card", "ts2", "0min"])
        self.assertEqual(column_key[2], ["2", "mcc", "mcc", "ts2", "0min"])
        
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
        
        zk_root_path = '/remote'
        msg = self.ns_switch_mode(self.ns_leader, 'leader') 
        self.assertIn('switchmode ok', msg)
        msg = self.add_replica_cluster(self.ns_leader, conf.zk_endpoint, zk_root_path, self.alias)
        self.assertIn('adrepcluster ok', msg)
        time.sleep(60)

        (schema, column_key) = self.ns_showschema(self.ns_leader_r, name)
        self.assertEqual(len(schema), 5)
        self.assertEqual(schema[0], ['0', 'card', 'string'])
        self.assertEqual(schema[1], ['1', 'mcc', 'string'])
        self.assertEqual(schema[2], ['2', 'amt', 'double'])
        self.assertEqual(schema[3], ['3', 'ts1', 'int64'])
        self.assertEqual(schema[4], ['4', 'ts2', 'int64'])
        self.assertEqual(len(column_key), 3)
        self.assertEqual(column_key[0], ["0", "card", "card", "ts1", "0min"])
        self.assertEqual(column_key[1], ["1", "card", "card", "ts2", "0min"])
        self.assertEqual(column_key[2], ["2", "mcc", "mcc", "ts2", "0min"])

        rs4 = self.ns_scan_multi_with_pair(self.ns_leader_r, name, 'card0', 'card',  '25', '0', 'ts1')
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
        rs5 = self.ns_scan_multi_with_pair(self.ns_leader_r, name, 'card0', 'card',  '25', '0', 'ts1', '1')
        self.assertEqual(len(rs5),1)
        self.assertEqual(rs5[0]['card'], 'card0')
        self.assertEqual(rs5[0]['mcc'], 'mcc1')
        self.assertEqual(rs5[0]['amt'], '2')
        self.assertEqual(rs5[0]['ts1'], '16')
        self.assertEqual(rs5[0]['ts2'], '19')
        rs6 = self.ns_scan_multi_with_pair(self.ns_leader_r, name, 'mcc1', 'mcc',  '25', '0', 'ts1', '1')
        self.assertEqual(len(rs6), 0)
        rs7 = self.ns_scan_multi_with_pair(self.ns_leader_r, name, 'mcc1', 'mcc',  '25', '0', 'ts2', '1')
        self.assertEqual(len(rs7), 1)

        msg = self.ns_drop(self.ns_leader, name)
        self.assertIn('drop ok', msg)
        time.sleep(3)
        rs = self.showtable(self.ns_leader_r, name)
        self.assertEqual(0, len(rs))


if __name__ == "__main__":
    load(TestMultiCluster)
