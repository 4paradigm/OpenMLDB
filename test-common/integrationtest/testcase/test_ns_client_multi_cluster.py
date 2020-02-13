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
@multi_dimension(True)
class TestMultiCluster(TestCaseBase):

    @ddt.data(
        ['kMemory'],
        ['kSSD'],
        ['kHDD'],
    )
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
                "partition_num": 4,
                "replica_num": 3,
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
        time.sleep(30)

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
                "partition_num": 4,
                "replica_num": 3,
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
        time.sleep(30)

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


    @ddt.data(
        ['kMemory'],
        ['kSSD'],
        ['kHDD'],
    )
    @ddt.unpack
    def test_synctable_without_table(self, storage_mode):
        """
        从集群没有与主集群有相同表名的表
        """
        name = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
                "name": name,
                "ttl": 0,
                "storage_mode": storage_mode,
                "table_partition": [
                    {"endpoint": self.leader,"pid_group": "0-2","is_leader": "true"},
                ],
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
        # leader cluster
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

        rs_up = self.ns_update_table_alive_cmd(self.ns_leader, 'updatetablealive', name, '*', self.leader, 'no')
        self.assertIn('update ok', rs_up)

        zk_root_path = '/remote'
        msg = self.ns_switch_mode(self.ns_leader, 'leader') 
        self.assertIn('switchmode ok', msg)
        msg = self.add_replica_cluster(self.ns_leader, conf.zk_endpoint, zk_root_path, self.alias)
        self.assertIn('adrepcluster ok', msg)
        time.sleep(10)
        
        (schema, column_key) = self.ns_showschema(self.ns_leader_r, name)
        self.assertEqual(len(schema), 0)
                
        msg = self.ns_synctable(self.ns_leader, name, self.alias, '1');
        self.assertIn('not need pid', msg)
        msg = self.ns_synctable(self.ns_leader, name, "ss");
        self.assertIn('replica name not found', msg)
        msg = self.ns_synctable(self.ns_leader, "ss", self.alias);
        self.assertIn('table is not exist', msg)
        msg = self.ns_synctable(self.ns_leader, name, self.alias);
        self.assertIn('local table has a no alive leader partition', msg)

        msg = self.ns_update_table_alive_cmd(self.ns_leader, 'updatetablealive', name, '*', self.leader, 'yes')
        self.assertIn('update ok', msg)
        msg = self.ns_synctable(self.ns_leader, name, self.alias);
        self.assertIn('synctable ok', msg)
        time.sleep(20)

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

    @ddt.data(
        ['kMemory'],
        ['kSSD'],
        ['kHDD'],
    )
    @ddt.unpack
    def test_synctable_with_table(self, storage_mode):
        """
        从集群有与主集群有可以match的表
        """
        name = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
                "name": name,
                "ttl": 0,
                "storage_mode": storage_mode,
                "table_partition": [
                    {"endpoint": self.leader,"pid_group": "0-2","is_leader": "true"},
                ],
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
        # leader cluster
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
        
        # follower cluster
        metadata_path_r = '{}/metadata.txt'.format(self.testpath)
        table_meta_r = {
                "name": name,
                "ttl": 0,
                "storage_mode": storage_mode,
                "table_partition": [
                    {"endpoint": self.leader_r,"pid_group": "0-2","is_leader": "true"},
                ],
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

        utils.gen_table_meta_file(table_meta_r, metadata_path_r)
        rs = self.ns_create(self.ns_leader_r, metadata_path)
        self.assertIn('Create table ok', rs)
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

        msg = self.ns_update_table_alive_cmd(self.ns_leader, 'updatetablealive', name, '*', self.leader, 'no')
        self.assertIn('update ok', msg)

        zk_root_path = '/remote'
        msg = self.ns_switch_mode(self.ns_leader, 'leader') 
        self.assertIn('switchmode ok', msg)
        msg = self.add_replica_cluster(self.ns_leader, conf.zk_endpoint, zk_root_path, self.alias)
        self.assertIn('adrepcluster ok', msg)
        time.sleep(10)
        
        rs4 = self.ns_scan_multi_with_pair(self.ns_leader_r, name, 'card0', 'card',  '25', '0', 'ts1')
        self.assertEqual(len(rs4), 0)
        rs5 = self.ns_scan_multi_with_pair(self.ns_leader_r, name, 'card0', 'card',  '25', '0', 'ts1', '1')
        self.assertEqual(len(rs5), 0)
        rs7 = self.ns_scan_multi_with_pair(self.ns_leader_r, name, 'mcc1', 'mcc',  '25', '0', 'ts2', '1')
        self.assertEqual(len(rs7), 0)

        msg = self.ns_synctable(self.ns_leader, name, self.alias);
        self.assertIn('local table has a no alive leader partition', msg)
        msg = self.ns_update_table_alive_cmd(self.ns_leader, 'updatetablealive', name, '*', self.leader, 'yes')
        self.assertIn('update ok', msg)
        msg = self.ns_update_table_alive_cmd(self.ns_leader_r, 'updatetablealive', name, '*', self.leader_r, 'no')
        self.assertIn('update ok', msg)
        msg = self.ns_synctable(self.ns_leader, name, self.alias);
        self.assertIn('remote table has a no alive leader partition', msg)
        msg = self.ns_update_table_alive_cmd(self.ns_leader_r, 'updatetablealive', name, '*', self.leader_r, 'yes')
        self.assertIn('update ok', msg)
        msg = self.ns_synctable(self.ns_leader, name, self.alias);
        self.assertIn('synctable ok', msg)
        time.sleep(10)

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
    @ddt.unpack
    def test_synctable_with_table_has_pid(self, storage_mode):
        """
        从集群有与主集群有可以match的表(同步某个分片)
        """
        name = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
                "name": name,
                "ttl": 0,
                "storage_mode": storage_mode,
                "table_partition": [
                    {"endpoint": self.leader,"pid_group": "0","is_leader": "true"},
                ],
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
        # leader cluster
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
        
        # follower cluster
        metadata_path_r = '{}/metadata.txt'.format(self.testpath)
        table_meta_r = {
                "name": name,
                "ttl": 0,
                "storage_mode": storage_mode,
                "table_partition": [
                    {"endpoint": self.leader_r,"pid_group": "0","is_leader": "true"},
                ],
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

        utils.gen_table_meta_file(table_meta_r, metadata_path_r)
        rs = self.ns_create(self.ns_leader_r, metadata_path)
        self.assertIn('Create table ok', rs)
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

        msg = self.ns_update_table_alive_cmd(self.ns_leader, 'updatetablealive', name, '*', self.leader, 'no')
        self.assertIn('update ok', msg)

        zk_root_path = '/remote'
        msg = self.ns_switch_mode(self.ns_leader, 'leader') 
        self.assertIn('switchmode ok', msg)
        msg = self.add_replica_cluster(self.ns_leader, conf.zk_endpoint, zk_root_path, self.alias)
        self.assertIn('adrepcluster ok', msg)
        time.sleep(10)
        
        rs4 = self.ns_scan_multi_with_pair(self.ns_leader_r, name, 'card0', 'card',  '25', '0', 'ts1')
        self.assertEqual(len(rs4), 0)
        rs5 = self.ns_scan_multi_with_pair(self.ns_leader_r, name, 'card0', 'card',  '25', '0', 'ts1', '1')
        self.assertEqual(len(rs5), 0)
        rs7 = self.ns_scan_multi_with_pair(self.ns_leader_r, name, 'mcc1', 'mcc',  '25', '0', 'ts2', '1')
        self.assertEqual(len(rs7), 0)

        msg = self.ns_synctable(self.ns_leader, name, self.alias, '0');
        self.assertIn('local table has a no alive leader partition', msg)
        msg = self.ns_update_table_alive_cmd(self.ns_leader, 'updatetablealive', name, '*', self.leader, 'yes')
        self.assertIn('update ok', msg)
        msg = self.ns_synctable(self.ns_leader, name, self.alias, '1');
        self.assertIn('table has no current pid', msg)
        msg = self.ns_update_table_alive_cmd(self.ns_leader_r, 'updatetablealive', name, '*', self.leader_r, 'no')
        self.assertIn('update ok', msg)
        msg = self.ns_synctable(self.ns_leader, name, self.alias, '0');
        self.assertIn('remote table has a no alive leader partition', msg)
        msg = self.ns_update_table_alive_cmd(self.ns_leader_r, 'updatetablealive', name, '*', self.leader_r, 'yes')
        self.assertIn('update ok', msg)
        msg = self.ns_synctable(self.ns_leader, name, self.alias, '0');
        self.assertIn('synctable ok', msg)
        time.sleep(10)

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
    @ddt.unpack
    def test_second_add_rep_cluster(self, storage_mode):
        """
        主从集群建连、同步、断连、建连，依然能够同步数据 
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
                "partition_num": 3,
                "replica_num": 1,
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
        time.sleep(20)

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

        msg = self.remove_replica_cluster(self.ns_leader, self.alias)
        self.assertIn('remove replica cluster ok', msg)
        time.sleep(5)
        rs1 = self.ns_put_multi_with_pair(self.ns_leader,name, ['card0', 'mcc1', '2', '17', '21'])
        self.assertIn('Put ok', rs1)
        rs4 = self.ns_scan_multi_with_pair(self.ns_leader, name, 'card0', 'card',  '25', '0', 'ts1')
        self.assertEqual(len(rs4),3)
        self.assertEqual(rs4[1]['card'], 'card0')
        self.assertEqual(rs4[1]['mcc'], 'mcc1')
        self.assertEqual(rs4[1]['amt'], '2')
        self.assertEqual(rs4[1]['ts1'], '16')
        self.assertEqual(rs4[1]['ts2'], '19')
        self.assertEqual(rs4[2]['card'], 'card0')
        self.assertEqual(rs4[2]['mcc'], 'mcc0')
        self.assertEqual(rs4[2]['amt'], '1')
        self.assertEqual(rs4[2]['ts1'], '15')
        self.assertEqual(rs4[2]['ts2'], '18')
        self.assertEqual(rs4[0]['card'], 'card0')
        self.assertEqual(rs4[0]['mcc'], 'mcc1')
        self.assertEqual(rs4[0]['amt'], '2')
        self.assertEqual(rs4[0]['ts1'], '17')
        self.assertEqual(rs4[0]['ts2'], '21')
        rs5 = self.ns_scan_multi_with_pair(self.ns_leader, name, 'card0', 'card',  '25', '0', 'ts1', '1')
        self.assertEqual(len(rs5),1)
        self.assertEqual(rs5[0]['card'], 'card0')
        self.assertEqual(rs5[0]['mcc'], 'mcc1')
        self.assertEqual(rs5[0]['amt'], '2')
        self.assertEqual(rs5[0]['ts1'], '17')
        self.assertEqual(rs5[0]['ts2'], '21')
        rs6 = self.ns_scan_multi_with_pair(self.ns_leader, name, 'mcc1', 'mcc',  '25', '0', 'ts1', '1')
        self.assertEqual(len(rs6), 0)
        rs7 = self.ns_scan_multi_with_pair(self.ns_leader, name, 'mcc1', 'mcc',  '25', '0', 'ts2')
        self.assertEqual(len(rs7), 2)
        time.sleep(3);
        
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
        rs7 = self.ns_scan_multi_with_pair(self.ns_leader_r, name, 'mcc1', 'mcc',  '25', '0', 'ts2')
        self.assertEqual(len(rs7), 1)

        msg = self.add_replica_cluster(self.ns_leader, conf.zk_endpoint, zk_root_path, self.alias)
        self.assertIn('adrepcluster ok', msg)
        time.sleep(10)

        rs4 = self.ns_scan_multi_with_pair(self.ns_leader_r, name, 'card0', 'card',  '25', '0', 'ts1')
        self.assertEqual(len(rs4),3)
        self.assertEqual(rs4[1]['card'], 'card0')
        self.assertEqual(rs4[1]['mcc'], 'mcc1')
        self.assertEqual(rs4[1]['amt'], '2')
        self.assertEqual(rs4[1]['ts1'], '16')
        self.assertEqual(rs4[1]['ts2'], '19')
        self.assertEqual(rs4[2]['card'], 'card0')
        self.assertEqual(rs4[2]['mcc'], 'mcc0')
        self.assertEqual(rs4[2]['amt'], '1')
        self.assertEqual(rs4[2]['ts1'], '15')
        self.assertEqual(rs4[2]['ts2'], '18')
        self.assertEqual(rs4[0]['card'], 'card0')
        self.assertEqual(rs4[0]['mcc'], 'mcc1')
        self.assertEqual(rs4[0]['amt'], '2')
        self.assertEqual(rs4[0]['ts1'], '17')
        self.assertEqual(rs4[0]['ts2'], '21')
        rs5 = self.ns_scan_multi_with_pair(self.ns_leader_r, name, 'card0', 'card',  '25', '0', 'ts1', '1')
        self.assertEqual(len(rs5),1)
        self.assertEqual(rs5[0]['card'], 'card0')
        self.assertEqual(rs5[0]['mcc'], 'mcc1')
        self.assertEqual(rs5[0]['amt'], '2')
        self.assertEqual(rs5[0]['ts1'], '17')
        self.assertEqual(rs5[0]['ts2'], '21')
        rs6 = self.ns_scan_multi_with_pair(self.ns_leader_r, name, 'mcc1', 'mcc',  '25', '0', 'ts1', '1')
        self.assertEqual(len(rs6), 0)
        rs7 = self.ns_scan_multi_with_pair(self.ns_leader_r, name, 'mcc1', 'mcc',  '25', '0', 'ts2')
        self.assertEqual(len(rs7), 2)

        msg = self.ns_drop(self.ns_leader, name)
        self.assertIn('drop ok', msg)
        time.sleep(3)
        rs = self.showtable(self.ns_leader_r, name)
        self.assertEqual(0, len(rs))
  

if __name__ == "__main__":
    load(TestMultiCluster)
