# -*- coding: utf-8 -*-
from testcasebase import TestCaseBase
import time
from libs.test_loader import load
import libs.utils as utils
from libs.deco import *


class TestMakeSnapshotNsClient(TestCaseBase):

    def test_makesnapshot_normal_success(self):
        """
        makesnapshot功能正常，op是kMakeSnapshotOP
        :return:
        """
        db_path='ssd_db'
        self.clear_ns_table(self.ns_leader)
        old_last_op_id = max(self.showopstatus(self.ns_leader).keys()) if self.showopstatus(self.ns_leader) != {} else 1
        name = 't{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)

        pid_group = '"0-2"'
        m = utils.gen_table_metadata_ssd(
            '"{}"'.format(name), None, 144000, 8,'kSSD',
            ('table_partition', '"{}"'.format(self.leader), pid_group, 'true'),
            ('table_partition', '"{}"'.format(self.slave1), pid_group, 'false'),
            ('table_partition', '"{}"'.format(self.slave2), pid_group, 'false'),
            ('column_desc', '"k1"', '"string"', 'true'),
            ('column_desc', '"k2"', '"string"', 'false'),
            ('column_desc', '"k3"', '"string"', 'true'))
        utils.gen_table_metadata_file(m, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs)

        table_info = self.showtable(self.ns_leader, name)
        tid = table_info.keys()[0][1]
        pid = table_info.keys()[0][2]

        self.put(self.leader, tid, pid, 'testkey0', self.now(), 'testvalue0')

        rs3 = self.makesnapshot(self.ns_leader, name, pid, 'ns_client')
        self.assertIn('MakeSnapshot ok', rs3)
        time.sleep(2)
        mf = self.get_manifest_by_realpath(self.leaderpath + "/" + db_path, tid, pid)
        # mf = self.get_manifest(self.leaderpath, tid, pid)
        self.assertEqual(mf['offset'], '1')
        self.assertTrue(mf['name'])
        self.assertEqual(mf['count'], '1')
        last_op_id = max(self.showopstatus(self.ns_leader).keys())
        self.assertFalse(old_last_op_id == last_op_id)
        last_opstatus = self.showopstatus(self.ns_leader)[last_op_id]
        self.assertIn('kMakeSnapshotOP', last_opstatus)
        self.clear_ns_table(self.ns_leader)

    def test_makesnapshot_expired(self):
        """
        数据全部过期后, termid正常
        makesnapshot功能正常，op是kMakeSnapshotOP
        :return:
        """
        self.clear_ns_table(self.ns_leader)
        old_last_op_id = max(self.showopstatus(self.ns_leader).keys()) if self.showopstatus(self.ns_leader) != {} else 1
        name = 't{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)

        pid_group = '"0"'
        # m = utils.gen_table_metadata(
        #     '"{}"'.format(name), None, 1, 8,
        #     ('table_partition', '"{}"'.format(self.leader), pid_group, 'true'),
        #     ('table_partition', '"{}"'.format(self.slave1), pid_group, 'false'),
        #     ('table_partition', '"{}"'.format(self.slave2), pid_group, 'false'),
        #     ('column_desc', '"k1"', '"string"', 'true'),
        #     ('column_desc', '"k2"', '"string"', 'false'),
        #     ('column_desc', '"k3"', '"string"', 'true'))
        # utils.gen_table_metadata_file(m, metadata_path)

        table_meta = {
            "name": name,
            "ttl": 1,
            "storage_mode": "kSSD",
            "table_partition": [
                {"endpoint": self.leader,"pid_group": pid_group,"is_leader": "true"},
                {"endpoint": self.slave1,"pid_group": pid_group,"is_leader": "false"},
                {"endpoint": self.slave2,"pid_group": pid_group,"is_leader": "false"},
            ],
            "column_desc":[
                {"name": "k1", "type": "string", "add_ts_idx": "true"},
                {"name": "k2", "type": "string", "add_ts_idx": "false"},
                {"name": "k3", "type": "string", "add_ts_idx": "true"},
            ],
        }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs)

        table_info = self.showtable(self.ns_leader, name)
        tid = table_info.keys()[0][1]
        pid = '0'

        self.put(self.leader, tid, pid, 'testkey0', self.now() - 120000, 'testvalue0')
        self.assertFalse("0" == "1")

        rs3 = self.makesnapshot(self.ns_leader, name, pid, 'ns_client')
        self.assertIn('MakeSnapshot ok', rs3)
        time.sleep(2)

        mf = self.get_manifest(self.leaderpath, tid, pid)
        self.assertEqual(mf['offset'], '1')
        self.assertTrue(mf['name'])
        self.assertEqual(mf['count'], '0')
        self.assertFalse(mf['term'] == '0')
        last_op_id = max(self.showopstatus(self.ns_leader).keys())
        self.assertFalse(old_last_op_id == last_op_id)
        last_opstatus = self.showopstatus(self.ns_leader)[last_op_id]
        self.assertIn('kMakeSnapshotOP', last_opstatus)
        self.ns_drop(self.ns_leader, name)


    def test_makesnapshot_name_notexist(self):
        """
        name不存在时，makesnapshot失败
        :return:
        """
        name = 't{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        # m = utils.gen_table_metadata(
        #     '"{}"'.format(name), None, 144000, 8,
        #     ('table_partition', '"{}"'.format(self.leader), '"0-3"', 'true'),
        #     ('table_partition', '"{}"'.format(self.slave1), '"0-3"', 'false'),
        #     ('table_partition', '"{}"'.format(self.slave2), '"0-3"', 'false'),
        #     ('column_desc', '"k1"', '"string"', 'true'),
        #     ('column_desc', '"k2"', '"string"', 'false'),
        #     ('column_desc', '"k3"', '"string"', 'true'))
        # utils.gen_table_metadata_file(m, metadata_path)

        table_meta = {
            "name": name,
            "ttl": 144000,
            "storage_mode": "kSSD",
            "table_partition": [
                {"endpoint": self.leader,"pid_group": "0-3","is_leader": "true"},
                {"endpoint": self.slave1,"pid_group": "0-3","is_leader": "false"},
                {"endpoint": self.slave2,"pid_group": "0-3","is_leader": "false"},
            ],
            "column_desc":[
                {"name": "k1", "type": "string", "add_ts_idx": "true"},
                {"name": "k2", "type": "string", "add_ts_idx": "false"},
                {"name": "k3", "type": "string", "add_ts_idx": "true"},
            ],
        }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs)
        table_info = self.showtable(self.ns_leader, name)
        tid = table_info.keys()[0][1]
        pid = table_info.keys()[0][2]
        self.put(self.leader, tid, pid, 'testkey0', self.now(), 'testvalue0')
        rs3 = self.makesnapshot(self.ns_leader, name + 'aaa', 2, 'ns_client')
        self.assertIn('Fail to makesnapshot', rs3)
        self.ns_drop(self.ns_leader, name)


    def test_makesnapshot_pid_notexist(self):
        """
        pid不存在时，makesnapshot失败
        :return:
        """
        name = 't{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        # m = utils.gen_table_metadata(
        #     '"{}"'.format(name), None, 144000, 8,
        #     ('table_partition', '"{}"'.format(self.leader), '"0-3"', 'true'),
        #     ('table_partition', '"{}"'.format(self.slave1), '"0-3"', 'false'),
        #     ('table_partition', '"{}"'.format(self.slave2), '"0-3"', 'false'),
        #     ('column_desc', '"k1"', '"string"', 'true'),
        #     ('column_desc', '"k2"', '"string"', 'false'),
        #     ('column_desc', '"k3"', '"string"', 'true'))
        # utils.gen_table_metadata_file(m, metadata_path)

        table_meta = {
            "name": name,
            "ttl": 144000,
            "storage_mode": "kSSD",
            "table_partition": [
                {"endpoint": self.leader,"pid_group": "0-3","is_leader": "true"},
                {"endpoint": self.slave1,"pid_group": "0-3","is_leader": "false"},
                {"endpoint": self.slave2,"pid_group": "0-3","is_leader": "false"},
            ],
            "column_desc":[
                {"name": "k1", "type": "string", "add_ts_idx": "true"},
                {"name": "k2", "type": "string", "add_ts_idx": "false"},
                {"name": "k3", "type": "string", "add_ts_idx": "true"},
            ],
        }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs)
        table_info = self.showtable(self.ns_leader, name)
        tid = table_info.keys()[0][1]
        pid = table_info.keys()[0][2]
        self.put(self.leader, tid, pid, 'testkey0', self.now(), 'testvalue0')
        rs3 = self.makesnapshot(self.ns_leader, name, 4, 'ns_client')
        self.assertIn('Fail to makesnapshot', rs3)
        self.ns_drop(self.ns_leader, name)


    def test_changeleader_and_makesnapshot(self):
        """
        changeleader后，可以makesnapshot，未changeleader的无法makesnapshot
        :return:
        """
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        name = 'tname{}'.format(time.time())
        # m = utils.gen_table_metadata(
        #     '"{}"'.format(name), None, 144000, 2,
        #     ('table_partition', '"{}"'.format(self.leader), '"0-2"', 'true'),
        #     ('table_partition', '"{}"'.format(self.slave1), '"0-2"', 'false'),
        #     ('column_desc', '"merchant"', '"string"', 'true'),
        #     ('column_desc', '"amt"', '"double"', 'false'),
        #     ('column_desc', '"card"', '"string"', 'true'),
        # )
        # utils.gen_table_metadata_file(m, metadata_path)

        table_meta = {
            "name": name,
            "ttl": 144000,
            "storage_mode": "kSSD",
            "table_partition": [
                {"endpoint": self.leader,"pid_group": "0-2","is_leader": "true"},
                {"endpoint": self.slave1,"pid_group": "0-2","is_leader": "false"},
            ],
            "column_desc":[
                {"name": "merchant", "type": "string", "add_ts_idx": "true"},
                {"name": "amt", "type": "double", "add_ts_idx": "false"},
                {"name": "card", "type": "string", "add_ts_idx": "true"},
            ],
        }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs0 = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs0)

        rs1 = self.showtable(self.ns_leader, name)
        tid = rs1.keys()[0][1]

        self.put(self.leader, tid, 0, 'testkey0', self.now(), 'testvalue0')

        self.stop_client(self.leader)
        self.updatetablealive(self.ns_leader, name, '*', self.leader, 'no')
        time.sleep(10)

        self.changeleader(self.ns_leader, name, 0)
        time.sleep(2)

        rs2 = self.showtable(self.ns_leader, name)
        rs3 = self.makesnapshot(self.ns_leader, name, 0, 'ns_client')
        rs4 = self.makesnapshot(self.ns_leader, name, 1, 'ns_client')
        self.start_client(self.leader)
        time.sleep(10)
        self.assertEqual(rs2[(name, tid, '0', self.leader)], ['leader', '144000min', 'no', 'kNoCompress'])
        self.assertEqual(rs2[(name, tid, '1', self.leader)], ['leader', '144000min', 'no', 'kNoCompress'])
        self.assertEqual(rs2[(name, tid, '2', self.leader)], ['leader', '144000min', 'no', 'kNoCompress'])
        self.assertEqual(rs2[(name, tid, '0', self.slave1)], ['leader', '144000min', 'yes', 'kNoCompress'])
        self.assertEqual(rs2[(name, tid, '1', self.slave1)], ['follower', '144000min', 'yes', 'kNoCompress'])
        self.assertEqual(rs2[(name, tid, '2', self.slave1)], ['follower', '144000min', 'yes', 'kNoCompress'])

        self.assertIn('MakeSnapshot ok', rs3)
        self.assertIn('Fail to makesnapshot', rs4)
        mf = self.get_manifest(self.slave1path, tid, 0)
        self.assertEqual(mf['offset'], '1')
        self.assertTrue(mf['name'])
        self.assertEqual(mf['count'], '1')
        self.ns_drop(self.ns_leader, name)

    def test_one_ts(self):
        name = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
                "name": name,
                "ttl": 10,
                "partition_num": 1,
                "replica_num": 1,
                "storage_mode": "kSSD",
                "column_desc":[
                    {"name": "card", "type": "string", "add_ts_idx": "true"},
                    {"name": "mcc", "type": "string", "add_ts_idx": "false"},
                    {"name": "amt", "type": "double", "add_ts_idx": "false"},
                    {"name": "ts", "type": "int64", "add_ts_idx": "false", "is_ts_col": "true"},
                    ]
                }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs)
        curtime = int(time.time() * 1000)
        self.ns_put_multi(self.ns_leader, name, '', ['card0', 'mcc0', '1.1', str(curtime)])
        self.ns_put_multi(self.ns_leader, name, '', ['card0', 'mcc1', '1.2', str(curtime - 10)])
        self.ns_put_multi(self.ns_leader, name, '', ['card1', 'mcc2', '1.3', str(curtime - 20)])
        self.ns_put_multi(self.ns_leader, name, '', ['card1', 'mcc3', '1.4', str(curtime - 30)])
        self.ns_put_multi(self.ns_leader, name, '', ['card2', 'mcc4', '1.5', str(curtime - 40)])
        self.ns_put_multi(self.ns_leader, name, '', ['card2', 'mcc5', '1.6', str(curtime - 10*60*1000 - 10)])
        self.ns_put_multi(self.ns_leader, name, '', ['card2', 'mcc6', '1.7', str(curtime - 10*60*1000 - 20)])
        rs3 = self.makesnapshot(self.ns_leader, name, 0, 'ns_client')

        table_info = self.showtable(self.ns_leader, name)
        tid = table_info.keys()[0][1]

        rs3 = self.makesnapshot(self.ns_leader, name, '0', 'ns_client')
        self.assertIn('MakeSnapshot ok', rs3)
        mf = self.get_manifest(self.leaderpath, tid, 0)
        self.assertEqual(mf['offset'], '7')
        self.assertEqual(mf['count'], '5')
        self.ns_drop(self.ns_leader, name)

    def test_one_ts(self):
        name = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
                "name": name,
                "ttl": 10,
                "partition_num": 1,
                "storage_mode": "kSSD",
                "replica_num": 1,
                "column_desc":[
                    {"name": "card", "type": "string", "add_ts_idx": "true"},
                    {"name": "mcc", "type": "string", "add_ts_idx": "false"},
                    {"name": "amt", "type": "double", "add_ts_idx": "false"},
                    {"name": "ts", "type": "int64", "add_ts_idx": "false", "is_ts_col": "true"},
                    ]
                }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs)
        curtime = int(time.time() * 1000)
        self.ns_put_multi(self.ns_leader, name, '', ['card0', 'mcc0', '1.1', str(curtime)])
        self.ns_put_multi(self.ns_leader, name, '', ['card0', 'mcc1', '1.2', str(curtime - 10)])
        self.ns_put_multi(self.ns_leader, name, '', ['card1', 'mcc2', '1.3', str(curtime - 20)])
        self.ns_put_multi(self.ns_leader, name, '', ['card1', 'mcc3', '1.4', str(curtime - 30)])
        self.ns_put_multi(self.ns_leader, name, '', ['card2', 'mcc4', '1.5', str(curtime - 40)])
        self.ns_put_multi(self.ns_leader, name, '', ['card2', 'mcc5', '1.6', str(curtime - 10*60*1000 - 10)])
        self.ns_put_multi(self.ns_leader, name, '', ['card2', 'mcc6', '1.7', str(curtime - 10*60*1000 - 20)])
        rs3 = self.makesnapshot(self.ns_leader, name, 0, 'ns_client')

        table_info = self.showtable(self.ns_leader, name)
        tid = table_info.keys()[0][1]

        rs3 = self.makesnapshot(self.ns_leader, name, '0', 'ns_client')
        self.assertIn('MakeSnapshot ok', rs3)
        mf = self.get_manifest(self.leaderpath, tid, 0)
        self.assertEqual(mf['offset'], '7')
        self.assertEqual(mf['count'], '5')

    def test_two_ts(self):
        name = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
                "name": name,
                "ttl": 10,
                "partition_num": 1,
                "replica_num": 1,
                "storage_mode": "kSSD",
                "column_desc":[
                    {"name": "card", "type": "string", "add_ts_idx": "true"},
                    {"name": "mcc", "type": "string", "add_ts_idx": "true"},
                    {"name": "amt", "type": "double", "add_ts_idx": "false"},
                    {"name": "ts", "type": "int64", "add_ts_idx": "false", "is_ts_col": "true"},
                    {"name": "ts1", "type": "int64", "add_ts_idx": "false", "is_ts_col": "true"},
                    ],
		"column_key":[
                    {"index_name":"card", "ts_name":["ts", "ts1"]},
                    {"index_name":"mcc", "ts_name":["ts1"]},
                    ]
                }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs)
        curtime = int(time.time() * 1000)
        self.ns_put_multi(self.ns_leader, name, '', ['card0', 'mcc0', '1.1', str(curtime), str(curtime - 10)])
        self.ns_put_multi(self.ns_leader, name, '', ['card0', 'mcc1', '1.2', str(curtime - 10), str(curtime)])
        self.ns_put_multi(self.ns_leader, name, '', ['card1', 'mcc2', '1.3', str(curtime), str(curtime - 10*60*1000 - 10)])
        self.ns_put_multi(self.ns_leader, name, '', ['card1', 'mcc3', '1.4', str(curtime - 10*60*1000 - 10), str(curtime - 30)])
        self.ns_put_multi(self.ns_leader, name, '', ['card2', 'mcc5', '1.6', str(curtime - 10*60*1000 - 10), str(curtime - 10*60*1000 - 20)])
        self.ns_put_multi(self.ns_leader, name, '', ['card0', 'mcc2', '1.7', str(curtime - 10*60*1000 - 20), str(curtime - 10*60*1000 - 10)])
        self.ns_put_multi(self.ns_leader, name, '', ['card3', 'mcc6', '1.7', str(curtime - 10*60*1000 - 20), str(curtime - 10*60*1000 - 10)])
        rs3 = self.makesnapshot(self.ns_leader, name, 0, 'ns_client')

        table_info = self.showtable(self.ns_leader, name)
        tid = table_info.keys()[0][1]

        rs3 = self.makesnapshot(self.ns_leader, name, '0', 'ns_client')
        self.assertIn('MakeSnapshot ok', rs3)
        mf = self.get_manifest(self.leaderpath, tid, 0)
        self.assertEqual(mf['offset'], '7')
        self.assertEqual(mf['count'], '4')

    def test_two_ts_1(self):
        name = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
                "name": name,
                "ttl": 10,
                "partition_num": 1,
                "replica_num": 1,
            "storage_mode": "kSSD",
                "column_desc":[
                    {"name": "card", "type": "string", "add_ts_idx": "true"},
                    {"name": "mcc", "type": "string", "add_ts_idx": "true"},
                    {"name": "amt", "type": "double", "add_ts_idx": "false"},
                    {"name": "ts", "type": "int64", "add_ts_idx": "false", "is_ts_col": "true"},
                    {"name": "ts1", "type": "int64", "add_ts_idx": "false", "is_ts_col": "true"},
                    ],
		"column_key":[
                    {"index_name":"card_mcc", "col_name": ["card", "mcc"], "ts_name":["ts"]},
                    {"index_name":"mcc", "ts_name":["ts"]},
                    ]
                }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs)
        curtime = int(time.time() * 1000)
        self.ns_put_multi(self.ns_leader, name, '', ['card0', 'mcc0', '1.1', str(curtime), str(curtime - 10)])
        self.ns_put_multi(self.ns_leader, name, '', ['card0', 'mcc1', '1.2', str(curtime - 10), str(curtime)])
        self.ns_put_multi(self.ns_leader, name, '', ['card1', 'mcc2', '1.3', str(curtime), str(curtime - 10*60*1000 - 10)])
        self.ns_put_multi(self.ns_leader, name, '', ['card1', 'mcc3', '1.4', str(curtime - 10*60*1000 - 10), str(curtime - 30)])
        self.ns_put_multi(self.ns_leader, name, '', ['card2', 'mcc5', '1.6', str(curtime - 10*60*1000 - 10), str(curtime - 10*60*1000 - 20)])
        self.ns_put_multi(self.ns_leader, name, '', ['card0', 'mcc2', '1.7', str(curtime - 10*60*1000 - 20), str(curtime - 10*60*1000 - 10)])
        self.ns_put_multi(self.ns_leader, name, '', ['card3', 'mcc6', '1.7', str(curtime - 10*60*1000 - 20), str(curtime - 10*60*1000 - 10)])
        rs3 = self.makesnapshot(self.ns_leader, name, 0, 'ns_client')

        table_info = self.showtable(self.ns_leader, name)
        tid = table_info.keys()[0][1]

        rs3 = self.makesnapshot(self.ns_leader, name, '0', 'ns_client')
        self.assertIn('MakeSnapshot ok', rs3)
        mf = self.get_manifest(self.leaderpath, tid, 0)
        self.assertEqual(mf['offset'], '7')
        self.assertEqual(mf['count'], '4')

    def test_two_ts_ttl(self):
        name = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
                "name": name,
                "ttl": 10,
                "partition_num": 1,
                "replica_num": 1,
            "storage_mode": "kSSD",
                "column_desc":[
                    {"name": "card", "type": "string", "add_ts_idx": "true"},
                    {"name": "mcc", "type": "string", "add_ts_idx": "true"},
                    {"name": "amt", "type": "double", "add_ts_idx": "false"},
                    {"name": "ts", "type": "int64", "add_ts_idx": "false", "is_ts_col": "true"},
                    {"name": "ts1", "type": "int64", "add_ts_idx": "false", "is_ts_col": "true", "ttl": 20},
                    ],
		"column_key":[
                    {"index_name":"card_mcc", "col_name": ["card", "mcc"], "ts_name":["ts", "ts1"]},
                    {"index_name":"mcc", "ts_name":["ts"]},
                    ]
                }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs)
        curtime = int(time.time() * 1000)
        self.ns_put_multi(self.ns_leader, name, '', ['card0', 'mcc0', '1.1', str(curtime), str(curtime - 10)])
        self.ns_put_multi(self.ns_leader, name, '', ['card0', 'mcc1', '1.2', str(curtime - 10), str(curtime)])
        self.ns_put_multi(self.ns_leader, name, '', ['card1', 'mcc2', '1.3', str(curtime), str(curtime - 10*60*1000 - 10)])
        self.ns_put_multi(self.ns_leader, name, '', ['card1', 'mcc3', '1.4', str(curtime - 10*60*1000 - 10), str(curtime - 30)])
        self.ns_put_multi(self.ns_leader, name, '', ['card2', 'mcc5', '1.6', str(curtime - 10*60*1000 - 10), str(curtime - 20*60*1000 - 20)])
        self.ns_put_multi(self.ns_leader, name, '', ['card0', 'mcc2', '1.7', str(curtime - 20*60*1000 - 20), str(curtime - 10*60*1000 - 10)])
        self.ns_put_multi(self.ns_leader, name, '', ['card3', 'mcc6', '1.7', str(curtime - 20*60*1000 - 20), str(curtime - 20*60*1000 - 10)])
        rs3 = self.makesnapshot(self.ns_leader, name, 0, 'ns_client')

        table_info = self.showtable(self.ns_leader, name)
        tid = table_info.keys()[0][1]

        rs3 = self.makesnapshot(self.ns_leader, name, '0', 'ns_client')
        self.assertIn('MakeSnapshot ok', rs3)
        mf = self.get_manifest(self.leaderpath, tid, 0)
        self.assertEqual(mf['offset'], '7')
        self.assertEqual(mf['count'], '5')

    def test_one_ts_latest(self):
        name = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
                "name": name,
                "ttl": 2,
                "ttl_type": "kLatestTime",
                "partition_num": 1,
                "replica_num": 1,
            "storage_mode": "kSSD",
                "column_desc":[
                    {"name": "card", "type": "string", "add_ts_idx": "true"},
                    {"name": "mcc", "type": "string", "add_ts_idx": "true"},
                    {"name": "amt", "type": "double", "add_ts_idx": "false"},
                    {"name": "ts", "type": "int64", "add_ts_idx": "false", "is_ts_col": "true"},
                    ],
		"column_key":[
                    {"index_name":"card", "ts_name":["ts"]},
                    ]
                }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs)
        curtime = int(time.time() * 1000)
        self.ns_put_multi(self.ns_leader, name, '', ['card0', 'mcc0', '1.1', '1'])
        self.ns_put_multi(self.ns_leader, name, '', ['card0', 'mcc1', '1.2', '2'])
        self.ns_put_multi(self.ns_leader, name, '', ['card0', 'mcc2', '1.3', '3'])
        self.ns_put_multi(self.ns_leader, name, '', ['card1', 'mcc3', '1.4', '4'])
        self.ns_put_multi(self.ns_leader, name, '', ['card2', 'mcc5', '1.6', '5'])
        self.ns_put_multi(self.ns_leader, name, '', ['card2', 'mcc2', '1.7', '6'])
        self.ns_put_multi(self.ns_leader, name, '', ['card3', 'mcc6', '1.7', '7'])
        rs3 = self.makesnapshot(self.ns_leader, name, 0, 'ns_client')

        table_info = self.showtable(self.ns_leader, name)
        tid = table_info.keys()[0][1]
        tablet_endpoint = table_info.keys()[0][3]
        self.execute_gc(tablet_endpoint, tid, '0')

        rs3 = self.makesnapshot(self.ns_leader, name, '0', 'ns_client')
        self.assertIn('MakeSnapshot ok', rs3)
        mf = self.get_manifest(self.leaderpath, tid, 0)
        self.assertEqual(mf['offset'], '7')
        self.assertEqual(mf['count'], '6')

    def test_one_ts_two_index_latest(self):
        name = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
                "name": name,
                "ttl": 2,
                "ttl_type": "kLatestTime",
                "partition_num": 1,
                "replica_num": 1,
            "storage_mode": "kSSD",
                "column_desc":[
                    {"name": "card", "type": "string", "add_ts_idx": "true"},
                    {"name": "mcc", "type": "string", "add_ts_idx": "true"},
                    {"name": "amt", "type": "double", "add_ts_idx": "false"},
                    {"name": "ts", "type": "int64", "add_ts_idx": "false", "is_ts_col": "true"},
                    ],
		"column_key":[
                    {"index_name":"card", "ts_name":["ts"]},
                    {"index_name":"mcc", "ts_name":["ts"]},
                    ]
                }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs)
        curtime = int(time.time() * 1000)
        self.ns_put_multi(self.ns_leader, name, '', ['card0', 'mcc0', '1.1', '1'])
        self.ns_put_multi(self.ns_leader, name, '', ['card0', 'mcc0', '1.2', '2'])
        self.ns_put_multi(self.ns_leader, name, '', ['card0', 'mcc0', '1.3', '3'])
        self.ns_put_multi(self.ns_leader, name, '', ['card0', 'mcc1', '1.4', '4'])
        self.ns_put_multi(self.ns_leader, name, '', ['card2', 'mcc1', '1.6', '5'])
        self.ns_put_multi(self.ns_leader, name, '', ['card2', 'mcc1', '1.7', '6'])
        self.ns_put_multi(self.ns_leader, name, '', ['card3', 'mcc6', '1.7', '7'])
        rs3 = self.makesnapshot(self.ns_leader, name, 0, 'ns_client')

        table_info = self.showtable(self.ns_leader, name)
        tid = table_info.keys()[0][1]
        tablet_endpoint = table_info.keys()[0][3]
        self.execute_gc(tablet_endpoint, tid, '0')

        rs3 = self.makesnapshot(self.ns_leader, name, '0', 'ns_client')
        self.assertIn('MakeSnapshot ok', rs3)
        mf = self.get_manifest(self.leaderpath, tid, 0)
        self.assertEqual(mf['offset'], '7')
        self.assertEqual(mf['count'], '6')

    def test_two_ts_latest_ttl(self):
        name = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
                "name": name,
                "ttl": 2,
                "ttl_type": "kLatestTime",
                "partition_num": 1,
                "replica_num": 1,
            "storage_mode": "kSSD",
                "column_desc":[
                    {"name": "card", "type": "string", "add_ts_idx": "true"},
                    {"name": "mcc", "type": "string", "add_ts_idx": "true"},
                    {"name": "amt", "type": "double", "add_ts_idx": "false"},
                    {"name": "ts", "type": "int64", "add_ts_idx": "false", "is_ts_col": "true"},
                    {"name": "ts1", "type": "int64", "add_ts_idx": "false", "is_ts_col": "true", "ttl": 3},
                    ],
		"column_key":[
                    {"index_name":"card", "ts_name":["ts", "ts1"]},
                    ]
                }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs)
        curtime = int(time.time() * 1000)
        self.ns_put_multi(self.ns_leader, name, '', ['card0', 'mcc0', '1.1', '1', '11'])
        self.ns_put_multi(self.ns_leader, name, '', ['card0', 'mcc1', '1.2', '2', '12'])
        self.ns_put_multi(self.ns_leader, name, '', ['card0', 'mcc2', '1.3', '3', '13'])
        self.ns_put_multi(self.ns_leader, name, '', ['card0', 'mcc3', '1.4', '4', '14'])
        self.ns_put_multi(self.ns_leader, name, '', ['card2', 'mcc5', '1.6', '5', '15'])
        self.ns_put_multi(self.ns_leader, name, '', ['card2', 'mcc2', '1.7', '6', '16'])
        self.ns_put_multi(self.ns_leader, name, '', ['card2', 'mcc6', '1.7', '7', '17'])
        rs3 = self.makesnapshot(self.ns_leader, name, 0, 'ns_client')

        table_info = self.showtable(self.ns_leader, name)
        tid = table_info.keys()[0][1]
        tablet_endpoint = table_info.keys()[0][3]
        self.execute_gc(tablet_endpoint, tid, '0')

        rs3 = self.makesnapshot(self.ns_leader, name, '0', 'ns_client')
        self.assertIn('MakeSnapshot ok', rs3)
        mf = self.get_manifest(self.leaderpath, tid, 0)
        self.assertEqual(mf['offset'], '7')
        self.assertEqual(mf['count'], '6')

    def test_two_ts_two_index_latest_ttl(self):
        name = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
                "name": name,
                "ttl": 2,
                "ttl_type": "kLatestTime",
                "partition_num": 1,
                "replica_num": 1,
            "storage_mode": "kSSD",
                "column_desc":[
                    {"name": "card", "type": "string", "add_ts_idx": "true"},
                    {"name": "mcc", "type": "string", "add_ts_idx": "true"},
                    {"name": "amt", "type": "double", "add_ts_idx": "false"},
                    {"name": "ts", "type": "int64", "add_ts_idx": "false", "is_ts_col": "true"},
                    {"name": "ts1", "type": "int64", "add_ts_idx": "false", "is_ts_col": "true", "ttl": 3},
                    ],
		"column_key":[
                    {"index_name":"card", "ts_name":["ts", "ts1"]},
                    {"index_name":"mcc", "ts_name":["ts"]},
                    ]
                }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs)
        curtime = int(time.time() * 1000)
        self.ns_put_multi(self.ns_leader, name, '', ['card0', 'mcc0', '1.1', '1', '11'])
        self.ns_put_multi(self.ns_leader, name, '', ['card0', 'mcc1', '1.2', '2', '12'])
        self.ns_put_multi(self.ns_leader, name, '', ['card0', 'mcc2', '1.3', '3', '13'])
        self.ns_put_multi(self.ns_leader, name, '', ['card0', 'mcc3', '1.4', '4', '14'])
        self.ns_put_multi(self.ns_leader, name, '', ['card0', 'mcc0', '1.6', '5', '15'])
        self.ns_put_multi(self.ns_leader, name, '', ['card0', 'mcc0', '1.7', '6', '16'])
        self.ns_put_multi(self.ns_leader, name, '', ['card2', 'mcc1', '1.8', '7', '17'])
        self.ns_put_multi(self.ns_leader, name, '', ['card2', 'mcc1', '1.9', '8', '18'])
        self.ns_put_multi(self.ns_leader, name, '', ['card3', 'mcc4', '2.0', '9', '19'])
        rs3 = self.makesnapshot(self.ns_leader, name, 0, 'ns_client')

        table_info = self.showtable(self.ns_leader, name)
        tid = table_info.keys()[0][1]
        tablet_endpoint = table_info.keys()[0][3]
        self.execute_gc(tablet_endpoint, tid, '0')

        rs3 = self.makesnapshot(self.ns_leader, name, '0', 'ns_client')
        self.assertIn('MakeSnapshot ok', rs3)
        mf = self.get_manifest(self.leaderpath, tid, 0)
        self.assertEqual(mf['offset'], '9')
        self.assertEqual(mf['count'], '7')

if __name__ == "__main__":
    load(TestMakeSnapshotNsClient)
