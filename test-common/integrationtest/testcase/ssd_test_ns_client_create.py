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


@ddt.ddt
class TestCreateTableByNsClient(TestCaseBase):

    leader, slave1, slave2 = (i for i in conf.tb_endpoints)

    @ddt.data(
        ('t{}'.format(time.time()), None, 144000, 8,
         'Create table ok'),
        ('t{}'.format(time.time()), None, 157680000, 8,
         'Create failed. The max num of AbsoluteTime ttl is 15768000'),
        ('t{}'.format(time.time()), 'notype', 144000, 8,
         'ttl type notype is invalid'),    #
        ('t{}'.format(time.time()), '', 144000, 8,
         'ttl type  is invalid'),      #table meta file format error
        ('', None, 144000, 8,
         'Fail to create table'),
        ('t{}'.format(time.time()), None, -1, 8,
         'src/brpc/global.cpp:260] google/protobuf/text_format.cc:274 Error parsing text-format rtidb.client.TableInfo'),
        ('t{}'.format(time.time()), None, '', 8,
         'src/brpc/global.cpp:260] google/protobuf/text_format.cc:274 Error parsing text-format rtidb.client.TableInfo'),
        ('t{}'.format(time.time()), None, '"144000"', 8,
         'table meta file format error'),
        ('t{}'.format(time.time()), None, 144, -8,
         'src/brpc/global.cpp:260] google/protobuf/text_format.cc:274 Error parsing text-format rtidb.client.TableInfo'),
        ('t{}'.format(time.time()), None, 144, '',
         'src/brpc/global.cpp:260] google/protobuf/text_format.cc:274 Error parsing text-format rtidb.client.TableInfo'),
        ('t{}'.format(time.time()), None, 144, '8',
         'table meta file format error'),
        (None, None, 144000, 8,
         'Message missing required fields: name'),
        ('t{}'.format(time.time()), None, None, 8,
         'Message missing required fields: ttl'),
    )
    @ddt.unpack
    def test_create_name_ttltype_ttl_seg(self, name, ttl_type, ttl, seg_cnt, exp_msg):
        """
        name，ttp type，ttl和seg的参数检查
        :param ttl_type:
        :param name:
        :param seg_cnt:
        :param ttl:
        :param exp_msg:
        :return:
        """
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        # m = utils.gen_table_metadata(
        #     name, ttl_type, ttl, seg_cnt,
        #     ('table_partition', '"{}"'.format(self.leader), '"0-2"', 'true'),
        #     ('table_partition', '"{}"'.format(self.slave1), '"0-1"', 'false'),
        #     ('table_partition', '"{}"'.format(self.slave2), '"1-2"', 'false'),
        #     ('column_desc', '"k1"', '"string"', 'true'),
        #     ('column_desc', '"k2"', '"double"', 'false'),
        #     ('column_desc', '"k3"', '"int32"', 'true'),
        # )
        # utils.gen_table_metadata_file(m, metadata_path)

        table_meta = {
            "name": name,
            "ttl": ttl,
            "seg_cnt": seg_cnt,
            "ttl_type": ttl_type,
            "storage_mode": "kSSD",
            "table_partition": [
                {"endpoint": self.leader,"pid_group": "0-2","is_leader": "true"},
                {"endpoint": self.slave1,"pid_group": "0-1","is_leader": "false"},
                {"endpoint": self.slave2,"pid_group": "1-2","is_leader": "false"},
            ],
            "column_desc":[
                {"name": "k1", "type": "string", "add_ts_idx": "true"},
                {"name": "k2", "type": "double", "add_ts_idx": "false"},
                {"name": "k3", "type": "int32", "add_ts_idx": "true"},
            ],
        }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn(exp_msg, rs)

        d = {'k1': ('string:index', 'testvalue0'),
             'k2': ('double', 1.111),
             'k3': ('int32:index', -20)}
        self.multidimension_vk = collections.OrderedDict(sorted(d.items(), key = lambda t:t[0]))
        self.multidimension_scan_vk = {'k1': 'testvalue0'}

        if exp_msg == 'Create table ok':
            table_info = self.showtable(self.ns_leader, name)
            tid = table_info.keys()[0][1]
            pid = 1
            self.put(self.leader, tid, pid, 'testkey0', self.now() + 100, 'testvalue0')
            time.sleep(0.5)
            self.assertIn(
                'testvalue0', self.scan(self.slave1, tid, pid, 'testkey0', self.now(), 1))
        self.ns_drop(self.ns_leader, name)


    @multi_dimension(False)
    @ddt.data(
        ('"t{}"'.format(time.time()), '"kLatestTime"', 10, 8),
        ('"t{}"'.format(time.time()), '"kAbsoluteTime"', 1, 8),  # RTIDB-202
    )
    @ddt.unpack
    def test_create_ttl_type(self, name, ttl_type, ttl, seg_cnt):
        """
        两种ttltype，过期后的数据，get时直接不返回
        :param ttl_type:
        :param name:
        :param seg_cnt:
        :param ttl:
        :param exp_msg:
        :return:
        """
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        m = utils.gen_table_metadata_ssd(
            name, ttl_type, ttl, seg_cnt,'kSSD',
            ('table_partition', '"{}"'.format(self.leader), '"0-2"', 'true'),
            ('table_partition', '"{}"'.format(self.slave1), '"0-1"', 'false'),
            ('table_partition', '"{}"'.format(self.slave2), '"1-2"', 'false'))
        utils.gen_table_metadata_file(m, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs)

        table_info = self.showtable(self.ns_leader, name)
        tid = table_info.keys()[0][1]
        pid = 1
        ts = self.now() + 1000
        for _ in range(10):
            self.put(self.leader, tid, pid, 'testkey0', ts, 'testvalue0')
        time.sleep(2)
        self.assertIn('testvalue0', self.get(self.slave1, tid, pid, 'testkey0', ts))
        for _ in range(10):
            self.put(self.leader, tid, pid, 'testkey0', 1999999999999, 'testvalue1')
        time.sleep(1)
        if ttl_type == '"kAbsoluteTime"':
            time.sleep(61)
        else:
            pass
        infoLogger.info(self.now())
        self.assertIn('Get failed', self.get(self.slave1, tid, pid, 'testkey0', ts))
        self.assertNotIn('testvalue0', self.get(self.slave1, tid, pid, 'testkey0', 0))
        self.assertIn('testvalue1', self.get(self.slave1, tid, pid, 'testkey0', 0))
        self.ns_drop(self.ns_leader, name)


    def test_create_name_repeat(self):
        """
        表名重复，创建失败
        :return:
        """
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        name = 'naysatest'
        # m = utils.gen_table_metadata(
        #     name, None, 144000, 8,
        #     ('table_partition', '"{}"'.format(self.leader), '"0-2"', 'true'),
        #     ('table_partition', '"{}"'.format(self.slave1), '"0-1"', 'false'),
        #     ('table_partition', '"{}"'.format(self.slave2), '"1-2"', 'false'))
        # utils.gen_table_metadata_file(m, metadata_path)

        table_meta = {
            "name": name,
            "ttl": 144000,
            "partition_num": 8,
            "storage_mode": "kSSD",
            "table_partition": [
                {"endpoint": self.leader,"pid_group": "0-2","is_leader": "true"},
                {"endpoint": self.slave1,"pid_group": "0-1","is_leader": "false"},
                {"endpoint": self.slave2,"pid_group": "1-2","is_leader": "false"},
            ],
        }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs1 = self.run_client(self.ns_leader, 'create ' + metadata_path, 'ns_client')
        self.assertIn('Create table ok', rs1)
        rs2 = self.run_client(self.ns_leader, 'create ' + metadata_path, 'ns_client')
        self.assertIn('Fail to create table', rs2)
        self.ns_drop(self.ns_leader, name)

    def test_create_name_too_many_field(self):
        """
        4000个字段 创建成功
        :return:
        """
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        name = '"large_table"'
        m = utils.gen_table_metadata_ssd(
            name, None, 144000, 8,'kSSD',
            ('table_partition', '"{}"'.format(self.leader), '"0-2"', 'true'),
            ('table_partition', '"{}"'.format(self.slave1), '"0-1"', 'false'),
            ('table_partition', '"{}"'.format(self.slave2), '"1-2"', 'false'),
            ('column_desc', '"k1000"', '"string"', 'true'),
            ('column_desc', '"k1001"', '"double"', 'false'),
            ('column_desc', '"k1002"', '"int32"', 'true'),
        )
        for num in range(1003, 4000):
            name = 'k' + str(num)
            m.append(('column_desc', [('name', '"' + name + '"'), ('type', '"int32"'), ('add_ts_idx', 'false')]))
        print(m[-1])
        #self.assertTrue(False)
        utils.gen_table_metadata_file(m, metadata_path)
        rs1 = self.run_client(self.ns_leader, 'create ' + metadata_path, 'ns_client')
        self.assertIn('Create table ok', rs1)
        self.ns_drop(self.ns_leader, name)

    @ddt.data(
        (('"0-9"', 'true'), ('"1-3"', 'false'), 'Create table ok'),
        (('"0-9"', 'true'), ('"0-9"', 'false'), 'Create table ok'),
        (('"0-3"', 'true'), ('"2-9"', 'false'), 'has not leader'),
        (('"0-3"', 'true'), ('"0-4"', 'false'), 'has not leader'),
        (('"-1-3"', 'true'), ('"0-2"', 'false'), 'pid_group[-1-3] format error.'),
        (('"0"', 'true'), ('"0"', 'false'), 'Create table ok'),
        (('"-1"', 'true'), ('"-1"', 'false'), 'pid_group[-1] format error.'),
        (('"0"', 'true'), ('"2"', 'false'), ' has not leader'),
        (('"3-0"', 'true'), ('"2"', 'false'), 'has not leader'),
        (('"3-0"', 'true'), ('"2"', 'true'), 'pid is not start with zero and consecutive'),
        (('"0"', 'true'), ('"1"', 'true'), 'Create table ok'),
        (('"0"', 'true'), ('"0"', 'true'), 'pid 0 has two leader'),
        (('"0-3"', 'true'), ('"2-4"', 'true'), 'pid 2 has two leader'),
        (('""', 'true'), ('"2-4"', 'true'), 'pid_group[] format error.'),
        (('"0-4000"', 'true'), ('"1"', 'false'), 'Create table ok'),  # RTIDB-238
        (('"0"', 'true'), (None, 'false'), 'table_partition[1].pid_group'),
        ((None, 'true'), ('"1-3"', 'false'), 'table_partition[0].pid_group'),
        (('None', 'true'), ('"1-3"', 'false'), 'table meta file format error'),
        (('""', 'true'), ('"1-3"', 'false'), 'pid_group[] format error.'),
        (('"0-9"', 'false'), ('"1-3"', 'false'), 'has not leader'),
        (('"1-1"', 'false'), ('"0-3"', 'true'), 'Create table ok'),
        ((None, 'false'), (None, 'true'), 'table meta file format error'),
    )
    @ddt.unpack
    def test_create_pid_group(self, pid_group1, pid_group2, exp_msg):
        """
        pid_group参数测试
        :param pid_group1:
        :param pid_group2:
        :param exp_msg:
        :return:
        """
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        name = '"tname{}"'.format(time.time())
        infoLogger.info(name)

        table_partition1 = ('table_partition', '"{}"'.format(self.leader), pid_group1[0], pid_group1[1])
        table_partition2 = ('table_partition', '"{}"'.format(self.slave1), pid_group2[0], pid_group2[1])
        m = utils.gen_table_metadata_ssd(name, None, 144000, 2,'kSSD', table_partition1, table_partition2)
        utils.gen_table_metadata_file(m, metadata_path)

        # table_meta = {
        #     "name": name,
        #     "ttl": 144000,
        #     "storage_mode": "kSSD",
        #     "table_partition": [
        #         {"endpoint": self.leader,"pid_group": pid_group1[0],"is_leader": pid_group1[1]},
        #         {"endpoint": self.slave1,"pid_group": pid_group2[0],"is_leader": pid_group2[1]},
        #     ],
        # }
        # utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.run_client(self.ns_leader, 'create ' + metadata_path, 'ns_client')
        infoLogger.info(rs)
        self.assertIn(exp_msg, rs)
        self.showtable(self.ns_leader, name)
        if exp_msg == 'Create table ok':
            for x in [(self.leader, pid_group1), (self.slave1, pid_group2)]:
                table_status = self.get_table_status(x[0])
                tids = list(set(tpid[0] for tpid in table_status.keys()))
                tids.sort()
                pids = [tpid[1] for tpid in table_status.keys() if tpid[0] == tids[-1]]
                pid_group_start = int(x[1][0].split('-')[0][1:]) if '-' in x[1][0] else int(x[1][0][1:-1])
                pid_group_end = int(x[1][0].split('-')[1][:-1]) if '-' in x[1][0] else int(x[1][0][1:-1])
                infoLogger.info("*"*88)
                infoLogger.info(tids)
                infoLogger.info(table_status.keys())
                infoLogger.info(pids)
                for pid in range(pid_group_start, pid_group_end):
                    self.assertIn(pid, pids)
            time.sleep(1)
            rs1 = self.ns_drop(self.ns_leader, name)
            self.assertIn('drop ok', rs1)


    @ddt.data(
        (('{}'.format(conf.tb_endpoints[0]), '{}'.format(conf.tb_endpoints[0])), 'pid 0 leader and follower at same endpoint'),
        (('{}'.format(conf.tb_endpoints[0]), '172.27.128.35:37770'), 'Fail to create table'),
        (('0.0.0.0:37770', '172.27.128.35:37770'), 'Fail to create table'),
        (('{}'.format(conf.tb_endpoints[0]), '127.0.0.1:47771'), 'Fail to create table'),
        (('', '{}'.format(conf.tb_endpoints[0])), 'Fail to create table'),
        (('{}'.format(conf.tb_endpoints[0]), ''), 'Fail to create table'),
        (('{}'.format(conf.tb_endpoints[0]), '127.0.0.1:44444'), 'Fail to create table'),
        (('{}'.format(conf.tb_endpoints[0]), '127.0.0.1'), 'Fail to create table'),
        (('{}'.format(conf.tb_endpoints[0]), 'abc'), 'Fail to create table'),
        ((None, '{}'.format(conf.tb_endpoints[0])), 'missing required fields: table_partition[0].endpoint'),
        (('000', '{}'.format(conf.tb_endpoints[0])), 'Fail to create table'),
    )
    @ddt.unpack
    def test_create_endpoint(self, ep, exp_msg):
        """
        endpoint参数测试
        :param ep:
        :param exp_msg:
        :return:
        """
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        name = 'tname{}'.format(time.time())
        # m = utils.gen_table_metadata(
        #     name, None, 144000, 2,
        #     ('table_partition', ep[0], '"0-2"', 'true'),
        #     ('table_partition', ep[1], '"0-2"', 'false'))
        # utils.gen_table_metadata_file(m, metadata_path)

        table_meta = {
            "name": name,
            "ttl": 144000,
            "storage_mode": "kSSD",
            "table_partition": [
                {"endpoint": ep[0],"pid_group": "0-2","is_leader": "true"},
                {"endpoint": ep[1],"pid_group": "0-2","is_leader": "false"},
            ],
        }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.run_client(self.ns_leader, 'create ' + metadata_path, 'ns_client')
        infoLogger.info(rs)
        self.assertIn(exp_msg, rs)
        self.run_client(self.ns_leader, 'drop {}'.format(name), 'ns_client')


    @ddt.data(
        ('table meta file format error',
         ('table_partition', '"{}"'.format(leader), '"0-3"', None)),

        ('has not leader pid',
         ('table_partition', '"{}"'.format(leader), '"0-3"', 'false'),
         ('table_partition', '"{}"'.format(slave1), '"0-3"', 'false')),

        ('Create table ok',
         ('table_partition', '"{}"'.format(leader), '"0-3"', 'true'),
         ('table_partition', '"{}"'.format(slave1), '"0-3"', 'false')),

        ('table meta file format error',
         ('table_partition', '"{}"'.format(leader), '"0-3"', '""')),

        ('missing required fields: table_partition[0].endpoint, table_partition[0].pid_group, table_partition[0].is_leader',
         ('table_partition', None, None, None)),
    )
    @ddt.unpack
    def test_create_is_leader(self, exp_msg, *table_partition):
        """
        is_leader参数测试
        :param table_partition:
        :param exp_msg:
        :return:
        """
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        name = '"tname{}"'.format(time.time())
        m = utils.gen_table_metadata_ssd(
            name, None, 144000, 2,'kSSD',
            *table_partition)

        utils.gen_table_metadata_file(m, metadata_path)
        rs = self.run_client(self.ns_leader, 'create ' + metadata_path, 'ns_client')
        self.assertIn(exp_msg, rs)
        if exp_msg == 'Create table ok':
            rs = self.showtable(self.ns_leader, name)
            for k, v in rs.items():
                if k[3] == self.leader:
                    self.assertEqual(v[0], 'leader')
                elif k[3] == self.slave1:
                    self.assertEqual(v[0], 'follower')
        self.ns_drop(self.ns_leader, name)


    # @multi_dimension(True)
    @ddt.data(
        ('Create table ok',
        ('column_desc', '"card"', '"string"', 'true')),

        ('no index',
        ('column_desc', '"card"', '"double"', 'false')),

        ('no index',
        ('column_desc', '"k1"', '"string"', 'false'),
        ('column_desc', '"k2"', '"string"', 'false'),
        ('column_desc', '"k3"', '"double"', 'false')),

        ('Create table ok',
        ('column_desc', '"k1"', '"string"', 'true'),
        ('column_desc', '"k2"', '"string"', 'false'),
        ('column_desc', '"k3"', '"double"', 'false')),

        ('Create table ok',
        ('column_desc', '"k1"', '"string"', 'true'),
        ('column_desc', '"k2"', '"string"', 'true'),
        ('column_desc', '"k3"', '"double"', 'false')),

        ('check column_desc name failed. name is card',
        ('column_desc', '"card"', '"string"', 'true'),
        ('column_desc', '"card"', '"double"', 'false')),

        ('Create table ok',
        ('column_desc', '"k1"', '"string"', 'true'),
        ('column_desc', '"k2"', '"float"', 'false'),
        ('column_desc', '"k3"', '"double"', 'false'),
        ('column_desc', '"k4"', '"int32"', 'false'),
        ('column_desc', '"k5"', '"uint32"', 'false'),
        ('column_desc', '"k6"', '"int64"', 'false'),
        ('column_desc', '"k7"', '"uint64"', 'false')),

        ('Create table ok',
        ('column_desc', '"k1"', '"string"', 'true'),
        ('column_desc', '"k2"', '"float"', 'false'),
        ('column_desc', '"k3"', '"double"', 'false'),
        ('column_desc', '"k4"', '"int32"', 'true'),
        ('column_desc', '"k5"', '"uint32"', 'true'),
        ('column_desc', '"k6"', '"int64"', 'true'),
        ('column_desc', '"k7"', '"uint64"', 'true')),

        ('type double2 is invalid',
        ('column_desc', '"k1"', '"string"', 'true'),
        ('column_desc', '"k2"', '"double2"', 'true')),
    )
    @ddt.unpack
    def test_create_column_desc(self, exp_msg, *column_descs):
        """
        column_desc参数测试
        :param exp_msg:
        :param column_descs:
        :return:
        """
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        name = '"tname{}"'.format(time.time())
        m = utils.gen_table_metadata_ssd(
            name, '"kAbsoluteTime"', 144000, 8,'kSSD',
            ('table_partition', '"{}"'.format(self.leader), '"0-2"', 'true'),
            ('table_partition', '"{}"'.format(self.slave1), '"0-2"', 'false'),
            ('table_partition', '"{}"'.format(self.slave2), '"0-2"', 'false'),
            *column_descs)
        utils.gen_table_metadata_file(m, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        infoLogger.info(rs)
        self.assertIn(exp_msg, rs)
        if exp_msg == 'Create table ok':
            rs1 = self.showtable(self.ns_leader, name)
            tid = rs1.keys()[0][1]
            for edp in (self.leader, self.slave1, self.slave2):
                (schema, column_key) = self.showschema(edp, tid, 2)
                index_set = []
                for arr in column_key:
                    index_set.append(arr[1])
                infoLogger.info(schema)
                self.assertEqual(len(schema), len(column_descs))
                idx = 0
                for i in column_descs:
                    key = i[1][1:-1]
                    type = i[2][1:-1]
                    self.assertEqual(schema[idx][2], type)
                    if i[3] == 'true':
                        self.assertTrue(key in index_set)
                    idx += 1
        self.ns_drop(self.ns_leader, name)


    @ddt.data(
        ('Create table ok',
        ('table_partition', '"{}"'.format(leader), '"0-2"', 'true'),
        ('table_partition', '"{}"'.format(slave1), '"0-1"', 'false'),
        ('table_partition', '"{}"'.format(slave2), '"1-2"', 'false'),
        ('column_desc', '"k1"', '"string"', 'true'),
        ('column_desc', '"k2"', '"double"', 'false'),
        ('column_desc', '"k3"', '"int32"', 'true'),),

        ('Create table ok',
        ('column_desc', '"k1"', '"string"', 'true'),
        ('column_desc', '"k2"', '"double"', 'false'),
        ('column_desc', '"k3"', '"int32"', 'true'),
        ('table_partition', '"{}"'.format(leader), '"0-2"', 'true'),
        ('table_partition', '"{}"'.format(slave1), '"0-1"', 'false'),
        ('table_partition', '"{}"'.format(slave2), '"1-2"', 'false'),),

        ('Create table ok',
        ('table_partition', '"{}"'.format(leader), '"0-2"', 'true'),
        ('column_desc', '"k1"', '"string"', 'true'),
        ('table_partition', '"{}"'.format(slave1), '"0-1"', 'false'),
        ('column_desc', '"k2"', '"double"', 'false'),
        ('table_partition', '"{}"'.format(slave2), '"1-2"', 'false'),
        ('column_desc', '"k3"', '"int32"', 'true'),),

        ('Create table ok',
        ('column_desc', '"k1"', '"string"', 'true'),
        ('column_desc', '"k2"', '"double"', 'false'),
        ('table_partition', '"{}"'.format(leader), '"0-2"', 'true'),
        ('table_partition', '"{}"'.format(slave1), '"0-1"', 'false'),
        ('table_partition', '"{}"'.format(slave2), '"1-2"', 'false'),
        ('column_desc', '"k3"', '"int32"', 'true'),),
    )
    @ddt.unpack
    def test_create_partition_column_order(self, exp_msg, *eles):
        """
        table_partition和column_desc的前后顺序测试，无论顺序如何，都会拼成完整的schema
        :param exp_msg:
        :param eles:
        :return:
        """
        tname = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        m = utils.gen_table_metadata_ssd('"' + tname + '"', '"kAbsoluteTime"', 144000, 8,'kSSD', *eles)
        utils.gen_table_metadata_file(m, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        infoLogger.info(rs)
        self.assertIn(exp_msg, rs)
        rs1 = self.showtable(self.ns_leader, tname)
        tid = rs1.keys()[0][1]
        infoLogger.info(rs1)
        self.assertEqual(rs1[(tname, tid, '0', self.leader)], ['leader', '144000min', 'yes', 'kNoCompress'])
        self.assertEqual(rs1[(tname, tid, '0', self.slave1)], ['follower', '144000min', 'yes', 'kNoCompress'])
        self.assertEqual(rs1[(tname, tid, '2', self.slave2)], ['follower', '144000min', 'yes', 'kNoCompress'])
        (schema, column_key) = self.showschema(self.slave1, tid, 0)
        self.assertEqual(len(schema), 3)
        self.assertEqual(len(column_key), 0)
        self.assertEqual(schema[0], ['0', 'k1', 'string', 'yes'])
        self.assertEqual(schema[1], ['1', 'k2', 'double', 'no'])
        self.assertEqual(schema[2], ['2', 'k3', 'int32', 'yes'])
        # self.assertEqual(column_key[0], ['0', 'k1', 'k1', '-', '144000min'])
        # self.assertEqual(column_key[1], ['1', 'k3', 'k3', '-', '144000min'])
        self.ns_drop(self.ns_leader, tname)

    @ddt.data(
        ('Create table ok', '0', '8', '3', ''),
        ('Create table ok', 'latest:10', '8', '3', ''),
        ('Create table ok', '144000', '8', '3', 'k1:string:index k2:double k3:int32:index'),
        ('partition_num should be large than zero', '144000', '0', '3', ''),
    )
    @ddt.unpack
    def test_create_cmd(self, exp_msg, ttl, partition_num, replica_num, schema):
        """
        不用文件, 直接在命令行指定建表信息
        :return:
        """
        tname = 'tname{}'.format(time.time())
        rs = self.ns_create_cmd(self.ns_leader, tname, ttl, partition_num, replica_num, schema)
        infoLogger.info(rs)
        self.assertIn(exp_msg, rs)
        if (schema != ''):
            self.assertIn(exp_msg, rs)
            rs1 = self.showtable(self.ns_leader, tname)
            tid = rs1.keys()[0][1]
            infoLogger.info(rs1)
            (schema, column_key) = self.showschema(self.slave1, tid, 0)
            self.assertEqual(len(schema), 3)
            self.assertEqual(schema[0], ['0', 'k1', 'string', 'yes'])
            self.assertEqual(schema[1], ['1', 'k2', 'double', 'no'])
            self.assertEqual(schema[2], ['2', 'k3', 'int32', 'yes'])
        self.ns_drop(self.ns_leader, tname)

    @ddt.data(
        ('float or double column can not be index', '0', '8', '1', 'card:string:index mcc:string:index money:float:index'),
        ('float or double column can not be index', '0', '8', '1', 'card:string:index mcc:string:index money:double:index'),
    )
    @ddt.unpack
    def test_create_cmd_index_float_or_double(self, exp_msg, ttl, partition_num, replica_num, schema):
        """
        不用文件, 直接在命令行指定建表信息
        :return:
        """
        tname = 'tname{}'.format(time.time())
        rs = self.ns_create_cmd(self.ns_leader, tname, ttl, partition_num, replica_num, schema)
        infoLogger.info(rs)
        self.assertIn(exp_msg, rs)

    @ddt.data(
        ('Fail to create table. key_entry_max_height must be greater than 0', '0'),
        ('Create table ok', '1'),
        ('Create table ok', '5'),
        ('Create table ok', '12'),
        ('Fail to create table. key_entry_max_height 13 is greater than the max heght 12', '13'),
    )
    @ddt.unpack
    def test_create_key_entry_max_height(self, exp_msg, height):
        self.tname = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        name = '"{}"'.format(self.tname)
        m = utils.gen_table_metadata_ssd(
            name, '"kAbsoluteTime"', 144000, 8,'kSSD',
            ('table_partition', '"{}"'.format(self.leader), '"0-3"', 'true'),
            ('table_partition', '"{}"'.format(self.slave1), '"0-3"', 'false'),
            ('table_partition', '"{}"'.format(self.slave2), '"2-3"', 'false'),
            ('column_desc', '"k1"', '"string"', 'true'),
            ('column_desc', '"k2"', '"string"', 'false'),
            ('column_desc', '"k3"', '"string"', 'false'))
        m[0].append(("key_entry_max_height", height))
        utils.gen_table_metadata_file(m, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        infoLogger.info(rs)
        self.assertIn(exp_msg, rs)
        table_info = self.showtable(self.ns_leader, name)
        if len(table_info) > 0:
            tid = table_info.keys()[0][1]
            for pid in range(3):
                table_meta = self.get_table_meta(self.leaderpath, tid, pid)
                self.assertEqual(table_meta['key_entry_max_height'], height)
        self.ns_drop(self.ns_leader, name)
    def test_create_pid_leader_distribute(self):
        self.clear_ns_table(self.ns_leader);
        name1 = 'tname1{}'.format(time.time())
        name2 = 'tname2{}'.format(time.time())
        # rs = self.ns_create_cmd(self.ns_leader, name1, '0', '32', '3')

        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
            "name": name1,
            "ttl": 0,
            "partition_num": 32,
            "replica_num": 3,
            "storage_mode": "kSSD",
        }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)

        self.assertIn('Create table ok', rs)
        # rs = self.ns_create_cmd(self.ns_leader, name2, '0', '32', '2')

        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
            "name": name2,
            "ttl": 0,
            "partition_num": 32,
            "replica_num": 2,
            "storage_mode": "kSSD",
        }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs)
        table_info = self.showtable(self.ns_leader)
        leader_map = {}
        pid_map = {}
        for (k, v) in table_info.items():
            endpoint = k[3]
            leader_map.setdefault(endpoint, 0)
            pid_map.setdefault(endpoint, 0)
            pid_map[endpoint] += 1
            if v[0] == 'leader':
                leader_map[endpoint] += 1
        self.assertEqual(leader_map[self.leader], 22)
        self.assertEqual(leader_map[self.slave1], 21)
        self.assertEqual(leader_map[self.slave2], 21)
        self.assertEqual(pid_map[self.leader], 54)
        self.assertEqual(pid_map[self.slave1], 53)
        self.assertEqual(pid_map[self.slave2], 53)
        self.ns_drop(self.ns_leader, name1)
        self.ns_drop(self.ns_leader, name2)

    def test_create_pid_drop_create(self):
        self.clear_ns_table(self.ns_leader);
        name1 = 'tname1{}'.format(time.time())
        name2 = 'tname2{}'.format(time.time())
        name3 = 'tname3{}'.format(time.time())
        name4 = 'tname4{}'.format(time.time())
        name5 = 'tname5{}'.format(time.time())
        name6 = 'tname6{}'.format(time.time())
        name7 = 'tname7{}'.format(time.time())
        # rs = self.ns_create_cmd(self.ns_leader, name1, '0', '8', '3')

        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
            "name": name1,
            "ttl": 0,
            "partition_num": 8,
            "replica_num": 3,
            "storage_mode": "kSSD",
        }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs)
        # rs = self.ns_create_cmd(self.ns_leader, name2, '0', '8', '2')

        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
            "name": name2,
            "ttl": 0,
            "partition_num": 8,
            "replica_num": 2,
            "storage_mode": "kSSD",
        }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs)
        # rs = self.ns_create_cmd(self.ns_leader, name3, '0', '8', '1')

        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
            "name": name3,
            "ttl": 0,
            "partition_num": 8,
            "replica_num": 1,
            "storage_mode": "kSSD",
        }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)

        self.assertIn('Create table ok', rs)

        # rs = self.ns_create_cmd(self.ns_leader, name4, '0', '8', '3')

        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
            "name": name4,
            "ttl": 0,
            "partition_num": 8,
            "replica_num": 3,
            "storage_mode": "kSSD",
        }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)

        self.assertIn('Create table ok', rs)
        # rs = self.ns_create_cmd(self.ns_leader, name5, '0', '8', '2')

        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
            "name": name5,
            "ttl": 0,
            "partition_num": 8,
            "replica_num": 2,
            "storage_mode": "kSSD",
        }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)

        self.assertIn('Create table ok', rs)
        self.ns_drop(self.ns_leader, name2)
        # rs = self.ns_create_cmd(self.ns_leader, name6, '0', '8', '2')

        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
            "name": name6,
            "ttl": 0,
            "partition_num": 8,
            "replica_num": 2,
            "storage_mode": "kSSD",
        }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)

        self.assertIn('Create table ok', rs)
        self.ns_drop(self.ns_leader, name4)
        # rs = self.ns_create_cmd(self.ns_leader, name7, '0', '8', '3')

        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
            "name": name7,
            "ttl": 0,
            "partition_num": 8,
            "replica_num": 3,
            "storage_mode": "kSSD",
        }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)

        self.assertIn('Create table ok', rs)
        table_info = self.showtable(self.ns_leader)
        leader_map = {}
        pid_map = {}
        for (k, v) in table_info.items():
            endpoint = k[3]
            leader_map.setdefault(endpoint, 0)
            pid_map.setdefault(endpoint, 0)
            pid_map[endpoint] += 1
            if v[0] == 'leader':
                leader_map[endpoint] += 1
        self.assertEqual(leader_map[self.leader], 13)
        self.assertEqual(leader_map[self.slave1], 14)
        self.assertEqual(leader_map[self.slave2], 13)
        self.assertEqual(pid_map[self.leader], 30)
        self.assertEqual(pid_map[self.slave1], 29)
        self.assertEqual(pid_map[self.slave2], 29)
        self.clear_ns_table(self.ns_leader);
        
if __name__ == "__main__":
    load(TestCreateTableByNsClient)
