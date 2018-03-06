# -*- coding: utf-8 -*-
import time
from testcasebase import TestCaseBase
from libs.deco import *
import libs.conf as conf
from libs.test_loader import load
import libs.ddt as ddt
import libs.utils as utils
from libs.logger import infoLogger


def get_base_attr(attr):
    TestCaseBase.setUpClass()
    return TestCaseBase.__getattribute__(TestCaseBase, attr)


@ddt.ddt
class TestCreateTableByNsClient(TestCaseBase):

    @multi_dimension(False)
    @ddt.data(
        ('"t{}"'.format(int(time.time() * 1000000 % 10000000000)), None, 144000, 8,
         'Create table ok'),
        ('"t{}"'.format(int(time.time() * 1000000 % 10000000000)), '"latest"', 144000, 8,
         'ttl type latest is invalid'),
        ('"t{}"'.format(int(time.time() * 1000000 % 10000000000)), '', 144000, 8,
         'table meta file format error'),
        ('""', None, 144000, 8,
         'Fail to create table'),
        ('"t{}"'.format(int(time.time() * 1000000 % 10000000000)), None, -1, 8,
         'Error parsing text-format rtidb.client.TableInfo: 2:5: Expected integer.'),
        ('"t{}"'.format(int(time.time() * 1000000 % 10000000000)), None, '', 8,
         'Error parsing text-format rtidb.client.TableInfo: 3:1: Expected integer.'),
        ('"t{}"'.format(int(time.time() * 1000000 % 10000000000)), None, '"144000"', 8,
         'table meta file format error'),
        ('"t{}"'.format(int(time.time() * 1000000 % 10000000000)), None, 144, -8,
         'Error parsing text-format rtidb.client.TableInfo: 3:9: Expected integer.'),
        ('"t{}"'.format(int(time.time() * 1000000 % 10000000000)), None, 144, '',
         'Error parsing text-format rtidb.client.TableInfo: 4:1: Expected integer.'),
        ('"t{}"'.format(int(time.time() * 1000000 % 10000000000)), None, 144, '"8"',
         'table meta file format error'),
        (None, None, 144000, 8,
         'Message missing required fields: name'),
        ('"t{}"'.format(int(time.time() * 1000000 % 10000000000)), None, None, 8,
         'Message missing required fields: ttl'),
        ('"t{}"'.format(int(time.time() * 1000000 % 10000000000)), None, 9, None,
         'Message missing required fields: seg_cnt'),
    )
    @ddt.unpack
    def test_create_name_ttltype_ttl_seg(self, name, ttl_type, ttl, seg_cnt, exp_msg):
        """

        :param ttl_type:
        :param name:
        :param seg_cnt:
        :param ttl:
        :param exp_msg:
        :return:
        """
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        m = utils.gen_table_metadata(
            name, ttl_type, ttl, seg_cnt,
            ('table_partition', '"{}"'.format(self.leader), '"0-2"', 'true'),
            ('table_partition', '"{}"'.format(self.slave1), '"0-1"', 'false'),
            ('table_partition', '"{}"'.format(self.slave2), '"1-2"', 'false'))
        utils.gen_table_metadata_file(m, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertTrue(exp_msg in rs)

        if exp_msg == 'Create table ok':
            table_info = self.showtable(self.ns_leader)
            tid = table_info.keys()[0][1]
            pid = table_info.keys()[0][2]
            self.put(self.leader, tid, pid, 'testkey0', self.now() + 100, 'testvalue0')
            time.sleep(0.5)
            self.assertTrue(
                'testvalue0' in self.scan(self.slave1, tid, pid, 'testkey0', self.now(), 1))


    @multi_dimension(False)
    @ddt.data(
        ('"t{}"'.format(int(time.time())), '"kLatestTime"', 10, 8),
        ('"t{}"'.format(int(time.time())), '"kAbsoluteTime"', 1, 8),  # RTIDB-202
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
        m = utils.gen_table_metadata(
            name, ttl_type, ttl, seg_cnt,
            ('table_partition', '"{}"'.format(self.leader), '"0-2"', 'true'),
            ('table_partition', '"{}"'.format(self.slave1), '"0-1"', 'false'),
            ('table_partition', '"{}"'.format(self.slave2), '"1-2"', 'false'))
        utils.gen_table_metadata_file(m, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertTrue('Create table ok' in rs)

        table_info = self.showtable(self.ns_leader)
        tid = table_info.keys()[0][1]
        pid = table_info.keys()[0][2]
        for _ in range(10):
            self.put(self.leader, tid, pid, 'testkey0', 1444444444444, 'testvalue0')
        self.assertTrue('testvalue0' in self.get(self.slave1, tid, pid, 'testkey0', 1444444444444))
        for _ in range(10):
            self.put(self.leader, tid, pid, 'testkey0', 1999999999999, 'testvalue1')
        time.sleep(1)
        self.assertFalse('testvalue0' in self.get(self.slave1, tid, pid, 'testkey0', 1444444444444))


    def test_create_name_repeat(self):
        """

        :return:
        """
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        m = utils.gen_table_metadata(
            '"naysatest"', None, 144000, 8,
            ('table_partition', '"{}"'.format(self.leader), '"0-2"', 'true'),
            ('table_partition', '"{}"'.format(self.slave1), '"0-1"', 'false'),
            ('table_partition', '"{}"'.format(self.slave2), '"1-2"', 'false'))
        utils.gen_table_metadata_file(m, metadata_path)
        rs1 = self.run_client(self.ns_leader, 'create ' + metadata_path, 'ns_client')
        self.assertTrue('Create table ok' in rs1)
        rs2 = self.run_client(self.ns_leader, 'create ' + metadata_path, 'ns_client')
        self.assertTrue('Fail to create table' in rs2)


    @ddt.data(
        (('"0-9"', 'true'), ('"1-3"', 'false'), 'Create table ok'),
        (('"0-9"', 'true'), ('"0-9"', 'false'), 'Create table ok'),
        (('"0-3"', 'true'), ('"2-9"', 'false'), 'pid 4 has not leader'),
        (('"0-3"', 'true'), ('"0-4"', 'false'), 'pid 4 has not leader'),
        (('"-1-3"', 'true'), ('"0-2"', 'false'), 'pid_group[-1-3] format error.'),
        (('"0"', 'true'), ('"0"', 'false'), 'Create table ok'),
        (('"-1"', 'true'), ('"-1"', 'false'), 'pid_group[-1] format error.'),
        (('"0"', 'true'), ('"2"', 'false'), 'pid 2 has not leader'),
        (('"3-0"', 'true'), ('"2"', 'false'), 'has not leader pid'),
        (('"3-0"', 'true'), ('"2"', 'true'), 'pid is not start with zero and consecutive'),
        (('"0"', 'true'), ('"1"', 'true'), 'Create table ok'),
        (('"0"', 'true'), ('"0"', 'true'), 'pid 0 has two leader'),
        (('"0-3"', 'true'), ('"2-4"', 'true'), 'pid 2 has two leader'),
        (('""', 'true'), ('"2-4"', 'true'), 'pid_group[] format error.'),
        (('"0"', 'true'), ('"1-1024"', 'true'), 'Create table ok'),
        (('"0"', 'true'), (None, 'false'), 'table_partition[1].pid_group'),
        ((None, 'true'), ('"1-3"', 'false'), 'table_partition[0].pid_group'),
        (('None', 'true'), ('"1-3"', 'false'), 'table meta file format error'),
        (('""', 'true'), ('"1-3"', 'false'), 'pid_group[] format error.'),
        (('"0-9"', 'false'), ('"1-3"', 'false'), 'has not leader pid'),
        (('"1-1"', 'false'), ('"0-3"', 'true'), 'Create table ok'),
        ((None, 'false'), (None, 'true'), 'table meta file format error'),
    )
    @ddt.unpack
    def test_create_pid_group(self, pid_group1, pid_group2, exp_msg):
        """

        :param pid_group1:
        :param pid_group2:
        :param exp_msg:
        :return:
        """
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        name = '"tname{}"'.format(int(time.time() * 1000000 % 10000000000))
        table_partition1 = ('table_partition', '"{}"'.format(self.leader), pid_group1[0], pid_group1[1])
        table_partition2 = ('table_partition', '"{}"'.format(self.slave1), pid_group2[0], pid_group2[1])
        m = utils.gen_table_metadata(name, None, 144000, 2, table_partition1, table_partition2)
        utils.gen_table_metadata_file(m, metadata_path)
        rs = self.run_client(self.ns_leader, 'create ' + metadata_path, 'ns_client')
        infoLogger.info(rs)
        self.assertTrue(exp_msg in rs)
        self.showtable(self.ns_leader)
        if exp_msg == 'Create table ok':
            for x in [(self.leader, pid_group1), (self.slave1, pid_group2)]:
                table_status = self.get_table_status(x[0])
                tids = list(set(tpid[0] for tpid in table_status.keys()))
                tids.sort()
                pids = [tpid[1] for tpid in table_status.keys() if tpid[0] == tids[-1]]
                pid_group_start = int(x[1][0].split('-')[0][1:]) if '-' in x[1][0] else int(x[1][0][1:-1])
                pid_group_end = int(x[1][0].split('-')[1][:-1]) if '-' in x[1][0] else int(x[1][0][1:-1])
                for pid in range(pid_group_start, pid_group_end):
                    self.assertTrue(pid in pids)
            rs1 = self.ns_drop(self.ns_leader, name[1:-1])
            self.assertEqual('drop ok' in rs1, True)


    @ddt.data(
        (('"127.0.0.1:37770"', '"127.0.0.1:37770"'), 'pid 0 leader and follower at same endpoint'),
        (('"127.0.0.1:37770"', '"172.27.128.35:37770"'), 'Fail to create table'),
        (('"0.0.0.0:37770"', '"172.27.128.35:37770"'), 'Fail to create table'),
        (('"127.0.0.1:37770"', '"127.0.0.1:47771"'), 'Fail to create table'),
        (('""', '"127.0.0.1:37770"'), 'Fail to create table'),
        (('"127.0.0.1:37770"', '""'), 'Fail to create table'),
        (('"127.0.0.1:37770"', '"127.0.0.1:44444"'), 'Fail to create table'),
        (('"127.0.0.1:37770"', '"127.0.0.1"'), 'Fail to create table'),
        (('"127.0.0.1:37770"', '"abc"'), 'Fail to create table'),
        ((None, '"127.0.0.1:37770"'), 'missing required fields: table_partition[0].endpoint'),
        (('"000"', '"127.0.0.1:37770"'), 'Fail to create table. error msg: create table failed'),
    )
    @ddt.unpack
    def test_create_endpoint(self, ep, exp_msg):
        """

        :param ep:
        :param exp_msg:
        :return:
        """
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        name = '"tname{}"'.format(int(time.time() * 1000000 % 10000000000))
        m = utils.gen_table_metadata(
            name, None, 144000, 2,
            ('table_partition', ep[0], '"0-2"', 'true'),
            ('table_partition', ep[1], '"0-2"', 'false'))
        utils.gen_table_metadata_file(m, metadata_path)
        rs = self.run_client(self.ns_leader, 'create ' + metadata_path, 'ns_client')
        infoLogger.info(rs)
        self.assertTrue(exp_msg in rs)
        self.run_client(self.ns_leader, 'drop {}'.format(name), 'ns_client')


    @ddt.data(
        ('table meta file format error',
         ('table_partition', '"{}"'.format(get_base_attr('leader')), '"0-3"', None)),

        ('has not leader pid',
         ('table_partition', '"{}"'.format(get_base_attr('leader')), '"0-3"', 'false'),
         ('table_partition', '"{}"'.format(get_base_attr('slave1')), '"0-3"', 'false')),

        ('Create table ok',
         ('table_partition', '"{}"'.format(get_base_attr('leader')), '"0-3"', 'true'),
         ('table_partition', '"{}"'.format(get_base_attr('slave1')), '"0-3"', 'false')),

        ('table meta file format error',
         ('table_partition', '"{}"'.format(get_base_attr('leader')), '"0-3"', '""')),

        ('has not table_partition in table meta file', None),  # RTIDB-193

        ('missing required fields: table_partition[0].endpoint, table_partition[0].pid_group, table_partition[0].is_leader',
         ('table_partition', None, None, None)),
    )
    @ddt.unpack
    def test_create_is_leader(self, exp_msg, *table_partition):
        """

        :param table_partition:
        :param exp_msg:
        :return:
        """
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        name = '"tname{}"'.format(int(time.time() * 1000000 % 10000000000))
        m = utils.gen_table_metadata(
            name, None, 144000, 2,
            *table_partition)

        utils.gen_table_metadata_file(m, metadata_path)
        rs = self.run_client(self.ns_leader, 'create ' + metadata_path, 'ns_client')
        self.assertEqual(exp_msg in rs, True)
        if exp_msg == 'Create table ok':
            rs = self.showtable(self.ns_leader)
            for k, v in rs.items():
                if k[3] == self.leader:
                    self.assertEqual(v[0], 'leader')
                elif k[3] == self.slave1:
                    self.assertEqual(v[0], 'follower')


    @multi_dimension(True)
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
        ('column_desc', '"k3"', '"double"', 'true')),

        ('check name failed',
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
        ('column_desc', '"k2"', '"float"', 'true'),
        ('column_desc', '"k3"', '"double"', 'true'),
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

        :param exp_msg:
        :param column_descs:
        :return:
        """
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        m = utils.gen_table_metadata(
            '"tname{}"'.format(int(time.time())), '"kAbsoluteTime"', 144000, 8,
            ('table_partition', '"{}"'.format(self.leader), '"0-2"', 'true'),
            ('table_partition', '"{}"'.format(self.slave1), '"0-1"', 'false'),
            ('table_partition', '"{}"'.format(self.slave2), '"1-2"', 'false'),
            *column_descs)
        utils.gen_table_metadata_file(m, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        infoLogger.info(rs)
        self.assertTrue(exp_msg in rs)
        if exp_msg == 'Create table ok':
            rs1 = self.showtable(self.ns_leader)
            tid = rs1.keys()[0][1]
            schema = self.showschema(self.slave1, tid, 2)
            infoLogger.info(schema)
            self.assertEqual(len(schema), len(column_descs))
            for i in column_descs:
                key = i[1][1:-1]
                type = i[2][1:-1]
                index = 'yes' if i[3] == 'true' else 'no'
                self.assertEqual(schema[key], [type, index])


    @ddt.data(
        ('Create table ok',
        ('table_partition', '"{}"'.format(get_base_attr('leader')), '"0-2"', 'true'),
        ('table_partition', '"{}"'.format(get_base_attr('slave1')), '"0-1"', 'false'),
        ('table_partition', '"{}"'.format(get_base_attr('slave2')), '"1-2"', 'false'),
        ('column_desc', '"k1"', '"string"', 'true'),
        ('column_desc', '"k2"', '"double"', 'false'),
        ('column_desc', '"k3"', '"int32"', 'true'),),

        ('Create table ok',
        ('column_desc', '"k1"', '"string"', 'true'),
        ('column_desc', '"k2"', '"double"', 'false'),
        ('column_desc', '"k3"', '"int32"', 'true'),
        ('table_partition', '"{}"'.format(get_base_attr('leader')), '"0-2"', 'true'),
        ('table_partition', '"{}"'.format(get_base_attr('slave1')), '"0-1"', 'false'),
        ('table_partition', '"{}"'.format(get_base_attr('slave2')), '"1-2"', 'false'),),

        ('Create table ok',
        ('table_partition', '"{}"'.format(get_base_attr('leader')), '"0-2"', 'true'),
        ('column_desc', '"k1"', '"string"', 'true'),
        ('table_partition', '"{}"'.format(get_base_attr('slave1')), '"0-1"', 'false'),
        ('column_desc', '"k2"', '"double"', 'false'),
        ('table_partition', '"{}"'.format(get_base_attr('slave2')), '"1-2"', 'false'),
        ('column_desc', '"k3"', '"int32"', 'true'),),

        ('Create table ok',
        ('column_desc', '"k1"', '"string"', 'true'),
        ('column_desc', '"k2"', '"double"', 'false'),
        ('table_partition', '"{}"'.format(get_base_attr('leader')), '"0-2"', 'true'),
        ('table_partition', '"{}"'.format(get_base_attr('slave1')), '"0-1"', 'false'),
        ('table_partition', '"{}"'.format(get_base_attr('slave2')), '"1-2"', 'false'),
        ('column_desc', '"k3"', '"int32"', 'true'),),
    )
    @ddt.unpack
    def test_create_partition_column_order(self, exp_msg, *eles):
        """

        :param exp_msg:
        :param column_descs:
        :return:
        """
        tname = 'tname{}'.format(int(time.time()))
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        m = utils.gen_table_metadata('"' + tname + '"', '"kAbsoluteTime"', 144000, 8, *eles)
        utils.gen_table_metadata_file(m, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        infoLogger.info(rs)
        self.assertTrue(exp_msg in rs)
        rs1 = self.showtable(self.ns_leader)
        tid = rs1.keys()[0][1]
        infoLogger.info(rs1)
        self.assertEqual(rs1[(tname, tid, '0', self.leader)], ['leader', '8', '144000', 'yes'])
        self.assertEqual(rs1[(tname, tid, '0', self.slave1)], ['follower', '8', '144000', 'yes'])
        self.assertEqual(rs1[(tname, tid, '2', self.slave2)], ['follower', '8', '144000', 'yes'])
        schema = self.showschema(self.slave1, tid, 0)
        infoLogger.info(schema)
        self.assertEqual(len(schema), 3)
        self.assertEqual(schema['k1'], ['string', 'yes'])
        self.assertEqual(schema['k2'], ['double', 'no'])
        self.assertEqual(schema['k3'], ['int32', 'yes'])


if __name__ == "__main__":
    load(TestCreateTableByNsClient)
