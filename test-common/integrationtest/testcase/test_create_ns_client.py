# -*- coding: utf-8 -*-
import unittest
import time
from testcasebase import TestCaseBase
import xmlrunner
from libs.deco import *
import libs.conf as conf
from libs.test_loader import load
import libs.ddt as ddt
import libs.utils as utils
from libs.clients.ns_cluster import NsCluster
from libs.clients.tb_cluster import TbCluster
from libs.logger import infoLogger


nsc = NsCluster(conf.zk_endpoint, *(i[1] for i in conf.ns_endpoints))
tbc = TbCluster(conf.zk_endpoint, [i[1] for i in conf.tb_endpoints], [i[1] for i in conf.tb_scan_endpoints])
nsc.stop_zk()
nsc.clear_zk()
nsc.start_zk()
nsc.kill(*nsc.endpoints)
nsc.start(*nsc.endpoints)
tbc.kill(*tbc.endpoints)
tbc.start(tbc.endpoints, tbc.scan_endpoints)


@ddt.ddt
class TestCreateTableByNsClient(TestCaseBase):

    @ddt.data(
        ('"t{}"'.format(int(time.time() * 1000000 % 10000000000)), 144000, 8, 'Create table ok'),
        ('"aaa"', 144000, 8, 'Create table ok'),
        ('"aaa"', 144000, 8, 'Fail to create table'),
        ('""', 144000, 8, 'Fail to create table'),
        ('"t{}"'.format(int(time.time() * 1000000 % 10000000000)), -1, 8,
         'Error parsing text-format rtidb.client.TableInfo: 2:5: Expected integer.'),
        ('"t{}"'.format(int(time.time() * 1000000 % 10000000000)), '', 8,
         'Error parsing text-format rtidb.client.TableInfo: 3:1: Expected integer.'),
        ('"t{}"'.format(int(time.time() * 1000000 % 10000000000)), 144, -8,
         'Error parsing text-format rtidb.client.TableInfo: 3:9: Expected integer.'),
        ('"t{}"'.format(int(time.time() * 1000000 % 10000000000)), 144, '',
         'Error parsing text-format rtidb.client.TableInfo: 4:1: Expected integer.'),
        (None, 144000, 8, 'Message missing required fields: name'),
        ('"t{}"'.format(int(time.time() * 1000000 % 10000000000)), None, 8, 'Message missing required fields: ttl'),
        ('"t{}"'.format(int(time.time() * 1000000 % 10000000000)), 9, None, 'Message missing required fields: seg_cnt'),
    )
    @ddt.unpack
    def test_create_name_ttl_seg(self, name, ttl, seg_cnt, exp_msg):
        """

        :param name:
        :param seg_cnt:
        :param ttl:
        :param exp_msg:
        :return:
        """
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        m = utils.gen_table_metadata(
            name, ttl, seg_cnt,
            ('"{}"'.format(self.leader), '"1-3"', 'true'),
            ('"{}"'.format(self.slave1), '"1-2"', 'false'),
            ('"{}"'.format(self.slave2), '"2-3"', 'false'))
        utils.gen_table_metadata_file(m, metadata_path)
        rs = self.run_client(nsc.leader, 'create ' + metadata_path, 'ns_client')
        infoLogger.info(rs)
        self.assertTrue(exp_msg in rs)


    @ddt.data(
        (('0-9', 'true'), ('1-3', 'false'), 'Create table ok'),
        (('0-9', 'true'), ('0-9', 'false'), 'Create table ok'),
        (('1-3', 'true'), ('2-9', 'false'), 'pid 4 has not leader'),
        (('1-3', 'true'), ('0-2', 'false'), 'pid 0 has not leader'),
        (('-1-3', 'true'), ('0-2', 'false'), 'pid_group[-1-3] format error.'),
        (('2', 'true'), ('2', 'false'), 'Create table ok'),
        (('-1', 'true'), ('-1', 'false'), 'pid_group[-1] format error.'),
        (('1', 'true'), ('2', 'false'), 'pid 2 has not leader'),
        (('3-1', 'true'), ('2', 'false'), 'pid 2 has not leader'),
        (('3-1', 'true'), ('2', 'true'), 'Create table ok'),
        (('1', 'true'), ('2', 'true'), 'Create table ok'),
        (('1', 'true'), ('1', 'true'), 'pid 1 has two leader'),
        (('1-3', 'true'), ('2-4', 'true'), 'pid 2 has two leader'),
        (('', 'true'), ('2-4', 'true'), 'pid_group[] format error.'),
        (('0', 'true'), ('1-1024', 'true'), 'Create table ok'),
        (('0', 'true'), (None, 'false'), 'pid_group[None] format error.'),
        ((None, 'true'), ('1-3', 'false'), 'pid_group[None] format error.'),
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
        m = utils.gen_table_metadata(
            '"tname{}"'.format(int(time.time() * 1000000 % 10000000000)), 144000, 2,
            ('"{}"'.format(self.leader), '"{}"'.format(pid_group1[0]), pid_group1[1]),
            ('"{}"'.format(self.slave1), '"{}"'.format(pid_group2[0]), pid_group2[1]))
        utils.gen_table_metadata_file(m, metadata_path)
        rs = self.run_client(nsc.leader, 'create ' + metadata_path, 'ns_client')
        infoLogger.info(rs)
        self.assertTrue(exp_msg in rs)
        if exp_msg == 'Create table ok':
            for x in [(self.leader, pid_group1), (self.slave1, pid_group2)]:
                table_status = self.get_table_status(x[0])
                tids = list(set(tpid[0] for tpid in table_status.keys()))
                tids.sort()
                pids = [tpid[1] for tpid in table_status.keys() if tpid[0] == tids[-1]]
                pid_group_start = int(x[1][0].split('-')[0]) if '-' in x[1][0] else int(x[1][0])
                pid_group_end = int(x[1][0].split('-')[1]) if '-' in x[1][0] else int(x[1][0])
                for pid in range(pid_group_start, pid_group_end):
                    self.assertTrue(pid in pids)


    @ddt.data(
        (('127.0.0.1:37770', '127.0.0.1:37770'), 'pid 1 leader and follower at same endpoint'),
        (('127.0.0.1:37770', '0.0.0.0:37770'), 'Fail to create table'),
        (('127.0.0.1:37770', '127.0.0.1:47771'), 'Fail to create table'),
        (('', '127.0.0.1:37770'), 'Fail to create table'),
        (('127.0.0.1:37770', ''), 'Fail to create table'),
        (('127.0.0.1:37770', '127.0.0.1:44444'), 'Fail to create table'),
    )
    @ddt.unpack
    def test_create_endpoint(self, ep, exp_msg):
        """

        :param ep:
        :param exp_msg:
        :return:
        """
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        m = utils.gen_table_metadata(
            '"tname{}"'.format(int(time.time() * 1000000 % 10000000000)), 144000, 2,
            ('"{}"'.format(ep[0]), '"1-3"', 'true'),
            ('"{}"'.format(ep[1]), '"1-3"', 'false'))
        utils.gen_table_metadata_file(m, metadata_path)
        rs = self.run_client(nsc.leader, 'create ' + metadata_path, 'ns_client')
        infoLogger.info(rs)
        self.assertTrue(exp_msg in rs)


    @ddt.data(
        (('"127.0.0.1:37770"', '"1-3"', None), 'table meta file format error'),
        (('"127.0.0.1:37770"', '"1-3"', 'false'), 'pid 1 has not leader'),
        (('"127.0.0.1:37770"', '"1-3"', 'true'), 'Create table ok'),
        (('"127.0.0.1:37770"', '"1-3"', '""'), 'table meta file format error'),
    )
    @ddt.unpack
    def test_create_is_leader(self, table_partition, exp_msg):
        """

        :param table_partition:
        :param exp_msg:
        :return:
        """
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        m = utils.gen_table_metadata(
            '"tname{}"'.format(int(time.time() * 1000000 % 10000000000)), 144000, 2,
            table_partition)
        utils.gen_table_metadata_file(m, metadata_path)
        rs = self.run_client(nsc.leader, 'create ' + metadata_path, 'ns_client')
        infoLogger.info(rs)
        self.assertTrue(exp_msg in rs)


if __name__ == "__main__":
    import libs.test_loader
    load(TestCreateTableByNsClient)
