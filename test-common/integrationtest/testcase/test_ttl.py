# -*- coding: utf-8 -*-
from testcasebase import TestCaseBase
from libs.test_loader import load
import libs.ddt as ddt
import random
import time
from libs.logger import infoLogger
import libs.utils as utils
from libs.deco import multi_dimension


@ddt.ddt
class TestTtl(TestCaseBase):

    def setUp(self):
        infoLogger.info('\n' * 5 + 'TEST CASE NAME: ' + self._testMethodName + self._testMethodDoc)
        self.ns_leader = utils.exe_shell('head -n 1 {}/ns_leader'.format(self.testpath))
        self.ns_leader_path = utils.exe_shell('tail -n 1 {}/ns_leader'.format(self.testpath))
        self.tid = random.randint(1, 1000)
        self.pid = random.randint(1, 1000)
        self.clear_ns_table(self.ns_leader)
        self.confset(self.ns_leader, 'auto_failover', 'true')
        self.confset(self.ns_leader, 'auto_recover_table', 'true')
        infoLogger.info('\n\n' + '|' * 50 + ' SETUP FINISHED ' + '|' * 50 + '\n')

    @multi_dimension(False)
    @ddt.data(
        ('latest:0', 'Create table ok'),
        ('latest:-1', 'ttl should be equal or greater than 0'),
        ('latest:1.0', 'Invalid args, tid , pid or ttl should be uint32_t'),
        ('latest:1.5', 'Invalid args, tid , pid or ttl should be uint32_t'),
        ('latest:111111111111', 'Fail to create table'),
        ('latest:a', 'Invalid args, tid , pid or ttl should be uint32_t'),
        ('latestt:1', 'invalid ttl type'),
    )
    @ddt.unpack
    def test_ttl_abnormal_create(self, ttl, exp_msg):
        """
        ttl = latest:abnormal
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid, ttl, 2, 'true')
        infoLogger.info(rs1)
        self.assertIn(exp_msg, rs1)


    @multi_dimension(True)
    @ddt.data(
        ('latest:0', 'Create table ok'),
        ('latest:-1', 'invalid ttl which should be equal or greater than 0'),
        ('latest:1.0', 'Invalid args bad lexical cast: source type value could not be interpreted as target'),
        ('latest:1.5', 'Invalid args bad lexical cast: source type value could not be interpreted as target'),
        ('latest:1111111111111111', 'Fail to create table'),
        ('latest:a', 'Invalid args bad lexical cast: source type value could not be interpreted as target'),
        ('latestt:1', 'invalid ttl type'),
    )
    @ddt.unpack
    def test_ttl_abnormal_create_md(self, ttl, exp_msg):
        """
        ttl = latest:abnormal
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid, ttl, 2, 'true')
        infoLogger.info(rs1)
        self.assertIn(exp_msg, rs1)


    def test_ttl_latest_1_ready_test(self):
        """
        READYTEST：为test_ttl_latest_2做测前准备，提前将数据put好
        :return:
        """
        ddt = (
            (10000, 'latest:1', {('v1', 10): False, ('v2', 20): False, ('v3', 30): True}),
            (10001, 'latest:1', {('v1', 20): False, ('v2', 30): True, ('v3', 10): False}),
            (10002, 'latest:1', {('v1', 30): True, ('v2', 10): False, ('v3', 20): False}),
            (10003, 'latest:1', {('v1', 10): False, ('v2', 10): False, ('v3', 30): True}),
            (10004, 'latest:1', {('v1', 10): False, ('v2', 30): False, ('v3', 30): True}),
            (10005, 'latest:1', {('v1', 10): False, ('v2', 20): True}),
            (10006, 'latest:2', {('v1', 10): True, ('v2', 20): True}),
            (10007, 'latest:3', {('v1', 10): True, ('v2', 20): True}),
            (10008, 'latest:1', {('v1', 10): True}),
            (10009, 'latest:0', {('v1', 10): True, ('v2', 20): True}),
            (10010, '1', {('v1', 10): False, ('v2', 20): False, ('v3', 30): False}),
            (10011, '144000', {('v1', int(time.time() * 1000)): True, ('v2', 20): False, ('v3', 30): False}),
            (10012, '0', {('v1', 10): True, ('v2', 20): True, ('v3', 30): True}),
        )
        for data in ddt:
            tid = data[0]
            ttl = data[1]
            value_ts_scannable = data[2]
            self.drop(self.leader, tid, 0)
            self.create(self.leader, 't', tid, 0, ttl, 2, 'true')
            for i in value_ts_scannable.keys():
                # for multidimension test
                self.multidimension_vk = {'card': ('string:index', 'pk'),
                                          'merchant': ('string:index', '|{}|'.format(i[0])),
                                          'amt': ('double', 1.1)}
                self.put(self.leader, tid, 0, 'pk', i[1], '|{}|'.format(i[0]))
            time.sleep(1)
            self.multidimension_scan_vk = {'card': 'pk'}  # for multidimension
            if ttl.find("latest") == -1:
                rs = self.scan(self.leader, tid, 0, 'pk', self.now(), 1)
                infoLogger.info(rs)
                for k, v in value_ts_scannable.items():
                    if v is True:
                        self.assertIn('|{}|'.format(k[0]), rs)
                    else:
                         self.assertNotIn('|{}|'.format(k[0]), rs)
            else:
                for k, v in value_ts_scannable.items():
                    rs = self.get(self.leader, tid, 0, 'pk', k[1])
                    infoLogger.info(rs)
                    if v is True:
                        self.assertIn('|{}|'.format(k[0]), rs)
                    else:
                         self.assertNotIn('|{}|'.format(k[0]), rs)
        #time.sleep(61)


    @ddt.data(
        (10000, 'latest:1', {('v1', 10): False, ('v2', 20): False, ('v3', 30): True}),
        (10001, 'latest:1', {('v1', 20): False, ('v2', 30): True, ('v3', 10): False}),
        (10002, 'latest:1', {('v1', 30): True, ('v2', 10): False, ('v3', 20): False}),
        (10003, 'latest:1', {('v1', 10): False, ('v2', 10): False, ('v3', 30): True}),
        (10004, 'latest:1', {('v1', 10): False, ('v2', 30): False, ('v3', 30): True}),
        (10005, 'latest:1', {('v1', 10): False, ('v2', 20): True}),
        (10006, 'latest:2', {('v1', 10): True, ('v2', 20): True}),
        (10007, 'latest:3', {('v1', 10): True, ('v2', 20): True}),
        (10008, 'latest:1', {('v1', 10): True}),
        (10009, 'latest:0', {('v1', 10): True, ('v2', 20): True}),
        (10010, '1', {('v1', 10): False, ('v2', 20): False, ('v3', 30): False}),
        (10011, '144000', {('v1', int(time.time() * 1000)): True, ('v2', 20): False, ('v3', 30): False}),
        (10012, '0', {('v1', 10): True, ('v2', 20): True, ('v3', 30): True}),
    )
    @ddt.unpack
    def test_ttl_latest_2_check_ttl(self, tid, ttl, value_ts_scannable):
        """
        depends on test_ttl_latest_1_ready_test
        :param tid:
        :param ttl:
        :param value_ts_scannable:
        :return:
        """
        self.multidimension_scan_vk = {'card': 'pk'}  # for multidimension
        if ttl.find("latest") == -1:
            rs1 = self.scan(self.leader, tid, 0, 'pk', self.now(), 1)
            infoLogger.info(rs1)
            for k, v in value_ts_scannable.items():
                if v is True:
                    self.assertIn('|{}|'.format(k[0]), rs1)
                else:
                    self.assertNotIn('|{}|'.format(k[0]), rs1)
        else:
            for k, v in value_ts_scannable.items():
                rs = self.get(self.leader, tid, 0, 'pk', k[1])
                infoLogger.info(rs)
                if v is True:
                    self.assertIn('|{}|'.format(k[0]), rs)
                else:
                     self.assertNotIn('|{}|'.format(k[0]), rs)
        self.drop(self.leader, tid, 0)


    @ddt.data(
        ('latest:10',
         {('v1', 100): False, ('v2', 200): True},
         [('v3', 300)],
         {('v2', 100): False, ('v3', 300): True}),
        ('latest:10',
         {('v1', 100): False, ('v2', 200): True},
         [('v3', 100)],
         {('v2', 200): True, ('v3', 100): False}),
    )
    @ddt.unpack
    def test_ttl_put_after_ttl(self, ttl, value_ts_scannable, put_value_ts, value_ts_scannable2):  # RTIDB-181
        """
        ttl后put数据，再次ttl后数据正确
        :param ttl:
        :param value_ts_scannable:
        :param put_value_ts:
        :param value_ts_scannable2:
        :return:
        """
        self.create(self.leader, 't', self.tid, self.pid, ttl, 2, 'true')
        for i in value_ts_scannable.keys():
            # for multidimension test
            self.multidimension_vk = {'card': ('string:index', 'pk'),
                                      'merchant': ('string:index', '|{}|'.format(i[0])),
                                      'amt': ('double', 1.1)}
            for ts in range(100):
                self.put(self.leader, self.tid, self.pid, 'pk', i[1] - ts, '|{}|'.format(i[0]))
        time.sleep(1)

        self.multidimension_scan_vk = {'card': 'pk'}  # for multidimension
        for k, v in value_ts_scannable.items():
            rs = self.get(self.leader, self.tid, self.pid, 'pk', k[1])
            infoLogger.info(rs)
            if v is True:
                self.assertIn('|{}|'.format(k[0]), rs)
            else:
                self.assertNotIn('|{}|'.format(k[0]), rs)
        # put again after the last gc
        for i in put_value_ts:
            # for multidimension test
            self.multidimension_vk = {'card': ('string:index', 'pk'),
                                      'merchant': ('string:index', '|{}|'.format(i[0])),
                                      'amt': ('double', 1.1)}
            for ts in range(100):
                self.put(self.leader, self.tid, self.pid, 'pk', i[1] - ts, '|{}|'.format(i[0]))
        time.sleep(1)
        for k, v in value_ts_scannable2.items():
            rs = self.get(self.leader, self.tid, self.pid, 'pk', k[1])
            infoLogger.info(rs)
            if v is True:
                self.assertIn('|{}|'.format(k[0]), rs)
            else:
                self.assertNotIn('|{}|'.format(k[0]), rs)



if __name__ == "__main__":
    load(TestTtl)
