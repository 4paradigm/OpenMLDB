# -*- coding: utf-8 -*-
import unittest
from testcasebase import TestCaseBase
import xmlrunner
from libs.deco import multi_dimension
import libs.conf as conf
from libs.test_loader import load
import libs.ddt as ddt
import time
from libs.logger import infoLogger


@ddt.ddt
class TestTtl(TestCaseBase):

    @multi_dimension(False)
    @ddt.data(
        ('latest:0', 'Create table failed'),
        ('latest:-1', 'Create table failed'),
        ('latest:1.0', 'Invalid args, tid , pid or ttl should be uint32_t'),
        ('latest:1.5', 'Invalid args, tid , pid or ttl should be uint32_t'),
        ('latest:1111111111111111', 'Create table ok'),
        ('latest:a', 'Invalid args, tid , pid or ttl should be uint32_t'),
        ('latestt:1', 'Create table failed'),
    )
    @ddt.unpack
    def test_ttl_abnormal_create(self, ttl, exp_msg):
        """
        ttl = latest:abnormal
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid, ttl, 2, '')
        infoLogger.info(rs1)
        self.assertTrue(exp_msg in rs1)


    @multi_dimension(False)
    @ddt.data(
        ('latest:1', {('v1', 10): False, ('v2', 20): False, ('v3', 30): True}),
        ('latest:1', {('v1', 20): False, ('v2', 30): True, ('v3', 10): False}),
        ('latest:1', {('v1', 30): True, ('v2', 10): False, ('v3', 20): False}),
        ('latest:1', {('v1', 10): False, ('v2', 10): False, ('v3', 30): True}),
        ('latest:1', {('v1', 10): False, ('v2', 30): False, ('v3', 30): False}),
        ('latest:1', {('v1', 10): False, ('v2', 20): True}),
        ('latest:2', {('v1', 10): True, ('v2', 20): True}),
        ('latest:3', {('v1', 10): True, ('v2', 20): True}),
        ('latest:1', {('v1', 10): True}),
    )
    @ddt.unpack
    def test_ttl_latest(self, ttl, value_ts_scannable):
        """

        :param ttl:
        :param value_ts_scannable:
        :return:
        """
        self.create(self.leader, 't', self.tid, self.pid, ttl, 2, '')
        for i in value_ts_scannable.keys():
            self.put(self.leader, self.tid, self.pid, 'pk', i[1], '|{}|'.format(i[0]))
        time.sleep(1)
        rs = self.scan(self.leader, self.tid, self.pid, 'pk', self.now(), 1)
        infoLogger.info(rs)
        for i in value_ts_scannable.keys():
            self.assertTrue('|{}|'.format(i[0]) in rs)
        time.sleep(61)
        rs1 = self.scan(self.leader, self.tid, self.pid, 'pk', self.now(), 1)
        infoLogger.info(rs1)
        for k, v in value_ts_scannable.items():
            if v is True:
                self.assertTrue('|{}|'.format(k[0]) in rs1)
            else:
                self.assertFalse('|{}|'.format(k[0]) in rs1)


    @multi_dimension(False)
    @ddt.data(
        ('latest:1', {('v1', 10): False, ('v2', 20): True}, [('v3', 30)], {('v2', 10): False, ('v3', 30): True}),
        ('latest:1', {('v1', 10): False, ('v2', 20): True}, [('v3', 10)], {('v2', 20): True, ('v3', 10): False}),
    )
    @ddt.unpack
    def test_ttl_put_after_ttl(self, ttl, value_ts_scannable, put_value_ts, value_ts_scannable2):
        """

        :param ttl:
        :param value_ts_scannable:
        :param put_value_ts:
        :param value_ts_scannable2:
        :return:
        """
        self.create(self.leader, 't', self.tid, self.pid, ttl, 2, '')
        for i in value_ts_scannable.keys():
            self.put(self.leader, self.tid, self.pid, 'pk', i[1], '|{}|'.format(i[0]))
        time.sleep(1)
        rs = self.scan(self.leader, self.tid, self.pid, 'pk', self.now(), 1)
        infoLogger.info(rs)
        for i in value_ts_scannable.keys():
            self.assertTrue('|{}|'.format(i[0]) in rs)
        time.sleep(61)
        rs1 = self.scan(self.leader, self.tid, self.pid, 'pk', self.now(), 1)
        infoLogger.info(rs1)
        for k, v in value_ts_scannable.items():
            if v is True:
                self.assertTrue('|{}|'.format(k[0]) in rs1)
            else:
                self.assertFalse('|{}|'.format(k[0]) in rs1)
        # put again after the last gc
        for i in put_value_ts:
            self.put(self.leader, self.tid, self.pid, 'pk', i[1], '|{}|'.format(i[0]))
        time.sleep(1)
        rs = self.scan(self.leader, self.tid, self.pid, 'pk', self.now(), 1)
        infoLogger.info(rs)
        for i in value_ts_scannable2.keys():
            self.assertTrue('|{}|'.format(i[0]) in rs)
        time.sleep(61)
        rs1 = self.scan(self.leader, self.tid, self.pid, 'pk', self.now(), 1)
        infoLogger.info(rs1)
        for k, v in value_ts_scannable2.items():
            if v is True:
                self.assertTrue('|{}|'.format(k[0]) in rs1)
            else:
                self.assertFalse('|{}|'.format(k[0]) in rs1)


if __name__ == "__main__":
    import libs.test_loader
    load(TestTtl)
