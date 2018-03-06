# -*- coding: utf-8 -*-
from testcasebase import TestCaseBase
from libs.test_loader import load
import libs.ddt as ddt
import time
from libs.logger import infoLogger
from libs.deco import multi_dimension


@ddt.ddt
class TestTtl(TestCaseBase):

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
        self.assertTrue(exp_msg in rs1)


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
        self.assertTrue(exp_msg in rs1)


    def test_ttl_latest_1_ready_test(self):
        """

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
            if ttl.find("latest") == -1:
                self.multidimension_scan_vk = {'card': 'pk'}  # for multidimension
                rs = self.scan(self.leader, tid, 0, 'pk', self.now(), 1)
                infoLogger.info(rs)
                for k, v in value_ts_scannable.items():
                    if v is True:
                        self.assertTrue('|{}|'.format(k[0]) in rs)
                    else:
                         self.assertFalse('|{}|'.format(k[0]) in rs)
            else:
                multidimension_get_vk = {'key':'pk', 'idx_name':'card'}
                for k, v in value_ts_scannable.items():
                    rs = self.get(self.leader, tid, 0, multidimension_get_vk, k[1])
                    infoLogger.info(rs)
                    if v is True:
                        self.assertTrue('|{}|'.format(k[0]) in rs)
                    else:
                         self.assertFalse('|{}|'.format(k[0]) in rs)
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
        if ttl.find("latest") == -1:
            self.multidimension_scan_vk = {'card': 'pk'}  # for multidimension
            rs1 = self.scan(self.leader, tid, 0, 'pk', self.now(), 1)
            infoLogger.info(rs1)
            for k, v in value_ts_scannable.items():
                if v is True:
                    self.assertTrue('|{}|'.format(k[0]) in rs1)
                else:
                    self.assertFalse('|{}|'.format(k[0]) in rs1)
        else:
            multidimension_get_vk = {'key':'pk', 'idx_name':'card'}
            for k, v in value_ts_scannable.items():
                rs = self.get(self.leader, tid, 0, multidimension_get_vk, k[1])
                infoLogger.info(rs)
                if v is True:
                    self.assertTrue('|{}|'.format(k[0]) in rs)
                else:
                     self.assertFalse('|{}|'.format(k[0]) in rs)
        self.drop(self.leader, tid, 0)


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
        self.create(self.leader, 't', self.tid, self.pid, ttl, 2, 'true')
        for i in value_ts_scannable.keys():
            # for multidimension test
            self.multidimension_vk = {'card': ('string:index', 'pk'),
                                      'merchant': ('string:index', '|{}|'.format(i[0])),
                                      'amt': ('double', 1.1)}
            self.put(self.leader, self.tid, self.pid, 'pk', i[1], '|{}|'.format(i[0]))
        time.sleep(1)

        multidimension_get_vk = {'key':'pk', 'idx_name':'card'}
        for k, v in value_ts_scannable.items():
            rs = self.get(self.leader, self.tid, self.pid, multidimension_get_vk, k[1])
            infoLogger.info(rs)
            if v is True:
                self.assertTrue('|{}|'.format(k[0]) in rs)
            else:
                self.assertFalse('|{}|'.format(k[0]) in rs)
        # put again after the last gc
        for i in put_value_ts:
            # for multidimension test
            self.multidimension_vk = {'card': ('string:index', 'pk'),
                                      'merchant': ('string:index', '|{}|'.format(i[0])),
                                      'amt': ('double', 1.1)}
            self.put(self.leader, self.tid, self.pid, 'pk', i[1], '|{}|'.format(i[0]))
        time.sleep(1)
        for k, v in value_ts_scannable2.items():
            rs = self.get(self.leader, self.tid, self.pid, multidimension_get_vk, k[1])
            infoLogger.info(rs)
            if v is True:
                self.assertTrue('|{}|'.format(k[0]) in rs)
            else:
                self.assertFalse('|{}|'.format(k[0]) in rs)



if __name__ == "__main__":
    load(TestTtl)
