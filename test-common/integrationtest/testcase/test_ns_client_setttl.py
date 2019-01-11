# -*- coding: utf-8 -*-
from testcasebase import TestCaseBase
import threading
import time
from libs.deco import multi_dimension
from libs.logger import infoLogger
import libs.ddt as ddt
from libs.test_loader import load
import libs.utils as utils


@ddt.ddt
class TestSetTTL(TestCaseBase):

    def test_set_ttl_normal(self):
        """
        测试setttl函数，正常修改ttl值，查看返回值
        :return:
        """
        rs_absolute1 = self.ns_create_cmd(self.ns_leader, 't1', '10', str(8), str(3), '')
        rs_latest1 = self.ns_create_cmd(self.ns_leader, 'latest1', 'latest:10', str(8), str(3), '')
        self.assertIn('Create table ok', rs_absolute1)
        self.assertIn('Create table ok', rs_latest1)


        rs_absolute2 = self.check_setttl_ns_client(self.ns_leader, 'setttl', 't1', 'absolute', 1)
        rs_latest2 = self.check_setttl_ns_client(self.ns_leader, 'setttl', 'latest1', 'latest', 1)
        self.assertIn('Set ttl ok !', rs_absolute2)
        self.assertIn('Set ttl ok !', rs_latest2)


        rs_absolute3 = self.check_setttl_ns_client(self.ns_leader, 'setttl', 't1', 'absolute', -1)
        rs_latest3 = self.check_setttl_ns_client(self.ns_leader, 'setttl', 'latest1', 'latest', -1)
        self.assertIn('Set ttl failed! fail to update ttl from tablet', rs_absolute3)
        self.assertIn('Set ttl failed! fail to update ttl from tablet', rs_latest3)


    def test_set_ttl_expired(self):
        """
        测试修改后的ttl值，scan后的数据被删除。两种类型latest和absolute都要测试
        :return:
        """
        rs_absolute1 = self.ns_create_cmd(self.ns_leader, 't1', '10', str(8), str(3), '')
        rs_latest1 = self.ns_create_cmd(self.ns_leader, 'latest1', 'latest:10', str(8), str(3), '')
        self.assertIn('Create table ok', rs_absolute1)
        self.assertIn('Create table ok', rs_latest1)
        rs_absolute2 = self.check_setttl_ns_client(self.ns_leader, 'setttl', 't1', 'absolute', 1)
        rs_latest2 = self.check_setttl_ns_client(self.ns_leader, 'setttl', 'latest1', 'latest', 1)
        self.assertIn('Set ttl ok !', rs_absolute2)
        self.assertIn('Set ttl ok !', rs_latest2)

        rs_time = self.now()
        rs_absolute3 = self.ns_put_kv(self.ns_leader, 't1', 'testkey0', str(rs_time), 'testvalue0')
        rs_latest3 = self.ns_put_kv(self.ns_leader, 'latest1', 'testkey0', str(rs_time), 'testvalue0')
        self.assertIn('Put ok', rs_absolute3)
        self.assertIn('Put ok', rs_latest3)

        rs_absolute4 = self.ns_get_kv(self.ns_leader, 't1', 'testkey0', str(rs_time))
        rs_latest4 = self.ns_get_kv(self.ns_leader, 'latest1', 'testkey0', str(rs_time))
        self.assertIn('testvalue0', rs_absolute4)
        self.assertIn('testvalue0', rs_latest4)

        rs_latest5 = self.ns_put_kv(self.ns_leader, 'latest1', 'testkey0', str(rs_time), 'testvalue1')
        self.assertIn('Put ok', rs_latest5)
        time.sleep(1)

        rs_latest6 = self.ns_get_kv(self.ns_leader, 'latest1', 'testkey0', str(rs_time))
        self.assertFalse('testvalue0' in rs_latest6)
        rs_absolute5 = self.ns_scan_kv(self.ns_leader, 't1', 'testkey0', str(rs_time), '0', ' ')
        self.assertTrue('testvalue0' in rs_absolute5)
        time.sleep(61)

        rs_absolute5 = self.ns_scan_kv(self.ns_leader, 't1', 'testkey0', str(rs_time), '0', ' ')
        self.assertFalse('testvalue0' in rs_absolute5)


if __name__ == "__main__":
    load(TestSetTTL)















