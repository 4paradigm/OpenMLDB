# -*- coding: utf-8 -*-
from testcasebase import TestCaseBase
import time
from libs.deco import multi_dimension
from libs.logger import infoLogger
import libs.ddt as ddt
from libs.test_loader import load


@ddt.ddt
class TestScan(TestCaseBase):

    @multi_dimension(True)
    @ddt.data(
        ({'card': ('string:index', '0'), 's2': ('int32', 7)}, {'card': '0'}, ' 7 '),
        ({'card': ('string', '1'), 's2': ('int32', 7)}, {'card': '1'}, 'Fail to scan table'),
        ({'card': ('string', '2'), 's2': ('int32', 77)}, {'card': '2', 's2': '77'},
         'Invalid args, tid pid should be uint32_t, st and et should be uint64_t'),
        ({'card': ('string:index', '3'), 's2': ('int32:index', 77), 's3': ('int32:index', 88)},
         {'s2': '77'}, ' 88 '),
        ({'card': ('string', '4'), 's2': ('int32', 77), 's3': ('int32', 88)},
         {'card': '4'}, 'Fail to scan table'),
    )
    @ddt.unpack
    def test_sscan_index(self, kv, scan_kv, scan_value):
        """
        创建高维表，对scan功能进行测试
        :return:
        """
        self.create(self.leader, 't', self.tid, self.pid, 144000, 2, 'true', **{k: v[0] for k, v in kv.items()})
        self.put(self.leader, self.tid, self.pid, '', self.now(), *[str(v[1]) for v in kv.values()])
        infoLogger.info(self.scan(self.leader, self.tid, self.pid, scan_kv, self.now(), 1))
        self.assertTrue(
            str(scan_value) in self.scan(self.leader, self.tid, self.pid, scan_kv, self.now(), 1))


    def test_sscan_ttl(self):
        """
        创建表，已经过期的数据无法再scan出来
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid, 1, 2)
        self.assertTrue('Create table ok' in rs1)
        self.put(self.leader, self.tid, self.pid, 'testkey0', self.now() - 1000000000, 'testvalue0')
        infoLogger.info(self.scan(self.leader, self.tid, self.pid, 'testkey0', self.now(), 1))
        self.assertTrue('testvalue0' in self.scan(
            self.leader, self.tid, self.pid, 'testkey0', self.now(), 1))
        time.sleep(61)
        self.assertFalse('testvalue0' in self.scan(
            self.leader, self.tid, self.pid, 'testkey0', self.now(), 1))


    @multi_dimension(True)
    def test_sscan_1index(self):
        """
        创建高维表，card设置index，scan 1 0 card0 3 0，失败
        :return:
        """
        kv = {'card': ('string:index', 'card0'), 's2': ('int32', 7)}
        self.create(self.leader, 't', self.tid, self.pid, 144000, 2, 'true', **{k: v[0] for k, v in kv.items()})
        self.put(self.leader, self.tid, self.pid, '', self.now(), *[str(v[1]) for v in kv.values()])
        rs1 = self.run_client(self.leader, 'scan {} {} {} {} {}'.format(self.tid, self.pid, 'card0', self.now(), 1))
        infoLogger.info(rs1)
        self.assertTrue('card0' in rs1)


if __name__ == "__main__":
    load(TestScan)
