# -*- coding: utf-8 -*-
import time
from testcasebase import TestCaseBase
from libs.deco import *
from libs.test_loader import load
import libs.utils as utils
from libs.logger import infoLogger


class TestSendSnapshot(TestCaseBase):

    def test_sendsnapshot_normal(self):
        """
        主表可以sendsnapshot给新的目标节点
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid, 144000, 2, 'true')
        self.assertTrue('Create table ok' in rs1)
        for i in range(100):
            self.put(self.leader, self.tid, self.pid, 'testkey', self.now() + 10000 - i, 'testvalue')
        rs2 = self.makesnapshot(self.leader, self.tid, self.pid)
        self.assertTrue('MakeSnapshot ok' in rs2)
        self.pausesnapshot(self.leader, self.tid, self.pid)
        rs3 = self.sendsnapshot(self.leader, self.tid, self.pid, self.slave1)
        self.assertTrue('SendSnapshot ok' in rs3)
        time.sleep(5)
        rs4 = self.loadtable(self.slave1, 't', self.tid, self.pid)
        self.assertTrue('LoadTable ok' in rs4)
        mf = self.get_manifest(self.slave1path, self.tid, self.pid)
        self.assertEqual(mf['offset'], '100')
        self.assertTrue(mf['name'])
        self.assertEqual(mf['count'], '100')


    def test_sendsnapshot_without_snapshot(self):
        """
        主表没有生成snapshot，不可以sendsnapshot给目标节点
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid, 144000, 2, 'true')
        self.assertTrue('Create table ok' in rs1)
        self.put(self.leader, self.tid, self.pid, 'testkey', self.now(), 'testvalue')
        rs2 = self.sendsnapshot(self.leader, self.tid, self.pid, self.slave1)
        self.assertTrue('Fail to SendSnapshot' in rs2)


    def test_sendsnapshot_no_paused(self):
        """
        主表没有pausesnapshot，不可以sendsnapshot给目标节点
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid, 144000, 2, 'true')
        self.assertTrue('Create table ok' in rs1)
        self.put(self.leader, self.tid, self.pid, 'testkey', self.now(), 'testvalue')
        rs2 = self.makesnapshot(self.leader, self.tid, self.pid)
        self.assertTrue('MakeSnapshot ok' in rs2)
        rs3 = self.sendsnapshot(self.leader, self.tid, self.pid, self.slave1)
        self.assertTrue('Fail to SendSnapshot' in rs3)


    def test_sendsnapshot_slave(self):
        """
        目标从表不能执行sendsnapshot命令
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid, 144000, 2, 'false')
        self.assertTrue('Create table ok' in rs1)
        self.put(self.leader, self.tid, self.pid, 'testkey', self.now(), 'testvalue')
        rs2 = self.makesnapshot(self.leader, self.tid, self.pid)
        self.assertTrue('MakeSnapshot ok' in rs2)
        self.pausesnapshot(self.leader, self.tid, self.pid)
        rs3 = self.sendsnapshot(self.leader, self.tid, self.pid, self.slave1)
        self.assertTrue('Fail to SendSnapshot' in rs3)


    def test_sendsnapshot_table_exist(self):
        """
        目标从表存在时，主表sendsnapshot失败
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid, 144000, 2, 'true')
        self.assertTrue('Create table ok' in rs1)
        rs2 = self.create(self.slave1, 't', self.tid, self.pid, 144000, 2, 'false')
        self.assertTrue('Create table ok' in rs2)
        self.put(self.leader, self.tid, self.pid, 'testkey', self.now(), 'testvalue')
        rs3 = self.makesnapshot(self.leader, self.tid, self.pid)
        self.assertTrue('MakeSnapshot ok' in rs3)
        self.pausesnapshot(self.leader, self.tid, self.pid)
        rs4 = self.sendsnapshot(self.leader, self.tid, self.pid, self.slave1)
        self.assertTrue('SendSnapshot ok' in rs4)


if __name__ == "__main__":
    load(TestSendSnapshot)
