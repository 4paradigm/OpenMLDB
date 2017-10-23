# -*- coding: utf-8 -*-
import unittest
from framework import TestCaseBase
import xmlrunner

class TestGetTableStatus(TestCaseBase):

    def test_gettablestatus_all(self):
        '''
        查看所有表状态
        :return:
        '''
        rs = self.create(self.leader, 't', self.tid, self.pid)
        self.assertTrue('ok' in rs)
        table_status = self.get_table_status(self.leader)
        self.assertTrue(len(table_status) > 1)
        self.assertEqual(table_status[(0, 0)], ['0', 'kTableFollower', 'kTableUndefined', '43200'])
        self.assertEqual(table_status[(self.tid, self.pid)], ['0', 'kTableLeader', 'kTableNormal', '144000'])


    def test_gettablestatus_tid_pid(self):
        '''
        查看指定tid和pid的表状态
        :return:
        '''
        rs = self.create(self.leader, 't', self.tid, self.pid)
        self.assertTrue('ok' in rs)
        table_status = self.get_table_status(self.leader, self.tid, self.pid)
        self.assertEqual(table_status, ['0', 'kTableLeader', 'kTableNormal', '144000'])


    def test_gettablestatus_making_snapshot(self):
        '''
        makesnapshot的过程中查看标的状态会显示为kMakingSnapshot
        :return:
        '''
        rs = self.create(self.leader, 't', self.tid, self.pid)
        self.assertTrue('ok' in rs)

        self.put_large_datas(100, 50)

        rs2 = self.run_client(self.leader, 'makesnapshot {} {}'.format(self.tid, self.pid))
        self.assertTrue('MakeSnapshot ok' in rs2)

        table_status = self.get_table_status(self.leader)
        self.assertEqual(table_status[(self.tid, self.pid)], ['5000', 'kTableLeader', 'kMakingSnapshot', '144000'])


if __name__ == "__main__":
    import sys
    import os
    suite = unittest.TestSuite()
    if len(sys.argv) == 1:
        suite = unittest.TestLoader().loadTestsFromTestCase(TestGetTableStatus)
    else:
        for test_name in sys.argv[1:]:
            suite.addTest(TestGetTableStatus(test_name))
    runner = xmlrunner.XMLTestRunner(output=os.getenv('reportpath'))
    runner.run(suite)