# -*- coding: utf-8 -*-
import unittest
from framework import TestCaseBase
import xmlrunner

class TestCreateTable(TestCaseBase):

    def test_create_table(self):
        '''
        创建带有主从关系的表成功
        :return:
        '''
        rs1 = self.create(self.leader, 't', self.tid, self.pid, 144000, 2, 'true', self.slave1, self.slave2)
        self.assertTrue('Create table ok' in rs1)
        table_status = self.get_table_status(self.leader, self.tid, self.pid)
        self.assertEqual(table_status, ['0', 'kTableLeader', 'kTableNormal', '144000'])

        rs2 = self.create( self.slave1, 't', self.tid, self.pid, 144000, 2, 'false', self.slave1, self.slave2)
        self.assertTrue('Create table ok' in rs2)
        table_status = self.get_table_status(self.slave1, self.tid, self.pid)
        self.assertEqual(table_status, ['0', 'kTableFollower', 'kTableNormal', '144000'])

        rs3 = self.create(self.slave2, 't', self.tid, self.pid, 144000, 2, 'false', self.slave1, self.slave2)
        self.assertTrue('Create table ok' in rs3)
        table_status = self.get_table_status(self.slave1, self.tid, self.pid)
        self.assertEqual(table_status, ['0', 'kTableFollower', 'kTableNormal', '144000'])


if __name__ == "__main__":
    import sys
    suite = unittest.TestSuite()
    if len(sys.argv) == 1:
        suite = unittest.TestLoader().loadTestsFromTestCase(TestCreateTable)
    else:
        for test_name in sys.argv[1:]:
            suite.addTest(TestCreateTable(test_name))
    runner = xmlrunner.XMLTestRunner(output='test-common/integrationtest/test-reports')
    runner.run(suite)
