# -*- coding: utf-8 -*-
import unittest
from framework import TestCaseBase
import time
import threading


class TestGetTableStatus(TestCaseBase):

    def test_gettablestatus_all(self):
        '''

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

        :return:
        '''
        rs = self.create(self.leader, 't', self.tid, self.pid)
        self.assertTrue('ok' in rs)
        table_status = self.get_table_status(self.leader, self.tid, self.pid)
        self.assertEqual(table_status, ['0', 'kTableLeader', 'kTableNormal', '144000'])


    def test_gettablestatus_making_snapshot(self):
        '''

        :return:
        '''
        rs = self.create(self.leader, 't', self.tid, self.pid)
        self.assertTrue('ok' in rs)
        for i in range(0, 100):
            self.put(self.leader, self.tid, self.pid, 'testkey', self.now() - i, 'testvalue'*10000)

        rs_list = []
        def gettablestatus(endpoint):
            rs = self.get_table_status(endpoint, self.tid, self.pid)
            rs_list.append(rs)
        def makesnapshot(endpoint):
            rs = self.run_client(endpoint, 'makesnapshot {} {}'.format(self.tid, self.pid))
            rs_list.append(rs)

        threads = []
        threads.append(threading.Thread(
            target=makesnapshot, args=(self.leader,)))
        threads.append(threading.Thread(
            target=gettablestatus, args=(self.leader,)))

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertTrue('MakeSnapshot ok' in rs_list)
        self.assertTrue(['100', 'kTableLeader', 'kMakingSnapshot', '144000'] in rs_list)


if __name__ == "__main__":
    import sys
    suite = unittest.TestSuite()
    if len(sys.argv) == 1:
        suite = unittest.TestLoader().loadTestsFromTestCase(TestGetTableStatus)
    else:
        for test_name in sys.argv[1:]:
            suite.addTest(TestGetTableStatus(test_name))
    unittest.TextTestRunner(verbosity=2).run(suite)