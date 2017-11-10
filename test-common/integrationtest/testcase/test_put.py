# -*- coding: utf-8 -*-
import unittest
from framework import TestCaseBase
import threading
import time
import xmlrunner

class TestPut(TestCaseBase):

    def test_put_slave_sync(self):
        '''
        put到leader后，slave同步成功
        :return:
        '''
        rs1 = self.create(self.leader, 't', self.tid, self.pid, 144000, 2, 'true', self.slave1, self.slave2)
        self.assertTrue('Create table ok' in rs1)
        rs2 = self.create( self.slave1, 't', self.tid, self.pid, 144000, 2, 'false', self.slave1, self.slave2)
        self.assertTrue('Create table ok' in rs2)
        rs2 = self.put(self.leader,
                       self.tid,
                       self.pid,
                       'testkey0',
                       self.now(),
                       'testvalue0')
        self.assertTrue('Put ok' in rs2)
        self.assertTrue(
            'testvalue0' in self.scan(self.slave1, self.tid, self.pid, 'testkey0', self.now(), 1))


    def test_put_slave_cannot_put(self):
        '''
        slave不允许put
        :return:
        '''
        rs1 = self.create(self.leader, 't', self.tid, self.pid, 144000, 8, 'false', self.leader)
        self.assertTrue('Create table ok' in rs1)
        rs2 = self.put(self.leader,
                       self.tid,
                       self.pid,
                       'testkey0',
                       self.now(),
                       'testvalue0')
        self.assertTrue('Put failed' in rs2)


    def test_put_slave_killed_while_leader_putting(self):
        '''
        写数据过程中从节点挂掉，不影响主节点
        重新启动后可以loadtable成功，数据与主节点一致
        :return:
        '''
        rs1 = self.create(self.leader, 't', self.tid, self.pid, 144000, 8, 'true', self.slave1)
        self.assertTrue('Create table ok' in rs1)
        rs2 = self.create(self.slave1, 't', self.tid, self.pid, 144000, 8, 'false', self.slave1)
        self.assertTrue('Create table ok' in rs2)

        def put(count):
            for i in range(0, count):
                self.put(self.leader,
                         self.tid,
                         self.pid,
                         'testkey',
                         self.now() - 1,
                         'testvalue{}'.format(i))

        def stop_client(endpoint):
            self.stop_client(endpoint)

        threads = []
        threads.append(threading.Thread(
            target=put, args=(20,)))
        threads.append(threading.Thread(
            target=stop_client, args=(self.slave1,)))

        # 写入数据1s后节点挂掉
        for t in threads:
            t.start()
            time.sleep(2)
        for t in threads:
            t.join()

        self.start_client(self.slave1path)
        self.exe_shell('rm -rf {}/db/{}_{}/binlog'.format(self.slave1path, self.tid, self.pid))
        self.cp_db(self.leaderpath, self.slave1path, self.tid, self.pid)
        rs4 = self.loadtable(self.slave1, 't', self.tid, self.pid, 144000, 8, 'false', self.slave1)
        self.assertTrue('LoadTable ok' in rs4)
        time.sleep(1)
        self.assertTrue('testvalue19' in self.scan(self.slave1, self.tid, self.pid, 'testkey', self.now(), 1))


if __name__ == "__main__":
    import sys
    import os
    suite = unittest.TestSuite()
    if len(sys.argv) == 1:
        suite = unittest.TestLoader().loadTestsFromTestCase(TestPut)
    else:
        for test_name in sys.argv[1:]:
            suite.addTest(TestPut(test_name))
    runner = xmlrunner.XMLTestRunner(output=os.getenv('reportpath'))
    runner.run(suite)
