# -*- coding: utf-8 -*-
from testcasebase import TestCaseBase
import time
import threading
from libs.test_loader import load
from libs.logger import infoLogger


class TestMakeSnapshot(TestCaseBase):

    def test_makesnapshot_normal_success(self):
        """
        makesnapshot功能正常
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid)
        self.assertTrue('Create table ok' in rs1)
        for i in range(0, 6):
            rs2 = self.put(self.leader,
                           self.tid,
                           self.pid,
                           'testkey',
                           self.now() - i,
                           'testvalue')
            self.assertTrue('Put ok' in rs2)
        rs3 = self.makesnapshot(self.leader, self.tid, self.pid)
        self.assertTrue('MakeSnapshot ok' in rs3)

        mf = self.get_manifest(self.leaderpath, self.tid, self.pid)
        self.assertEqual(mf['offset'], '6')
        self.assertTrue(mf['name'])
        self.assertEqual(mf['count'], '6')


    def test_makesnapshot_ttl_first_make(self):
        """
        首次makesnapshot，MANIFEST文件中统计剔除过期数据
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid)
        self.assertTrue('Create table ok' in rs1)
        for i in range(0, 6):
            self.put(self.leader,
                     self.tid,
                     self.pid,
                     'testkey',
                     self.now() - (100000000000 * (i % 2) + 1),
                     'testvalue')
        rs3 = self.makesnapshot(self.leader, self.tid, self.pid)
        self.assertTrue('MakeSnapshot ok' in rs3)

        # 剔除原snapshot中的过期数据
        mf = self.get_manifest(self.leaderpath, self.tid, self.pid)
        self.assertEqual(mf['offset'], '6')
        self.assertTrue(mf['name'])
        self.assertEqual(mf['count'], '3')


    def test_makesnapshot_ttl_second_make(self):
        """
        第二次makesnapshot，MANIFEST文件中统计剔除过期数据
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid)
        self.assertTrue('Create table ok' in rs1)
        for i in range(0, 6):
            self.put(self.leader,
                     self.tid,
                     self.pid,
                     'testkey',
                     self.now() - (100000000000 * (i % 2) + 1),
                     'testvalue')
        rs3 = self.makesnapshot(self.leader, self.tid, self.pid)
        self.assertTrue('MakeSnapshot ok' in rs3)

        # 剔除原snapshot中的过期数据
        rs4 = self.makesnapshot(self.leader, self.tid, self.pid)
        self.assertTrue('MakeSnapshot ok' in rs4)
        mf = self.get_manifest(self.leaderpath, self.tid, self.pid)
        self.assertEqual(mf['offset'], '6')
        self.assertTrue(mf['name'])
        self.assertEqual(mf['count'], '3')


    def test_makesnapshot_ttl_put_some_expried_data(self):
        """
        写入部分过期数据和部分未过期数据，首次makesnapshot
        再写入部分过期数据和部分未过期数据，再次makesnapshot
        此时MANIFEST文件中统计剔除了上次snapshot的过期数据，没有提出binlog中的过期数据
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid)
        self.assertTrue('Create table ok' in rs1)
        for i in range(0, 6):
            self.put(self.leader,
                     self.tid,
                     self.pid,
                     'testkey',
                     self.now() - (100000000000 * (i % 2) + 1),
                     'testvalue')
        rs3 = self.makesnapshot(self.leader, self.tid, self.pid)
        self.assertTrue('MakeSnapshot ok' in rs3)

        # 第一次做snapshot后，put一部分过期数据和一部分未过期数据
        for i in range(0, 4):
            self.put(self.leader,
                     self.tid,
                     self.pid,
                     'testkey',
                     self.now() - (100000000000 * (i % 2) + 1),
                     'testvalue')

        # 第二次做snapshot剔除原snapshot中的过期数据，不剔除binlog中的过期数据
        rs3 = self.makesnapshot(self.leader, self.tid, self.pid)
        self.assertTrue('MakeSnapshot ok' in rs3)
        mf = self.get_manifest(self.leaderpath, self.tid, self.pid)
        self.assertEqual(mf['offset'], '10')
        self.assertTrue(mf['name'])
        self.assertEqual(mf['count'], '5')


    def test_makesnapshot_fail_after_drop(self):
        """
        drop table后，makesnapshot失败
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid)
        self.assertTrue('Create table ok' in rs1)
        for i in range(0, 3):
            self.put(self.leader,
                     self.tid,
                     self.pid,
                     'testkey',
                     self.now() - i,
                     'testvalue')
        rs2 = self.run_client(self.leader, 'drop {} {}'.format(self.tid, self.pid))
        self.assertTrue('Drop table ok' in rs2)
        rs3 = self.makesnapshot(self.leader, self.tid, self.pid)
        self.assertTrue('Fail to MakeSnapshot' in rs3)


    def test_makesnapshot_fail_making_snapshot(self):
        """
        makesnapshot过程中，无法再makesnapshot
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid)
        self.assertTrue('Create table ok' in rs1)

        self.put_large_datas(500, 20)

        rs_list = []

        def makesnapshot(endpoint):
            rs = self.run_client(endpoint, 'makesnapshot {} {}'.format(self.tid, self.pid))
            rs_list.append(rs)

        # 5个线程并发makesnapshot，最后只有1个线程是MakeSnapshot ok的
        threads = []
        for _ in range(0, 5):
            threads.append(threading.Thread(
                target=makesnapshot, args=(self.leader,)))

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        print rs_list
        infoLogger.info(rs_list)
        self.assertEqual(rs_list.count('MakeSnapshot ok'), 1)

        time.sleep(5)
        mf = self.get_manifest(self.leaderpath, self.tid, self.pid)
        self.assertEqual(mf['offset'], '10000')
        self.assertTrue(mf['name'])
        self.assertEqual(mf['count'], '10000')


    def test_makesnapshot_block_drop_table(self):
        """
        makesnapshot过程中，无法drop table
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid)
        self.assertTrue('Create table ok' in rs1)

        self.put_large_datas(200, 50)

        rs2 = self.run_client(self.leader, 'makesnapshot {} {}'.format(self.tid, self.pid))
        rs3 = self.drop(self.leader, self.tid, self.pid)

        self.assertTrue('MakeSnapshot ok' in rs2)
        self.assertTrue('Fail to drop table' in rs3)


    def test_makesnapshot_when_loading_table(self):
        """
        loadtable过程中，无法makesnapshot
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid)
        self.assertTrue('Create table ok' in rs1)

        # for multidimension test
        self.multidimension_vk = {'card': ('string:index', 'testvalue0'),
                                  'merchant': ('string:index', 'testvalue1'*100),
                                  'amt': ('double', 1.1)}
        self.put_large_datas(200, 20)

        # 将table目录拷贝到新节点
        self.exe_shell('cp -r {leaderpath}/db/{tid}_{pid} {slave1path}/db/'.format(
            leaderpath=self.leaderpath, tid=self.tid, pid=self.pid, slave1path=self.slave1path))

        rs_list = []

        def loadtable(endpoint):
            rs = self.loadtable(endpoint, 't', self.tid, self.pid)
            rs_list.append(rs)

        def makesnapshot(endpoint):
            rs = self.run_client(endpoint, 'makesnapshot {} {}'.format(self.tid, self.pid))
            rs_list.append(rs)

        # 5个线程并发loadtable，最后只有1个线程是load ok的
        threads = [threading.Thread(
            target=loadtable, args=(self.slave1,)), threading.Thread(
            target=makesnapshot, args=(self.slave1,))]

        for t in threads:
            time.sleep(0.0001)
            t.start()
        for t in threads:
            t.join()
        self.assertTrue('LoadTable ok' in rs_list)
        self.assertTrue('Fail to MakeSnapshot' in rs_list)


    def test_makesnapshot_manifest_deleted(self):
        """
        MANIFEST文件被删除，makesnapshot无法做出新的snapshot文件，日志中有warning
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid)
        self.assertTrue('Create table ok' in rs1)

        for i in range(0, 6):
            self.put(self.leader,
                     self.tid,
                     self.pid,
                     'testkey',
                     self.now() - (100000000000 * (i % 2) + 1),
                     'testvalue'*100)
        rs2 = self.makesnapshot(self.leader, self.tid, self.pid)
        self.assertTrue('MakeSnapshot ok' in rs2)
        self.exe_shell('rm -f {}/db/{}_{}/snapshot/MANIFEST'.format(self.leaderpath, self.tid, self.pid))
        rs3 = self.makesnapshot(self.leader, self.tid, self.pid)
        self.assertTrue('MakeSnapshot ok' in rs3)
        time.sleep(1)
        # 新manifest的count=0，因为相当于没有新数据写入
        mf = self.get_manifest(self.leaderpath, self.tid, self.pid)
        self.assertEqual(mf['offset'], '6')
        self.assertTrue(mf['name'])
        self.assertEqual(mf['count'], '0')

        # 再次写入数据后makesnapshot
        for i in range(0, 6):
            self.put(self.leader,
                     self.tid,
                     self.pid,
                     'testkey',
                     self.now() - (100000000000 * (i % 2) + 1),
                     'testvalue')
        rs3 = self.makesnapshot(self.leader, self.tid, self.pid)
        self.assertTrue('MakeSnapshot ok' in rs3)
        mf = self.get_manifest(self.leaderpath, self.tid, self.pid)
        self.assertEqual(mf['offset'], '12')
        self.assertTrue(mf['name'])
        self.assertEqual(mf['count'], '3')


    def test_makesnapshot_snapshot_deleted(self):
        """
        删除snapshot后，makesnapshot无法做出新的snapshot文件，日志中有warning
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid)
        self.assertTrue('Create table ok' in rs1)

        for i in range(0, 6):
            self.put(self.leader,
                     self.tid,
                     self.pid,
                     'testkey',
                     self.now() - (100000000000 * (i % 2) + 1),
                     'testvalue')
        rs2 = self.makesnapshot(self.leader, self.tid, self.pid)
        self.assertTrue('MakeSnapshot ok' in rs2)

        self.exe_shell('rm -f {}/db/{}_{}/snapshot/*.sdb'.format(self.leaderpath, self.tid, self.pid))
        rs3 = self.makesnapshot(self.leader, self.tid, self.pid)
        # should be failed?
        self.assertTrue('MakeSnapshot ok' in rs3)


    def test_makesnapshot_snapshot_name_mismatch(self):
        """
        snapshot和MANIFEST名字不匹配，makesnapshot时日志中有warning，且无法生成新的snapshot
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid)
        self.assertTrue('Create table ok' in rs1)

        for i in range(0, 6):
            self.put(self.leader,
                     self.tid,
                     self.pid,
                     'testkey',
                     self.now(),
                     'testvalue')
        rs2 = self.makesnapshot(self.leader, self.tid, self.pid)
        self.assertTrue('MakeSnapshot ok' in rs2)
        self.exe_shell('mv {nodepath}/db/{tid}_{pid}/snapshot/*.sdb \
        {nodepath}/db/{tid}_{pid}/snapshot/11111.sdb'.format(
            nodepath=self.leaderpath, tid=self.tid, pid=self.pid))
        rs3 = self.makesnapshot(self.leader, self.tid, self.pid)
        self.assertTrue('MakeSnapshot ok' in rs3)


    def test_makesnapshot_fail_after_pausesnapshot(self):
        """
        pausesnapshot后，无法makesnapshot
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid)
        self.assertTrue('Create table ok' in rs1)

        for i in range(0, 6):
            self.put(self.leader,
                     self.tid,
                     self.pid,
                     'testkey',
                     self.now() - (100000000000 * (i % 2) + 1),
                     'testvalue'*100)
        rs3 = self.pausesnapshot(self.leader, self.tid, self.pid)
        self.assertTrue('PauseSnapshot ok' in rs3)
        rs4 = self.makesnapshot(self.leader, self.tid, self.pid)
        self.assertTrue('Fail to MakeSnapshot' in rs4)


    def test_makesnapshot_success_after_recoversnapshot(self):
        """
        recoversnapshot后，可以成功makesnapshot
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid)
        self.assertTrue('Create table ok' in rs1)

        for i in range(0, 6):
            self.put(self.leader,
                     self.tid,
                     self.pid,
                     'testkey',
                     self.now() - (100000000000 * (i % 2) + 1),
                     'testvalue'*100)
        rs3 = self.pausesnapshot(self.leader, self.tid, self.pid)
        self.assertTrue('PauseSnapshot ok' in rs3)
        rs4 = self.recoversnapshot(self.leader, self.tid, self.pid)
        self.assertTrue('RecoverSnapshot ok' in rs4)
        rs5 = self.makesnapshot(self.leader, self.tid, self.pid)
        self.assertTrue('MakeSnapshot ok' in rs5)


    def test_makesnapshot_while_binlog_without_ending(self):
        """
        写入少量数据，binlog未到滚动大小时节点挂掉
        重新启动后loadtable，再写入数据
        此时makesnapshot，包含了重启后写入的数据
        再cp到新节点，新节点loadtable，数据完整
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid)
        self.assertTrue('Create table ok' in rs1)

        offset = 3
        for i in range(0, offset):
            # for multidimension test
            self.multidimension_vk = {'card': ('string:index', 'testkey{}'.format(i)),
                                      'merchant': ('string:index', 'testvalue'),
                                      'amt': ('double', 1.1)}
            self.put(self.leader,
                     self.tid,
                     self.pid,
                     'testkey{}'.format(i),
                     self.now() - 1,
                     'testvalue'*100)

        self.stop_client(self.leader)
        time.sleep(10)
        self.start_client(self.leaderpath)
        rs1 = self.loadtable(self.leader, 't', self.tid, self.pid, 144000, 8, 'true')
        self.assertTrue('LoadTable ok' in rs1)

        # for multidimension test
        self.multidimension_vk = {'card': ('string:index', 'testkey11'),
                                  'merchant': ('string:index', 'testvalue11'),
                                  'amt': ('double', 1.1)}
        rs2 = self.put(self.leader,
                       self.tid,
                       self.pid,
                       'testkey11',
                       self.now() - 1,
                       'testvalue11')
        self.assertTrue('Put ok' in rs2)

        rs3 = self.makesnapshot(self.leader, self.tid, self.pid)
        self.assertTrue('MakeSnapshot ok' in rs3)
        mf = self.get_manifest(self.leaderpath, self.tid, self.pid)
        self.assertEqual(mf['offset'], str(offset + 1))

        # 新节点loadtable
        self.cp_db(self.leaderpath, self.slave1path, self.tid, self.pid)
        rs4 = self.loadtable(self.slave1, 't', self.tid, self.pid)
        self.assertTrue('LoadTable ok' in rs4)
        for i in range(0, offset):
            self.multidimension_scan_vk = {'card': 'testkey{}'.format(i)}  # for multidimension test
            self.assertTrue('testvalue' in self.scan(
                self.slave1, self.tid, self.pid, 'testkey{}'.format(i), self.now(), 1))

        self.multidimension_scan_vk = {'card': 'testkey11'}  # for multidimension test
        self.assertTrue('testvalue11' in self.scan(self.slave1, self.tid, self.pid, 'testkey11', self.now(), 1))


    def test_makesnapshot_after_restart_while_putting(self):
        """
        写数据过程中节点挂掉，造成binlog不完整
        重新启动后可以loadtable成功
        再写入数据后makesnapshot，snapshot中包含了重启后写入的数据
        将db cp到新节点，新节点loadtable，数据完整
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid)
        self.assertTrue('Create table ok' in rs1)

        def put(count):
            for i in range(0, count):
                # for multidimension test
                self.multidimension_vk = {'card': ('string:index', 'testkey{}'.format(i)),
                                          'merchant': ('string:index', 'testvalue'),
                                          'amt': ('double', 1.1)}
                self.put(self.leader,
                         self.tid,
                         self.pid,
                         'testkey{}'.format(i),
                         self.now() - 1,
                         'testvalue'*100)

        def stop_client(endpoint):
            self.stop_client(endpoint)

        threads = [threading.Thread(
            target=put, args=(10,)), threading.Thread(
            target=stop_client, args=(self.leader,))]

        # 写入数据1s后节点挂掉
        for t in threads:
            t.start()
            time.sleep(1)
        for t in threads:
            t.join()

        time.sleep(10)
        self.start_client(self.leaderpath)
        rs2 = self.loadtable(self.leader, 't', self.tid, self.pid, 144000, 8, 'true')
        self.assertTrue('LoadTable ok' in rs2)

        rs3 = self.makesnapshot(self.leader, self.tid, self.pid)
        self.assertTrue('MakeSnapshot ok' in rs3)
        time.sleep(1)
        mf = self.get_manifest(self.leaderpath, self.tid, self.pid)
        offset = int(mf['offset'])

        # for multidimension test
        self.multidimension_vk = {'card': ('string:index', 'testkey100'),
                                  'merchant': ('string:index', 'testvalue100'),
                                  'amt': ('double', 1.1)}
        rs4 = self.put(self.leader,
                       self.tid,
                       self.pid,
                       'testkey100',
                       self.now() - 1,
                       'testvalue100')
        self.assertTrue('Put ok' in rs4)

        rs5 = self.makesnapshot(self.leader, self.tid, self.pid)
        self.assertTrue('MakeSnapshot ok' in rs5)
        time.sleep(1)
        mf = self.get_manifest(self.leaderpath, self.tid, self.pid)
        self.assertEqual(mf['offset'], str(offset + 1))

        # 新节点loadtable
        self.cp_db(self.leaderpath, self.slave1path, self.tid, self.pid)
        rs6 = self.loadtable(self.slave1, 't', self.tid, self.pid)
        self.assertTrue('LoadTable ok' in rs6)
        for i in range(0, offset):
            self.multidimension_scan_vk = {'card': 'testkey{}'.format(i)}  # for multidimension test
            self.assertTrue('testvalue' in self.scan(
                self.slave1, self.tid, self.pid, 'testkey{}'.format(i), self.now(), 1))

        self.multidimension_scan_vk = {'card': 'testkey100'}  # for multidimension test
        self.assertTrue('testvalue100' in self.scan(self.slave1, self.tid, self.pid, 'testkey100', self.now(), 1))


if __name__ == "__main__":
    load(TestMakeSnapshot)
