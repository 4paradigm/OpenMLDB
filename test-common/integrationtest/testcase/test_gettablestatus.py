# -*- coding: utf-8 -*-
from testcasebase import TestCaseBase
from libs.test_loader import load
import libs.ddt as ddt
from libs.logger import infoLogger


@ddt.ddt
class TestGetTableStatus(TestCaseBase):

    def test_gettablestatus_all(self):
        """
        查看所有表状态
        :return:
        """
        self.create(self.leader, 't', self.tid, self.pid)
        rs = self.create(self.leader, 't', self.tid, self.pid + 1)
        self.assertIn('ok', rs)
        table_status = self.get_table_status(self.leader)
        self.assertTrue(len(table_status) > 1)
        self.assertEqual(table_status[(self.tid, self.pid)][:6],
                         ['0', 'kTableLeader', 'kTableNormal', 'true', '144000min', '0s'])


    def test_gettablestatus_tid_pid(self):
        """
        查看指定tid和pid的表状态
        :return:
        """
        rs = self.create(self.leader, 't', self.tid, self.pid)
        self.assertIn('ok', rs)
        table_status = self.get_table_status(self.leader, self.tid, self.pid)
        self.assertEqual(table_status[:6],
                         ['0', 'kTableLeader', 'kTableNormal', 'true', '144000min', '0s'])


    def test_gettablestatus_making_snapshot(self):
        """
        makesnapshot的过程中查看标的状态会显示为kMakingSnapshot
        :return:
        """
        rs = self.create(self.leader, 't', self.tid, self.pid)
        self.assertIn('ok', rs)

        self.put_large_datas(1000, 10)

        rs2 = self.run_client(self.leader, 'makesnapshot {} {}'.format(self.tid, self.pid))
        self.assertIn('MakeSnapshot ok', rs2)

        table_status = self.get_table_status(self.leader)
        self.assertIn('kMakingSnapshot', table_status[(self.tid, self.pid)], True)


    def test_gettablestatus_memused_valuesize(self):
        """
        检查value变大时，mem使用量是否正常
        :return:
        """
        self.multidimension_vk = {'card': ('string:index', 'pk0'),
                                  'merchant': ('string:index', 'pk1'),
                                  'amt': ('string', 'a' * 100)}
        rs = self.create(self.leader, 't', self.tid, self.pid)
        self.assertIn('ok', rs)
        self.put_large_datas(1, 1, 'a' * 100)
        table_status = self.get_table_status(self.leader, self.tid, self.pid)
        memused = float(table_status[6])

        infoLogger.info(self.scan(self.leader, self.tid, self.pid, {'card':'pk0'}, self.now(), 1))

        self.pid = self.pid + 1
        rs = self.create(self.leader, 't', self.tid, self.pid)
        self.assertIn('ok', rs)
        self.multidimension_vk = {'card': ('string:index', 'pk0'),
                                  'merchant': ('string:index', 'pk1'),
                                  'amt': ('string', 'a' * 128)}
        self.put_large_datas(1, 1, 'a' * 128)
        table_status = self.get_table_status(self.leader, self.tid, self.pid)
        memused2 = float(table_status[6])

        infoLogger.info(self.scan(self.leader, self.tid, self.pid, {'card':'pk0'}, self.now(), 1))

        self.assertEqual(memused2 > memused, True)


    def test_gettablestatus_memused_datacount(self):
        """
        检查数据量变大时，mem使用量是否正常
        :return:
        """
        self.multidimension_vk = {'card': ('string:index', 'pk0'),
                                  'merchant': ('string:index', 'pk1'),
                                  'amt': ('string', 'a' * 100)}
        rs = self.create(self.leader, 't', self.tid, self.pid)
        self.assertIn('ok', rs)
        self.put_large_datas(10, 1)
        table_status = self.get_table_status(self.leader, self.tid, self.pid)
        memused = float(table_status[6])

        self.pid = self.pid + 1
        rs = self.create(self.leader, 't', self.tid, self.pid)
        self.assertIn('ok', rs)
        self.put_large_datas(20, 1)
        table_status = self.get_table_status(self.leader, self.tid, self.pid)
        memused2 = float(table_status[6])

        self.assertEqual(memused2 > memused, True)


if __name__ == "__main__":
    load(TestGetTableStatus)
