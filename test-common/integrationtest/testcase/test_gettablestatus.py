# -*- coding: utf-8 -*-
from testcasebase import TestCaseBase
from libs.test_loader import load


class TestGetTableStatus(TestCaseBase):

    def test_gettablestatus_all(self):
        """
        查看所有表状态
        :return:
        """
        self.create(self.leader, 't', self.tid, self.pid)
        rs = self.create(self.leader, 't', self.tid, self.pid + 1)
        self.assertTrue('ok' in rs)
        table_status = self.get_table_status(self.leader)
        self.assertTrue(len(table_status) > 1)
        self.assertEqual(table_status[(self.tid, self.pid)][:6], ['0', 'kTableLeader', 'kTableNormal', 'true', '144000min', '0s'])


    def test_gettablestatus_tid_pid(self):
        """
        查看指定tid和pid的表状态
        :return:
        """
        rs = self.create(self.leader, 't', self.tid, self.pid)
        self.assertTrue('ok' in rs)
        table_status = self.get_table_status(self.leader, self.tid, self.pid)
        self.assertEqual(table_status[:6], ['0', 'kTableLeader', 'kTableNormal', 'true', '144000min', '0s'])


    def test_gettablestatus_making_snapshot(self):
        """
        makesnapshot的过程中查看标的状态会显示为kMakingSnapshot
        :return:
        """
        rs = self.create(self.leader, 't', self.tid, self.pid)
        self.assertTrue('ok' in rs)

        self.put_large_datas(1000, 10)

        rs2 = self.run_client(self.leader, 'makesnapshot {} {}'.format(self.tid, self.pid))
        self.assertTrue('MakeSnapshot ok' in rs2)

        table_status = self.get_table_status(self.leader)
        self.assertEqual('kMakingSnapshot' in table_status[(self.tid, self.pid)], True)


if __name__ == "__main__":
    load(TestGetTableStatus)
