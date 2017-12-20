# -*- coding: utf-8 -*-
import unittest
from testcasebase import TestCaseBase
import xmlrunner
import time
from libs.test_loader import load


class TestRecoverSnapshot(TestCaseBase):

    def test_recoversnapshot_after_pausesnapshot(self):
        """
        暂停snapshot后可以恢复snapshot
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid, 144000, 8, '')
        self.assertTrue('Create table ok' in rs1)
        rs2 = self.pausesnapshot(self.leader, self.tid, self.pid)
        self.assertTrue('PauseSnapshot ok' in rs2)
        table_status = self.get_table_status(self.leader, self.tid, self.pid)
        self.assertEqual(table_status, ['0', 'kTableLeader', 'kSnapshotPaused', 'true', '144000min', '0s'])
        rs3 = self.recoversnapshot(self.leader, self.tid, self.pid)
        self.assertTrue('RecoverSnapshot ok' in rs3)
        table_status = self.get_table_status(self.leader, self.tid, self.pid)
        self.assertEqual(table_status, ['0', 'kTableLeader', 'kTableNormal', 'true', '144000min', '0s'])


if __name__ == "__main__":
    import libs.test_loader
    load(TestRecoverSnapshot)
