#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright 2021 4Paradigm
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# -*- coding: utf-8 -*-
from testcasebase import TestCaseBase
from libs.test_loader import load


class TestRecoverSnapshot(TestCaseBase):

    def test_recoversnapshot_after_pausesnapshot(self):
        """
        暂停snapshot后可以恢复snapshot
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid, 144000, 8)
        self.assertIn('Create table ok', rs1)
        rs2 = self.pausesnapshot(self.leader, self.tid, self.pid)
        self.assertIn('PauseSnapshot ok', rs2)
        table_status = self.get_table_status(self.leader, self.tid, self.pid)
        self.assertEqual(table_status[:6], ['0', 'kTableLeader', 'kSnapshotPaused', 'true', '144000min', '0s'])
        rs3 = self.recoversnapshot(self.leader, self.tid, self.pid)
        self.assertIn('RecoverSnapshot ok', rs3)
        table_status = self.get_table_status(self.leader, self.tid, self.pid)
        self.assertEqual(table_status[:6], ['0', 'kTableLeader', 'kTableNormal', 'true', '144000min', '0s'])


if __name__ == "__main__":
    load(TestRecoverSnapshot)
