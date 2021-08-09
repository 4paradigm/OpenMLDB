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
import time
from testcasebase import TestCaseBase
from libs.deco import *
from libs.test_loader import load
import libs.utils as utils
from libs.logger import infoLogger

@multi_dimension(True)
class TestShowTablet(TestCaseBase):

    def test_showtablet_healthy(self):
        """
        健康的节点，状态为kTabletHealthy
        :return:
        """
        rs1 = self.showtablet(self.ns_leader)
        infoLogger.info(rs1)
        self.assertEqual(rs1[self.leader][0], 'kTabletHealthy')
        self.assertEqual(rs1[self.slave1][0], 'kTabletHealthy')
        self.assertEqual(rs1[self.slave2][0], 'kTabletHealthy')


    def test_showtablet_offline(self):
        """
        挂掉的节点，状态为kTabletOffline，启动后恢复为kTabletHealthy
        :return:
        """
        self.stop_client(self.slave1)
        time.sleep(10)
        rs1 = self.showtablet(self.ns_leader)
        infoLogger.info(rs1)
        self.assertEqual(rs1[self.leader][0], 'kTabletHealthy')
        self.assertEqual(rs1[self.slave1][0], 'kTabletOffline')
        self.assertEqual(rs1[self.slave2][0], 'kTabletHealthy')
        self.start_client(self.slave1)
        time.sleep(5)
        rs2 = self.showtablet(self.ns_leader)
        self.assertEqual(rs2[self.slave1][0], 'kTabletHealthy')


if __name__ == "__main__":
    load(TestShowTablet)
