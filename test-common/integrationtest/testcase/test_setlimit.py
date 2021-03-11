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
import libs.ddt as ddt
from libs.test_loader import load



@ddt.ddt
class TestSetLimit(TestCaseBase):
    @ddt.data(
        ('setlimit', 'Put', 10, 'Set Limit ok'),
        ('setlimit', 'Put', 0, 'Set Limit ok'),
        ('setlimit', 'Put', 1, 'Set Limit ok'),
        ('setlimit', 'Put', 2147483647, 'Set Limit ok'),
        ('setlimit', 'Put', -1, 'Fail to set limit'),
        ('setlimit', 'Put', 1.5, 'Bad set limit format'),
        ('setlimit', 'Get', 10, 'Set Limit ok'),
        ('setlimit', 'Get', 0, 'Set Limit ok'),
        ('setlimit', 'Get', 1, 'Set Limit ok'),
        ('setlimit', 'Get', 2147483647, 'Set Limit ok'),
        ('setlimit', 'Get', -1, 'Fail to set limit'),
        ('setlimit', 'Get', 1.5, 'Bad set limit format'),
        ('setlimit', 'Scan', 10, 'Set Limit ok'),
        ('setlimit', 'Scan', 0, 'Set Limit ok'),
        ('setlimit', 'Scan', 1, 'Set Limit ok'),
        ('setlimit', 'Scan', 2147483647, 'Set Limit ok'),
        ('setlimit', 'Scan', -1, 'Fail to set limit'),
        ('setlimit', 'Scan', 1.5, 'Bad set limit format'),
        ('setlimit', 'Server', 10, 'Set Limit ok'),
        ('setlimit', 'Server', 0, 'Set Limit ok'),
        ('setlimit', 'Server', 1, 'Set Limit ok'),
        ('setlimit', 'Server', 2147483647, 'Set Limit ok'),
        ('setlimit', 'Server', -1, 'Fail to set limit'),
        ('setlimit', 'Server', 1.5, 'Bad set limit format'),
        ('setLimit', 'Scan', 1, 'unsupported cmd')
    )
    @ddt.unpack
    def test_set_max_concurrency(self, command, method, max_concurrency_limit, rsp_msg):
        """
        修改并发限制的数值
        :param self:
        :param max_concurrency_limit:
        :return:
        """
        rs1 = self.ns_setlimit(self.leader, command, method, max_concurrency_limit)
        self.assertIn(rsp_msg, rs1)


if __name__ == "__main__":
    load(TestSetLimit)

