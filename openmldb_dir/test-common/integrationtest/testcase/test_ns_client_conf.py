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
import time
import os
from libs.test_loader import load
import libs.utils as utils
from libs.logger import infoLogger
from libs.deco import multi_dimension
import libs.ddt as ddt

@ddt.ddt
class TestConfSetGet(TestCaseBase):

    @ddt.data(
        ('false', 'true', 'set auto_failover ok','true'),
        ('false', 'TRUE', 'set auto_failover ok','true'),
        ('true', 'FALSE', 'set auto_failover ok','false'),
        ('true', 'FalsE', 'set auto_failover ok','false'),
        ('true', '0', 'failed to set auto_failover. error msg: invalid parameter', 'true'),
        ('true', 'FAlsee', 'failed to set auto_failover. error msg: invalid parameter', 'true'),
        ('true', 'true', 'set auto_failover ok','true'),
        ('true', 'false', 'set auto_failover ok','false'),
    )
    @ddt.unpack
    def test_auto_failover_confset(self, pre_set, set_value, msg, get_value):
        """

        :return:
        """
        self.confset(self.ns_leader, 'auto_failover', pre_set)
        rs = self.confset(self.ns_leader, 'auto_failover', set_value)
        self.assertIn(msg, rs)
        rs1 = self.confget(self.ns_leader, 'auto_failover')
        self.assertIn(get_value, rs1)
        if set_value == "true" or set_value == "TRUE":
            self.confset(self.ns_leader, 'auto_failover', 'false')


if __name__ == "__main__":
    load(TestConfSetGet)
