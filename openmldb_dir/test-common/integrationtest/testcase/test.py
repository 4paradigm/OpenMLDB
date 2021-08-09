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

import unittest
import sys,os
sys.path.append(os.getenv('testpath'))

from libs.ddt import ddt, data, unpack
import csv
from pprint import pprint


def add(a, b):
    print('*'*5 ,a, b)
    c = a + b
    print('c' ,c)
    return c

@ddt
class Test(unittest.TestCase):
    @data((1, 1, 2), (1, 2, 3))
    @unpack
    def test_addnum(self, a, b, expected_value):
        self.assertEqual(add(a, b), expected_value)


if __name__ == "__main__":
    import sys
    import os
    suite = unittest.TestSuite()
    if len(sys.argv) == 1:
        suite = unittest.TestLoader().loadTestsFromTestCase(Test)
    else:
        for test_name in sys.argv[1:]:
            suite.addTest(Test(test_name))
    runner = xmlrunner.XMLTestRunner(output=os.getenv('reportpath'), failfast=conf.failfast)
    runner.run(suite)
