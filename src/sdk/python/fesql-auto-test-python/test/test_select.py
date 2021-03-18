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


import common.fesql_test as fesql_test
from ddt import ddt,file_data,unpack,data,idata
import unittest
import executor.fesql_executor
import util.tools as tool

@ddt
class TestSelect(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.engine = fesql_test.getEngine()
        cls.connection = cls.engine.connect()

    def testEmpty(self):
        print('testEmpty')

    @idata(fesql_test.getCases(tool.getCasePath('/integration/v1/select/test_select_sample.yaml'), 'testSampleSelect'))
    def testSampleSelect(self, *testCases):
        executor.fesql_executor.build(self.connection,testCases[0]).run()

    @idata(fesql_test.getCases(tool.getCasePath('/integration/v1/expression/test_arithmetic.yaml'), 'testExpression'))
    def testExpression(self, *testCases):
        executor.fesql_executor.build(self.connection,testCases[0]).run()

    @idata(fesql_test.getCases(tool.getCasePath('/integration/v1/function/test_udaf_function.yaml'), 'testUDAFFunction'))
    def testUDAFFunction(self, *testCases):
        executor.fesql_executor.build(self.connection,testCases[0]).run()

    @idata(fesql_test.getCases(tool.getCasePath('/integration/v1/function/test_udf_function.yaml'), 'testUDFFunction'))
    def testUDFFunction(self, *testCases):
        executor.fesql_executor.build(self.connection,testCases[0]).run()

    @idata(fesql_test.getCases(tool.getCasePath('/integration/v1/select/test_sub_select.yaml'), 'testSubSelect'))
    def testSubSelect(self, *testCases):
        executor.fesql_executor.build(self.connection,testCases[0]).run()

if __name__ == '__main__':
    unittest.main()
