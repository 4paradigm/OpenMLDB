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
class TestWindow(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.engine = fesql_test.getEngine()
        cls.connection = cls.engine.connect()

    @idata(fesql_test.getCases(tool.getCasePath('/integration/v1/window/test_window_row_range.yaml')))
    def testRowRange(self, *testCases):
        executor.fesql_executor.build(self.connection,testCases[0]).run()

    @idata(fesql_test.getCases(tool.getCasePath('/integration/v1/window/test_window_row.yaml')))
    def testRow(self, *testCases):
        executor.fesql_executor.build(self.connection,testCases[0]).run()

    @idata(fesql_test.getCases(tool.getCasePath('/integration/v1/window/test_window_union.yaml')))
    def testWindowUnion(self, *testCases):
        executor.fesql_executor.build(self.connection,testCases[0]).run()

    @idata(fesql_test.getCases(tool.getCasePath('/integration/v1/window/test_window_row_range.yaml')))
    def testRowRangeRequestMode(self, *testCases):
        executor.fesql_executor.build(self.connection,testCases[0]).run()

    @idata(fesql_test.getCases(tool.getCasePath('/integration/v1/window/test_window_row.yaml')))
    def testRowRequestMode(self, *testCases):
        executor.fesql_executor.build(self.connection,testCases[0]).run()

    @idata(fesql_test.getCases(tool.getCasePath('/integration/v1/window/test_window_union.yaml')))
    def testWindowUnionRequestMode(self, *testCases):
        executor.fesql_executor.build(self.connection,testCases[0]).run()


if __name__ == '__main__':
    unittest.main()
