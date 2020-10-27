#! /usr/bin/env python
# -*- coding: utf-8 -*-

import common.fesql_test as fesql_test
from ddt import ddt,file_data,unpack,data,idata
import unittest
import executor.fesql_executor
import util.tools as tool

@ddt
class TestLastJoin(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.driver = fesql_test.getDriver()
        cls.engine = fesql_test.getEngine()
        cls.connection = cls.engine.connect()

    @idata(fesql_test.getCases(tool.getCasePath('/integration/v1/test_last_join.yaml')))
    def test_lastjoin(self, *testCases):
        executor.fesql_executor.build(self.connection, testCases[0]).run()

if __name__ == '__main__':
    unittest.main()
