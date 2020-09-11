#! /usr/bin/env python
# -*- coding: utf-8 -*-

import common.fesql_test as fesql_test
from ddt import ddt,file_data,unpack,data,idata
import unittest
import executor.fesql_executor
import util.tools as tool

@ddt
class TestInsert(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.driver = fesql_test.getDriver()

    @idata(fesql_test.getCases(tool.getCasePath('/integration/v1/test_insert.yaml')))
    def test_insert(self, *testCases):
        executor.fesql_executor.build(self.driver,testCases[0]).run()

if __name__ == '__main__':
    unittest.main()
