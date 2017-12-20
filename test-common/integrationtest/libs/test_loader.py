# -*- coding: utf-8 -*-
import unittest
import xmlrunner
import libs.conf as conf
import sys
import os


def load(cls):
    suite = unittest.TestSuite()
    suite_cases = unittest.TestLoader().loadTestsFromTestCase(cls)
    if len(sys.argv) == 1:
        suite = suite_cases
    else:
        for test_name in sys.argv[1:]:
            for tc in suite_cases:
                tc_name = tc._testMethodName
                if tc_name.startswith(test_name):
                    print tc_name
                    suite.addTest(cls(tc_name))
    runner = xmlrunner.XMLTestRunner(output=os.getenv('reportpath'), failfast=conf.failfast)
    runner.run(suite)
