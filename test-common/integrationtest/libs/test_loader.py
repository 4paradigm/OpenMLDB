# -*- coding: utf-8 -*-
import unittest
import xmlrunner
import commands
import importlib
import os
import sys
sys.path.append(os.getenv('testpath'))
import libs.conf as conf
from libs.logger import infoLogger


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
                    infoLogger.info(tc_name)
                    suite.addTest(cls(tc_name))
    runner = xmlrunner.XMLTestRunner(output=os.getenv('reportpath'), failfast=conf.failfast)
    runner.run(suite)


def load_all(runlist='', norunlist='^$'):
    testpath = os.getenv('testpath')
    cmd = 'ls {}/testcase|grep "^test_.*py$"|egrep "{}"|egrep -v "{}"'.format(testpath, runlist, norunlist)
    infoLogger.info(cmd)
    tests = commands.getstatusoutput(cmd)[1].split('\n')
    infoLogger.info(tests)
    test_suite = []
    for module in tests:
        mo = importlib.import_module('testcase.{}'.format(module[:-3]))
        test_classes = [attr for attr in dir(mo) if attr.startswith('Test') and attr != 'TestCaseBase']
        if len(test_classes) == 0:
            continue
        else:
            test_class = test_classes[0]
        test_suite.append(unittest.TestLoader().loadTestsFromTestCase(eval('mo.' + test_class)))
    for t in test_suite:
        infoLogger.info(t)
    return test_suite
