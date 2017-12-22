import unittest
import xmlrunner
import commands
import importlib
import os
import sys
sys.path.append(os.getenv('testpath'))
import libs.conf as conf
from libs.clients.ns_cluster import NsCluster
from libs.clients.tb_cluster import TbCluster


if __name__ == "__main__":
    testpath = os.getenv('testpath')
    tests = commands.getstatusoutput('ls {}/testcase|egrep -v "frame|pyc|init"'.format(testpath))[1].split('\n')
    test_suite = []
    for module in tests:
        mo = importlib.import_module('testcase.{}'.format(module[:-3]))
        test_classes = [attr for attr in dir(mo) if attr.startswith('Test') and attr != 'TestCaseBase']
        if len(test_classes) == 0:
            continue
        else:
            test_class = test_classes[0]
        test_suite.append(unittest.TestLoader().loadTestsFromTestCase(eval('mo.' + test_class)))

    suite = unittest.TestSuite(test_suite)
    runner = xmlrunner.XMLTestRunner(output=os.getenv('reportpath'), failfast=conf.failfast)
    runner.run(suite)
