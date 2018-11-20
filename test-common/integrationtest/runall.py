import unittest
import xmlrunner
import os
import sys
sys.path.append(os.getenv('testpath'))
import libs.conf as conf
from libs.test_loader import load_all
import nose


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description='filter testcases')
    ap.add_argument('-R', '--runlist', default=False, help='filter run testcases')
    ap.add_argument('-N', '--norunlist', default=False, help='filter no-run testcases')
    args = ap.parse_args()

    runlist = args.runlist or ''
    norunlist = args.norunlist or '^$'
    specified_path = 'changeleader'
    test_suite = load_all(specified_path,'')
    suite = unittest.TestSuite(test_suite)
    # nose.run(suite=suite, argv=['nosetests', '-v','--processes=2'], plugins=[nose.plugins.MultiProcess()])

    # nose.run(test_suite)
    # nose.main()
    runner = xmlrunner.XMLTestRunner(output=os.getenv('reportpath'), failfast=conf.failfast)
    runner.run(suite)
