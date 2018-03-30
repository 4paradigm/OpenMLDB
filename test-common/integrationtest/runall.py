import unittest
import xmlrunner
import os
import sys
sys.path.append(os.getenv('testpath'))
import libs.conf as conf
from libs.test_loader import load_all


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description='filter testcases')
    ap.add_argument('-R', '--runlist', default=False, help='filter run testcases')
    ap.add_argument('-N', '--norunlist', default=False, help='filter no-run testcases')
    args = ap.parse_args()

    runlist = args.runlist or ''
    norunlist = args.norunlist or '^$'
    test_suite = load_all(runlist, norunlist)
    suite = unittest.TestSuite(test_suite)
    runner = xmlrunner.XMLTestRunner(output=os.getenv('reportpath'), failfast=conf.failfast)
    runner.run(suite)
