import unittest
import xmlrunner
import commands
import importlib
import os
import sys
sys.path.append(os.getenv('testpath'))
import libs.conf as conf
from libs.test_loader import load_all


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description='filter testcases')
    ap.add_argument('-R', '--regex', default=False, help='filter testcases')
    args = ap.parse_args()

    reg = args.regex or '.'
    test_suite = load_all(reg)
    suite = unittest.TestSuite(test_suite)
    runner = xmlrunner.XMLTestRunner(output=os.getenv('reportpath'), failfast=conf.failfast)
    runner.run(suite)
