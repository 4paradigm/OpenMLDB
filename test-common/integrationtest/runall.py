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
    ret = runner.run(suite)
    arr = str(ret)[1:-1].split(' ')
    errors = 0
    failures = 0
    for item in arr:
        pair = item.strip().split('=')
        if len(pair) != 2:
            continue
        if (pair[0] == "errors"):
            errors = int(pair[1])
        elif (pair[0] == "failures"):
            failures = int(pair[1])
    if errors == 0 and failures == 0:
        exit(0)
    else:
        exit(1)
