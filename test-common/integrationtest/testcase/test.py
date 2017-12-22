import unittest
import sys,os
sys.path.append(os.getenv('testpath'))

from libs.ddt import ddt, data, unpack
import csv
from pprint import pprint


def add(a, b):
    print('*'*5 ,a, b)
    c = a + b
    print('c' ,c)
    return c

@ddt
class Test(unittest.TestCase):
    @data((1, 1, 2), (1, 2, 3))
    @unpack
    def test_addnum(self, a, b, expected_value):
        self.assertEqual(add(a, b), expected_value)


if __name__ == "__main__":
    import sys
    import os
    suite = unittest.TestSuite()
    if len(sys.argv) == 1:
        suite = unittest.TestLoader().loadTestsFromTestCase(Test)
    else:
        for test_name in sys.argv[1:]:
            suite.addTest(Test(test_name))
    runner = xmlrunner.XMLTestRunner(output=os.getenv('reportpath'), failfast=conf.failfast)
    runner.run(suite)
