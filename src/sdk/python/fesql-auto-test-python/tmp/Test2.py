import unittest
from util import HTMLTestRunner
import xmlrunner

# from test.create import TestCreate
# from test.test_select import TestSelect
class Test2(unittest.TestCase):
    def test1(self):
        print("test1")
        self.assertEqual(1,1)

if __name__ == '__main__':
    print("CCC")
    suite = unittest.TestSuite()
    suite.addTest(Test2('test1'))
    print("BBB")
    # suite.addTest(TestSelect())
    # html_file = "/Users/zhaowei/code/4paradigm/rtidb/src/sdk/python/fesql-auto-test-python/tmp/test2.html"
    # fp = open(html_file,"wb")
    # runner = unittest.TextTestRunner().run(suite)
    # print("AAAA")
    # HTMLTestRunner.HTMLTestRunner(fp).run(suite)
    # fp.close()
    runner = xmlrunner.XMLTestRunner(output="../report")
    runner.run(suite)