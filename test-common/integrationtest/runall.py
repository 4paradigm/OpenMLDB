import unittest
import commands
import importlib

if __name__ == "__main__":
    tests = commands.getstatusoutput('ls testcase|egrep -v "frame|pyc|init"')[1].split('\n')
    test_suite = []
    for module in tests:
        mo = importlib.import_module('testcase.{}'.format(module[:-3]))
        test_class = [attr for attr in dir(mo) if attr.startswith('Test') and attr != 'TestCaseBase'][0]
        test_suite.append(unittest.TestLoader().loadTestsFromTestCase(eval('mo.' + test_class)))

    suite = unittest.TestSuite(test_suite)
    unittest.TextTestRunner(verbosity=2).run(suite)
