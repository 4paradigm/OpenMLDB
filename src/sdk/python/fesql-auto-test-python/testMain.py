#! /usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
import util.tools as tool
import os
from util import HTMLTestRunner
import xmlrunner

if __name__ == '__main__':

    rootPath = tool.getRootPath()
    testDir = os.path.join(rootPath,"test")
    discover = unittest.defaultTestLoader.discover(testDir,"test_*.py")
    runner = xmlrunner.XMLTestRunner(output="report")
    runner.run(discover)