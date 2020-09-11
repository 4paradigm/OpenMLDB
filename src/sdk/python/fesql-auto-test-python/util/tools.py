#! /usr/bin/env python
# -*- coding: utf-8 -*-

import os

def getCasePath(path:str):
    currentPath = os.getcwd()
    index = currentPath.rfind('rtidb')
    prePath = currentPath[0:index]
    casePath = prePath+"rtidb/fesql/cases"+path
    return casePath

def getRootPath():
    currentPath = os.getcwd()
    index = currentPath.rfind('fesql-auto-test-python')
    prePath = currentPath[0:index]
    rootPath = prePath + "fesql-auto-test-python"
    return rootPath

def getAbsolutePath(path:str):
    rootPath = getRootPath()
    absolutePath = rootPath+"/"+path
    return absolutePath

if __name__ == '__main__':
    print(getRootPath())