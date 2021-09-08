#! /usr/bin/env python
# -*- coding: utf-8 -*-

import os

def getCasePath(path:str):
    currentPath = os.getcwd()
    index = currentPath.rfind('OpenMLDB')
    prePath = currentPath[0:index]
    casePath = prePath+"OpenMLDB/cases"+path
    return casePath

def getRootPath():
    currentPath = os.getcwd()
    index = currentPath.rfind('python-sdk-test')
    prePath = currentPath[0:index]
    rootPath = prePath + "python-sdk-test"
    return rootPath

def getAbsolutePath(path:str):
    rootPath = getRootPath()
    absolutePath = rootPath+"/"+path
    return absolutePath

def equalsFloat(a, b, precision=0.00001):
    if abs(a - b) <= precision:
        return True
    return False

if __name__ == '__main__':
    print(getRootPath())