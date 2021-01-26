#! /usr/bin/env python
# -*- coding: utf-8 -*-

import common.fesql_config as config
import yaml
import sqlalchemy as db

def isCaseInBlackList(case):
    if case == None:
        return False
    tag = case.get('tags')
    if (tag != None) and ("TODO" in tag):
        return True
    return False

def getCases(yamlPath: str, casePrefix='') -> list:
    with open(yamlPath) as f:
        dataMap = yaml.safe_load(f)
    db = dataMap.get('db')
    debugs = dataMap.get('debugs')
    executor = dataMap.get('executor')
    cases = dataMap.get('cases')
    testCases = []
    index = 0
    if debugs != None and len(debugs) > 0:
        for case in cases:
            if case['desc'] in debugs:
                if case.get('executor') == None:
                    case['executor'] = executor
                if case.get('db') == None:
                    case['db'] = db
                testCases.append(case)
                index_str = "{0:0{1}}".format(index + 1, 5)
                case['case_prefix'] = str(casePrefix) + '_' + index_str
        return testCases
    else:
        for case in cases:
            if not isCaseInBlackList(case):
                if case.get('executor') == None:
                    case['executor'] = executor
                if case.get('db') == None:
                    case['db'] = db
                index_str = "{0:0{1}}".format(index + 1, 5)
                case['case_prefix'] = str(casePrefix) + '_' + str(index_str)
                index+=1
                testCases.append(case)

        return testCases

def getEngine():
    engine = db.create_engine('fedb://@/test_zw?zk={}&zkPath={}'.format(config.zk_cluster, config.zk_root_path))
    return engine







