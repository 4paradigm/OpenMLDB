#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright 2021 4Paradigm
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


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
            if 'dataProvider' in case:
                continue
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
    engine = db.create_engine('openmldb://@/test_zw?zk={}&zkPath={}'.format(config.zk_cluster, config.zk_root_path))
    return engine







