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

import os.path
import re
from copy import deepcopy

import yaml

from common import fedb_config
from util import tools


def isCaseInBlackList(case):
    if case == None:
        return False
    tag = case.get('tags')
    if (tag != None) and ("TODO" in tag):
        return True
    return False


def getCases(yamlPaths: list, levels=fedb_config.levels, casePrefix='') -> list:
    testCases = []
    for yamlPath in yamlPaths:
        yamlPath = tools.getCasePath(yamlPath)
        if not os.path.exists(yamlPath):
            continue
        if os.path.isfile(yamlPath):
            cases = getCasesByYaml(yamlPath)
            testCases.extend(cases)
        else:
            for f in os.listdir(yamlPath):
                if f.endswith(".yml") or f.endswith(".yaml"):
                    ff = os.path.join(yamlPath, f)
                    cases = getCasesByYaml(os.path.join(yamlPath, f))
                    testCases.extend(cases)
    testCases = filter(lambda c: c['level'] in levels, testCases)
    return testCases


def getCasesByYaml(yamlPath: str, casePrefix='') -> list:
    with open(yamlPath) as f:
        dataMap = yaml.safe_load(f)
    db = dataMap.get('db')
    debugs = dataMap.get('debugs')
    executor = dataMap.get('executor')
    cases = dataMap.get('cases')
    sqlDialect = dataMap.get('sqlDialect')
    if sqlDialect is None:
        sqlDialect = ["ANSISQL"]
    testCases = []
    index = 0
    if debugs is not None and len(debugs) > 0:
        cases = filter(lambda c: c['desc'] in debugs, cases)
    for case in cases:
        if not isCaseInBlackList(case):
            if case.get('executor') is None:
                case['executor'] = executor
            if case.get('db') is None:
                case['db'] = db
            if case.get('level') is None:
                case['level'] = 0
            if case.get('sqlDialect') is None:
                case['sqlDialect'] = sqlDialect
            if case.get('expect') is None:
                case['expect'] = {}
            if case.get('expect').get('success') is None:
                case['expect']['success'] = True
            index_str = "{0:0{1}}".format(index + 1, 5)
            case['case_prefix'] = str(casePrefix) + '_' + str(index_str)
            index += 1
            addCase(case, testCases)
    return testCases


def addCase(tmpCase: dict, testCaseList: list):
    dataProviderList = tmpCase.get('dataProvider')
    if dataProviderList is None or len(dataProviderList) == 0:
        testCaseList.append(tmpCase)
    else:
        genList = generateCase(0, tmpCase, dataProviderList)
        testCaseList.extend(genList)


def generateCase(index: int, sqlCase: dict, dataProviderList: list) -> list:
    dataProvider = dataProviderList[index]
    caseList = generateCaseByDataProvider(sqlCase, dataProvider, index)
    if len(dataProviderList) - 1 == index:
        return caseList
    sqlCases = []
    for tmpCase in caseList:
        genCaseList = generateCase(index + 1, tmpCase, dataProviderList)
        sqlCases.extend(genCaseList)
    return sqlCases


def generateCaseByDataProvider(sqlCase: dict, dataProvider: list, index: int) -> list:
    sqlCases = []
    for i, data in enumerate(dataProvider):
        sql = sqlCase.get('sql')
        sql = re.sub('d\\[' + str(index) + '\\]', str(data), sql)
        newSqlCase = deepcopy(sqlCase)
        # 设置新的sql
        newSqlCase['sql'] = sql
        newSqlCase['id'] = str(newSqlCase.get('id')) + "_" + str(i)
        newSqlCase['desc'] = newSqlCase.get('desc') + "_" + str(i)
        # 根据expectProvider 生成新的 预期结果 只对第一级测dataProvider可以设置不同的expect
        if index == 0:
            map = sqlCase.get('expectProvider')
            if map is not None and len(map) > 0:
                expectDesc = map.get(i)
                if expectDesc is not None:
                    newExpectDesc = newSqlCase.get('expect')
                    if newExpectDesc is None:
                        newSqlCase['expect'] = expectDesc
                    else:
                        success = expectDesc.get('success')
                        order = expectDesc.get('order')
                        columns = expectDesc.get('columns')
                        rows = expectDesc.get('rows')
                        count = expectDesc.get('count')
                        if success is not None and not success:
                            newExpectDesc['success'] = success
                        if count is not None and count > 0:
                            newExpectDesc['count'] = count
                        if columns is not None and len(columns) > 0:
                            newExpectDesc['columns'] = columns
                        if order is not None and len(order) > 0:
                            newExpectDesc['order'] = order
                        if rows is not None and len(rows) > 0:
                            newExpectDesc['rows'] = rows
        sqlCases.append(newSqlCase)
    return sqlCases
