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
import time

from nb_log import LogManager
import check.checker
from abc import ABCMeta, abstractmethod

from util import fedb_util

log = LogManager('python-sdk-test').get_logger_and_add_handlers()


class BaseExecutor(metaclass=ABCMeta):
    def __init__(self, executor, fesqlCase):
        self.fesqlCase = fesqlCase
        self.executor = executor

    def process(self):
        if (self.fesqlCase == None):
            return
        log.info(str(self.fesqlCase['case_prefix']) + ': ' + self.fesqlCase['desc'] + " Begin!")
        self.prepare()
        fesqlResult = self.execute()
        print(fesqlResult)
        self.check(fesqlResult)
        self.tearDown()

    @abstractmethod
    def run(self):
        pass

    def prepare(self):
        pass

    @abstractmethod
    def execute(self):
        pass

    def tearDown(self):
        pass

    def check(self, fesqlResult):
        strategyList = check.checker.build(self.fesqlCase, fesqlResult)
        for checker in strategyList:
            checker.check()


class NullExecutor(BaseExecutor):
    def __init__(self, executor, fesqlCase):
        super(NullExecutor, self).__init__(executor, fesqlCase)

    def excute(self):
        return None

    def run(self):
        log.info("No case need to be run.")


class SQLExecutor(BaseExecutor):

    def __init__(self, executor, fesqlCase):
        super(SQLExecutor, self).__init__(executor, fesqlCase)
        self.dbName = fesqlCase['db']
        self.tableNames = None

    def run(self):
        if self.fesqlCase.get('mode') != None and "batch-unsupport" in self.fesqlCase.get('mode'):
            log.info("skip case in batch mode: {}".format(self.fesqlCase.get('desc')))
            return
        if self.fesqlCase.get('mode') != None and "rtidb-unsupport" in self.fesqlCase.get('mode'):
            log.info("skip case in rtidb mode: {}".format(self.fesqlCase.get('desc')))
            return
        if self.fesqlCase.get('mode') != None and "performnace-sensitive-unsupport" in self.fesqlCase.get('mode'):
            log.info("skip case in rtidb mode: {}".format(self.fesqlCase.get('desc')))
            return
        if self.fesqlCase.get('mode') != None and "rtidb-batch-unsupport" in self.fesqlCase.get('mode'):
            log.info("skip case in rtidb batch mode: {}".format(self.fesqlCase.get('desc')))
            return
        if self.fesqlCase.get('mode') != None and "python-unsupport" in self.fesqlCase.get('mode'):
            log.info("skip case in python mode: {}".format(self.fesqlCase.get('desc')))
            return
        self.process()

    def prepare(self):
        # createDB = self.fesqlCase.get('createDB')
        # if createDB == None or createDB:
        #     if hasattr(self.executor, "createDB"):
        #         dbOk = self.executor.createDB(self.dbName)
        #         log.info("create db:" + self.dbName + "," + str(dbOk))
        #     try:
        #         dbOk = self.executor.execute("create database {};".format(self.dbName))
        #         log.info("create db:" + self.dbName+ "," + str(dbOk))
        #     except Exception as e:
        #         pass
        inputs = self.fesqlCase.get('inputs')
        #if inputs.get(0).get('columns')==None:

        res, self.tableNames = fedb_util.createAndInsert(self.executor, self.dbName, inputs)
        if not res.ok:
            raise Exception("fail to run SQLExecutor: prepare fail")

    def execute(self):
        fesqlResult = None
        sqls = self.fesqlCase.get('sqls')
        if sqls != None and len(sqls) > 0:
            for sql in sqls:
                log.info("sql:" + sql)
                sql = fedb_util.formatSql(sql, self.tableNames)
                fesqlResult = fedb_util.sql(self.executor, self.dbName, sql)
        if self.fesqlCase.__contains__('sql') == False:
            return fesqlResult
        sql = self.fesqlCase['sql']
        if sql != None and len(sql) > 0:
            log.info("sql:" + sql)
            sql = fedb_util.formatSql(sql, self.tableNames)
            fesqlResult = fedb_util.sql(self.executor, self.dbName, sql)
        return fesqlResult

    def tearDown(self):
        if self.tableNames:
            for tableName in self.tableNames:
                dropSql = "drop table {};".format(tableName)
                fedb_util.ddl(self.executor, self.dbName, dropSql)
        log.info("drop table finish")


class RequestQuerySQLExecutor(SQLExecutor):

    def __init__(self, executor, fesqlCase):
        super(RequestQuerySQLExecutor, self).__init__(executor, fesqlCase)

    def run(self):
        if self.fesqlCase.get('mode') != None and "request-unsupport" in self.fesqlCase.get('mode'):
            log.info("skip case in request mode: {}".format(self.fesqlCase.get('desc')))
            return
        if self.fesqlCase.get('mode') != None and "rtidb-unsupport" in self.fesqlCase.get('mode'):
            log.info("skip case in rtidb mode: {}".format(self.fesqlCase.get('desc')))
            return
        if self.fesqlCase.get('mode') != None and "performnace-sensitive-unsupport" in self.fesqlCase.get('mode'):
            log.info("skip case in rtidb mode: {}".format(self.fesqlCase.get('desc')))
            return
        if self.fesqlCase.get('mode') != None and "rtidb-request-unsupport" in self.fesqlCase.get('mode'):
            log.info("skip case in rtidb request mode: {}".format(self.fesqlCase.get('desc')))
            return
        if self.fesqlCase.get('mode') != None and "python-unsupport" in self.fesqlCase.get('mode'):
            log.info("skip case in python mode: {}".format(self.fesqlCase.get('desc')))
            return
        self.process()

    def prepare(self):
        # createDB = self.fesqlCase.get('createDB')
        # if createDB == None or createDB:
        #     dbOk = self.executor.createDB(self.dbName)
        #     log.info("create db:" + self.dbName + "," + str(dbOk))
        inputs = self.fesqlCase.get('inputs')
        res, self.tableNames = fedb_util.createAndInsert(self.executor, self.dbName, inputs, True)
        if not res.ok:
            raise Exception("fail to run RequestQuerySQLExecutor: prepare fail")

    def execute(self):
        fesqlResult = None
        sqls = self.fesqlCase.get('sqls')
        if sqls != None and len(sqls) > 0:
            for sql in sqls:
                log.info("sql:" + sql)
                sql = fedb_util.formatSql(sql, self.tableNames)
                fesqlResult = fedb_util.sql(self.executor, self.dbName, sql)
        sql = self.fesqlCase['sql']
        if sql != None and len(sql) > 0:
            inputs = self.fesqlCase.get('inputs')
            if inputs == None or len(inputs) == 0 or inputs[0].get('rows') == None or len(inputs[0].get('rows')) == 0:
                log.error("fail to execute in request query sql executor: sql case inputs is empty");
                return None
            log.info("sql:" + sql)
            sql = fedb_util.formatSql(sql, self.tableNames)
            fesqlResult = fedb_util.sqlRequestMode(self.executor, self.dbName, sql, inputs[0])
        return fesqlResult


def getExecutor(driver, fesqlCase):
    executor = None
    executorType = fesqlCase.get('executor')
    if executorType == None:
        executor = SQLExecutor(driver, fesqlCase)
    else:
        if executorType == 'sql':
            executor = SQLExecutor(driver, fesqlCase)
        else:
            assert False, 'No executor named: ' + executorType
    return executor


def build(executor, fesqlCase, requestMode=False):
    if fesqlCase == None:
        return NullExecutor(executor, fesqlCase)
    if requestMode:
        return RequestQuerySQLExecutor(executor, fesqlCase)
    return getExecutor(executor, fesqlCase)
