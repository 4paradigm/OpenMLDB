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


import logging
from . import sql_router_sdk
logger = logging.getLogger("openmldb_driver")
class DriverOptions(object):
    def __init__(self, zk_cluster, zk_path, session_timeout = 3000):
        self.zk_cluster = zk_cluster
        self.zk_path = zk_path
        self.session_timeout = session_timeout

class Driver(object):
    def __init__(self, options):
        self.options = options
        self.sdk = None

    def init(self):
        options = sql_router_sdk.SQLRouterOptions()
        options.zk_cluster = self.options.zk_cluster
        options.zk_path = self.options.zk_path
        self.sdk = sql_router_sdk.NewClusterSQLRouter(options)
        if not self.sdk:
            logger.error("fail to init openmldb driver with zk cluster %s and zk path %s"%(options.zk_cluster, options.zk_path))
            return False
        logger.info("init openmldb driver done with zk cluster %s and zk path %s"%(options.zk_cluster, options.zk_path))
        return True

    def createDB(self, db):
        if not self.sdk:
            return False, "please init driver first"
        status = sql_router_sdk.Status()
        if self.sdk.CreateDB(db, status):
            return True, ""
        else:
            return False, status.msg

    def dropDB(self, db):
        if not self.sdk:
            return False, "please init driver first"
        status = sql_router_sdk.Status()
        if self.sdk.DropDB(db, status):
            return True, "ok"
        else:
            return False, status.msg

    def executeDDL(self, db, ddl):
        if not self.sdk:
            return False, "please init driver first"
        status = sql_router_sdk.Status()
        if not self.sdk.ExecuteDDL(db, ddl, status):
            return False, status.msg
        else:
            self.sdk.RefreshCatalog()
            return True, "ok"

    def getInsertBuilder(self, db, sql):
        if not self.sdk:
            return False, "please init driver first"
        status = sql_router_sdk.Status()
        row_builder = self.sdk.GetInsertRow(db, sql, status)
        if status.code != 0:
            return False, status.msg
        return True, row_builder
    
    def getInsertBatchBuilder(self, db, sql):
        if not self.sdk:
            return False, "please init driver first"
        status = sql_router_sdk.Status()
        rows_builder = self.sdk.GetInsertRows(db, sql, status)
        if status.code != 0:
            return False, status.msg
        return True, rows_builder

    def executeInsert(self, db, sql, row_builder = None):
        if not self.sdk:
            return False, "please init driver first"
        status = sql_router_sdk.Status()
        if row_builder is not None:
            if self.sdk.ExecuteInsert(db, sql, row_builder, status):
                return True, ""
            else:
                return False, status.msg
        else:
            if self.sdk.ExecuteInsert(db, sql, status):
                return True, ""
            else:
                return False, status.msg

    def getRequestBuilder(self, db, sql):
        if not self.sdk:
            return False, "please init driver first"
        status = sql_router_sdk.Status()
        row_builder = self.sdk.GetRequestRow(db, sql, status)
        if status.code != 0:
            return False, status.msg
        return True, row_builder
 
    def doQuery(self, db, sql, data):
        if data is None:
            return self.executeQuery(db, sql, None)
        ok, requestRow = self.getRequestBuilder(db, sql)
        if not ok:
            return ok, requestRow
        schema = requestRow.GetSchema()
        ok, msg = self._append_request_row(requestRow, schema, data)
        if not ok:
            return ok, msg
        return self.executeQuery(db, sql, requestRow)

    def executeQuery(self, db, sql, row_builder = None):
        if not self.sdk:
            return False, "please init driver first"

        status = sql_router_sdk.Status()
        if row_builder is not None:
            rs = self.sdk.ExecuteSQL(db, sql, row_builder, status)
        else:
            rs = self.sdk.ExecuteSQL(db, sql, status)
        if status.code != 0:
            return False, status.msg
        else:
            return True, rs

    def getRowBySp(self, db, sp):
        status = sql_router_sdk.Status()
        row_builder = self.sdk.GetRequestRowByProcedure(db, sp, status)
        if status.code != 0:
            return False, status.msg
        return True, row_builder
   
    def callProc(self, db, sp, rq):
        status = sql_router_sdk.Status()
        rs = self.sdk.CallProcedure(db, sp, rq, status)
        if status.code != 0:
            return False, status.msg
        return True, rs

    def doProc(self, db, sp, data):
        ok, requestRow = self.getRowBySp(db, sp)
        if not ok:
            return ok, requestRow
        schema = requestRow.GetSchema();
        ok, msg = self._append_request_row(requestRow, schema, data)
        if not ok:
            return ok, msg
        return self.callProc(db, sp, requestRow)
    
    def _append_request_row(self, requestRow, schema, data):
        appendMap = {
            sql_router_sdk.kTypeBool: requestRow.AppendBool,
            sql_router_sdk.kTypeInt16: requestRow.AppendInt16,
            sql_router_sdk.kTypeInt32: requestRow.AppendInt32,
            sql_router_sdk.kTypeInt64: requestRow.AppendInt64,
            sql_router_sdk.kTypeFloat: requestRow.AppendFloat,
            sql_router_sdk.kTypeDouble: requestRow.AppendDouble,
            sql_router_sdk.kTypeString: requestRow.AppendString,
            sql_router_sdk.kTypeDate: lambda x : len(x.split("-")) == 3 and requestRow.AppendDate(int(x.split("-")[0]), int(x.split("-")[1]), int(x.split("-")[2])),
            sql_router_sdk.kTypeTimestamp: requestRow.AppendTimestamp
            }
        count = schema.GetColumnCnt()
        strSize = 0
        for i in range(count):
            name = schema.GetColumnName(i)
            if name not in data:
                return False, "col {} data not given".format(name)
            val = data.get(name)
            if val == None:
                if schema.IsColumnNotNull(i):
                    return False, "column seq {} not allow null".format(i)
                continue
            colType = schema.GetColumnType(i)
            if colType != sql_router_sdk.kTypeString:
                continue
            if isinstance(val, str):
                strSize += len(val)
            else:
                return False, "{} value type is not str".format(name)
        requestRow.Init(strSize)
        for i in range(count):
            name = schema.GetColumnName(i)
            val = data.get(name)
            if val is None:
                builder.AppendNULL()
                continue
            colType = schema.GetColumnType(i)
            ok = appendMap[colType](val)
            if not ok:
                return False, "erred at append data seq {}".format(i)
        ok = requestRow.Build()
        if not ok:
           return False, "erred at build request row data"
        return ok, ""

    
    def doBatchRowRequest(self, db, sql, commonCol, parameters):
        ok, requestRow = self.getRequestBuilder(db, sql)
        if not ok:
            return ok, "get request builder fail"
        schema = requestRow.GetSchema()
        commonCols = sql_router_sdk.ColumnIndicesSet(schema)
        count = schema.GetColumnCnt()
        commnColAddCount = 0
        for i in range(count):
            colName = schema.GetColumnName(i)
            if colName in commonCol:
                commonCols.AddCommonColumnIdx(i)
                commonColAddCount+=1
        if commnColAddCount != len(commonCol):
            return False, "some common col is not in table schema"
        requestRowBatch = sql_router_sdk.SQLRequestRowBatch(schema, commonCols)
        if requestRowBatch is None:
            return False, "generate sql request row batch fail"
        if isinstance(parameters, dict):
            ok, msg = self._append_request_row(requestRow, schema, parameters)
            if not ok:
                return ok, msg
            requestRowBatch.AddRow(requestRow)
        else:
            for d in parameters:
                ok, msg = self._append_request_row(requestRow, schema, d)
                if not ok:
                    return ok, msg
                requestRowBatch.AddRow(requestRow)
                ok, requestRow = self.getRequestBuilder(db, sql)
                if not ok:
                    return ok, "get request builder fail"
        status = sql_router_sdk.Status()
        rs = self.sdk.ExecuteSQLBatchRequest(db, sql, requestRowBatch, status)
        if status.code != 0:
            return False, status.msg
        return True, rs
