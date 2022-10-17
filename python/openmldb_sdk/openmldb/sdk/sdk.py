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

# fmt:off
import sys
from pathlib import Path
# add parent directory
sys.path.append(Path(__file__).parent.parent.as_posix())
import native.sql_router_sdk as sql_router_sdk

import logging
from datetime import date
from datetime import datetime
from prettytable import PrettyTable
from warnings import warn

# fmt:on

logger = logging.getLogger("OpenMLDB_sdk")


class OpenMLDBClusterSdkOptions(object):

    def __init__(self,
                 zk_cluster,
                 zk_path,
                 session_timeout=None,
                 spark_conf_path=None,
                 request_timeout=None,
                 zk_log_level=None,
                 zk_log_file=None):
        self.zk_cluster = zk_cluster
        self.zk_path = zk_path
        # all timeout unit ms
        self.zk_session_timeout = session_timeout
        self.spark_conf_path = spark_conf_path
        self.request_timeout = request_timeout
        self.zk_log_level = zk_log_level
        self.zk_log_file = zk_log_file


class OpenMLDBStandaloneSdkOptions(object):

    def __init__(self, host, port, request_timeout=None):
        self.host = host
        self.port = port
        self.request_timeout = int(
            request_timeout) if request_timeout else 60000


class OpenMLDBSdk(object):

    def __init__(self, **options_map):
        self.options_map = options_map
        self.sdk = None

    def init(self):
        is_cluster_mode = True if 'zkPath' in self.options_map else False
        if is_cluster_mode:
            options = sql_router_sdk.SQLRouterOptions()
            options.zk_cluster = self.options_map['zk']
            options.zk_path = self.options_map['zkPath']
            # optionals
            if 'zkLogLevel' in self.options_map:
                options.zk_log_level = int(self.options_map['zkLogLevel'])
            if 'zkLogFile' in self.options_map:
                options.zk_log_file = self.options_map['zkLogFile']
        else:
            options = sql_router_sdk.StandaloneOptions()
            # use host
            if 'zkPath' not in self.options_map:
                options.host = self.options_map['host']
                options.port = int(self.options_map['port'])

        # common options
        if 'requestTimeout' in self.options_map:
            options.request_timeout = int(self.options_map['requestTimeout'])
        if 'glogLevel' in self.options_map:
            options.glog_level = self.options_map['glogLevel']
        if 'glogDir' in self.options_map:
            options.glog_dir = self.options_map['glogDir']
        if 'maxSqlCacheSize' in self.options_map:
            options.max_sql_cache_size = int(self.options_map['maxSqlCacheSize'])

        self.sdk = sql_router_sdk.NewClusterSQLRouter(
            options) if is_cluster_mode else sql_router_sdk.NewStandaloneSQLRouter(options)
        if not self.sdk:
            logger.error(
                "fail to init OpenMLDB sdk with %s, is cluster mode %s" % (self.options_map, is_cluster_mode))
            return False
        logger.info("init openmldb sdk done with %s, is cluster mode %s" % (self.options_map, is_cluster_mode))
        status = sql_router_sdk.Status()
        self.sdk.ExecuteSQL("SET @@execute_mode='online'", status)
        return True

    def getDatabases(self):
        if not self.sdk:
            return False, "please init sdk first"

        dbs = sql_router_sdk.VectorString()
        status = sql_router_sdk.Status()
        self.sdk.ShowDB(dbs, status)
        if status.code != 0:
            return False
        output_dbs = []
        for i in range(dbs.size()):
            output_dbs.append(dbs[i])
        return output_dbs

    def createDB(self, db):
        if not self.sdk:
            return False, "please init sdk first"
        status = sql_router_sdk.Status()
        if self.sdk.CreateDB(db, status):
            return True, ""
        else:
            return False, status.msg

    def dropDB(self, db):
        if not self.sdk:
            return False, "please init sdk first"
        status = sql_router_sdk.Status()
        if self.sdk.DropDB(db, status):
            return True, "ok"
        else:
            return False, status.msg

    def getTables(self, db):
        if not self.sdk:
            raise Exception("please init sdk first")
        return self.sdk.GetTableNames(db)

    def getAllTables(self):
        if not self.sdk:
            raise Exception("please init sdk first")
        return self.sdk.GetAllTables()

    def isOnlineMode(self):
        if not self.sdk:
            return False, "please init sdk first"
        return self.sdk.IsOnlineMode()

    def getParameterBuilder(self, data):
        logger.debug("getParameterBuilder data type: %s", str(type(data)))
        logger.debug(data)
        columnTypes = sql_router_sdk.ColumnTypes()
        for col in data:
            col_type = sql_router_sdk.kTypeUnknow
            if isinstance(col, int):
                col_type = sql_router_sdk.kTypeInt64
            elif isinstance(col, float):
                col_type = sql_router_sdk.kTypeDouble
            elif isinstance(col, str):
                col_type = sql_router_sdk.kTypeString
            elif isinstance(col, bool):
                col_type = sql_router_sdk.kTypeBool
            elif isinstance(col, datetime):
                col_type = sql_router_sdk.kTypeTimestamp
            elif isinstance(col, date):
                col_type = sql_router_sdk.kTypeDate
            else:
                return False, "invalid parameter type " + str(type(col))
            logger.debug("val type {} Column Type {}".format(
                type(col), sql_router_sdk.DataTypeName(col_type)))
            columnTypes.AddColumnType(col_type)

        parameterRow = sql_router_sdk.SQLRequestRow.CreateSQLRequestRowFromColumnTypes(
            columnTypes)
        schema = parameterRow.GetSchema()
        ok, msg = self._append_request_row(parameterRow, schema, data)
        if not ok:
            return ok, msg
        else:
            return True, parameterRow

    def getInsertBuilder(self, db, sql):
        if not self.sdk:
            return False, "please init sdk first"
        status = sql_router_sdk.Status()
        row_builder = self.sdk.GetInsertRow(db, sql, status)
        if status.code != 0:
            return False, status.msg
        return True, row_builder

    def getInsertBatchBuilder(self, db, sql):
        if not self.sdk:
            return False, "please init sdk first"
        status = sql_router_sdk.Status()
        rows_builder = self.sdk.GetInsertRows(db, sql, status)
        if status.code != 0:
            return False, status.msg
        return True, rows_builder

    def executeInsert(self, db, sql, row_builder=None):
        if not self.sdk:
            return False, "please init sdk first"
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
            return False, "please init sdk first"
        status = sql_router_sdk.Status()
        row_builder = self.sdk.GetRequestRow(db, sql, status)
        if status.code != 0:
            return False, status.msg
        return True, row_builder

    def doRequestQuery(self, db, sql, data):
        if data is None:
            return False, "please init request data"
        ok, requestRow = self.getRequestBuilder(db, sql)
        if not ok:
            return ok, requestRow
        schema = requestRow.GetSchema()
        ok, msg = self._append_request_row(requestRow, schema, data)
        if not ok:
            return ok, msg
        return self.executeSQL(db, sql, requestRow)

    def doParameterizedQuery(self, db, sql, data):
        logging.debug("doParameterizedQuery data: %s", str(data))
        if isinstance(data, tuple) and len(data) > 0:
            ok, parameterRow = self.getParameterBuilder(data)
        else:
            return False, "Invalid query data type " + str(type(data))
        if not ok:
            return False, parameterRow
        return self.executeQueryParameterized(db, sql, parameterRow)

    def doQuery(self, db, sql):
        return self.executeSQL(db, sql, None)

    def executeQuery(self, db, sql, row_builder=None):
        warn('This method is deprecated.', DeprecationWarning)
        return self.executeSQL(db, sql, row_builder)

    def executeSQL(self, db, sql, row_builder=None):
        """
        1. no row_builder: batch mode
        2. row_builder: request mode
        And if db, use this one, if not, use the db setting in sdk
        """
        if not self.sdk:
            return False, "please init sdk first"

        status = sql_router_sdk.Status()
        if row_builder is not None:
            # TODO(hw): must set db in request mode now, fix later
            rs = self.sdk.ExecuteSQLRequest(db, sql, row_builder, status)
        elif db:
            rs = self.sdk.ExecuteSQL(db, sql, status)
        else:
            # if no db specific in here, use the current db in sdk
            rs = self.sdk.ExecuteSQL(sql, status)
        if status.code != 0:
            return False, status.msg
        else:
            return True, rs

    def executeQueryParameterized(self, db, sql, row_builder):
        if not self.sdk:
            return False, "please init sdk first"

        if not row_builder:
            return False, "pealse init parameter row"

        status = sql_router_sdk.Status()
        rs = self.sdk.ExecuteSQLParameterized(db, sql, row_builder, status)
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
            return ok, "get row by sp failed"
        schema = requestRow.GetSchema()
        ok, msg = self._append_request_row(requestRow, schema, data)
        if not ok:
            return ok, msg
        return self.callProc(db, sp, requestRow)

    def _append_request_row(self, requestRow, schema, data):
        if isinstance(data, dict):
            return self._append_request_row_with_dict(requestRow, schema, data)
        elif isinstance(data, tuple) and len(data) > 0:
            return self._append_request_row_with_tuple(requestRow, schema, data)
        else:
            return False, "Invalid row type " + str(type(data))

    def _extract_timestamp(self, x):
        if isinstance(x, str):
            logging.debug("extract datetime/timestamp with string item")
            try:
                dt = datetime.fromisoformat(x)
                return True, int(dt.timestamp() * 1000)
            except Exception as e:
                return False, "fail extract date from string {}".format(e)
        elif isinstance(x, int):
            logging.debug("extract datetime/timestamp with integer")
            return True, x
        elif isinstance(x, datetime):
            logging.debug("extract datetime/timestamp with datetime item")
            return True, int(x.timestamp() * 1000)
        elif isinstance(x, date):
            logging.debug("extract datetime/timestamp with date item")
            dt = datetime(x.year, x.month, x.day, 0, 0, 0)
            return True, int(dt.timestamp() * 1000)
        else:
            return False, "fail extract datetime, invalid type {}".format(
                type(x))

    def _extract_date(self, x):
        if isinstance(x, str):
            logging.debug("append date with string item")
            try:
                dt = date.fromisoformat(x)
                return True, (dt.year, dt.month, dt.day)
            except Exception as e:
                return False, "fail extract date from string {}".format(e)
        elif isinstance(x, datetime):
            logging.debug("extract date with datetime item")
            return True, (x.year, x.month, x.day)
        elif isinstance(x, date):
            logging.debug("append date with date item")
            return True, (x.year, x.month, x.day)
        else:
            return False, "fail to extract date, invallid type {}".format(
                type(x))

    def _append_request_row_with_tuple(self, requestRow, schema, data):
        appendMap = {
            sql_router_sdk.kTypeBool:
                requestRow.AppendBool,
            sql_router_sdk.kTypeInt16:
                requestRow.AppendInt16,
            sql_router_sdk.kTypeInt32:
                requestRow.AppendInt32,
            sql_router_sdk.kTypeInt64:
                requestRow.AppendInt64,
            sql_router_sdk.kTypeFloat:
                requestRow.AppendFloat,
            sql_router_sdk.kTypeDouble:
                requestRow.AppendDouble,
            sql_router_sdk.kTypeString:
                requestRow.AppendString,
            sql_router_sdk.kTypeDate:
                lambda x: len(x) == 3 and requestRow.AppendDate(
                    x[0], x[1], x[2]),
            sql_router_sdk.kTypeTimestamp:
                requestRow.AppendTimestamp
        }
        count = schema.GetColumnCnt()
        strSize = 0
        for i in range(count):
            colType = schema.GetColumnType(i)
            if colType != sql_router_sdk.kTypeString:
                continue
            val = data[i]
            if isinstance(val, str):
                strSize += len(val)
            else:
                return False, "column[{}] type is not str".format(i)

        requestRow.Init(strSize)
        for i in range(count):
            val = data[i]
            if val is None:
                requestRow.AppendNULL()
                continue
            colType = schema.GetColumnType(i)
            if colType == sql_router_sdk.kTypeDate:
                ok, val = self._extract_date(val)
                if not ok:
                    return False, "error when extract date value {}".format(val)
            if colType == sql_router_sdk.kTypeTimestamp:
                ok, val = self._extract_timestamp(val)
                if not ok:
                    return False, val
                else:
                    logging.debug("timestamp val: {}".format(val))

            ok = appendMap[colType](val)
            if not ok:
                return False, "erred at append data seq {}".format(i)
        ok = requestRow.Build()
        if not ok:
            return False, "erred at build request row data"
        return ok, ""

    def _append_request_row_with_dict(self, requestRow, schema, data):
        appendMap = {
            sql_router_sdk.kTypeBool:
                requestRow.AppendBool,
            sql_router_sdk.kTypeInt16:
                requestRow.AppendInt16,
            sql_router_sdk.kTypeInt32:
                requestRow.AppendInt32,
            sql_router_sdk.kTypeInt64:
                requestRow.AppendInt64,
            sql_router_sdk.kTypeFloat:
                requestRow.AppendFloat,
            sql_router_sdk.kTypeDouble:
                requestRow.AppendDouble,
            sql_router_sdk.kTypeString:
                requestRow.AppendString,
            sql_router_sdk.kTypeDate:
                lambda x: len(x) == 3 and requestRow.AppendDate(
                    x[0], x[1], x[2]),
            sql_router_sdk.kTypeTimestamp:
                requestRow.AppendTimestamp
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
                requestRow.AppendNULL()
                continue
            colType = schema.GetColumnType(i)
            if colType == sql_router_sdk.kTypeDate:
                ok, val = self._extract_date(val)
                if not ok:
                    return False, "error when extract date value {}".format(val)
            if colType == sql_router_sdk.kTypeTimestamp:
                ok, val = self._extract_timestamp(val)
                if not ok:
                    return False, val
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
                commnColAddCount += 1
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

    def getJobLog(self, id):
        if not self.sdk:
            return False, "please init sdk first"

        status = sql_router_sdk.Status()

        log = self.sdk.GetJobLog(id, status)
        if status.code != 0:
            # TODO: Throw exception if get failure status
            return ""

        return log

    @staticmethod
    def print_table(schema, rows):
        t = PrettyTable(schema)
        for row in rows:
            t.add_row(row)
        print(t)


class TypeUtil(object):

    # Convert int type to string type
    @staticmethod
    def intTypeToStr(intType):
        # The map of type with number and type with readable string
        typeMap = {
            sql_router_sdk.kTypeBool: "bool",
            sql_router_sdk.kTypeInt16: "int16",
            sql_router_sdk.kTypeInt32: "int32",
            sql_router_sdk.kTypeInt64: "int64",
            sql_router_sdk.kTypeFloat: "float",
            sql_router_sdk.kTypeDouble: "double",
            sql_router_sdk.kTypeString: "string",
            sql_router_sdk.kTypeDate: "date",
            sql_router_sdk.kTypeTimestamp: "timestamp"
        }
        return typeMap[intType]
