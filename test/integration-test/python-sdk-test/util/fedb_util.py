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

from datetime import datetime

from nb_log import LogManager
#from sqlalchemy_openmldb.openmldbapi import Type as feType
from openmldb.dbapi import Type as feType
import re
import random
import string
import time

#from sqlalchemy_openmldb.openmldbapi.sql_router_sdk import DataTypeName, SQLRequestRow
from openmldb.native.sql_router_sdk import DataTypeName, SQLRequestRow

from common import fedb_config
from entity.fedb_result import FedbResult

log = LogManager('fesql-auto-test').get_logger_and_add_handlers()

reg = "\\{(\\d+)\\}"
formatSqlReg = re.compile(reg)


def getRandomName():
    tableName = ''.join(random.sample(string.ascii_letters + string.digits, 8))
    tableName = "auto_" + tableName
    return tableName


def getCreateSql(name: str, columns: list, inexs: list):
    time.sleep(1)
    createSql = "create table " + name + "("
    for column in columns:
        createSql += column + ","
    for index in inexs:
        ss = index.split(":")
        if len(ss) >= 2:
            keyStr = ss[1]
            if ("|" in ss[1]):
                keyStr = ",".join(ss[1].split("|"))
        if len(ss) == 2:
            createSql += "index(key=(%s))," % (keyStr)
        elif len(ss) == 3:
            createSql += "index(key=(%s),ts=%s)," % (keyStr, ss[2])
        elif len(ss) == 4:
            createSql += "index(key=(%s),ts=%s,ttl=%s)," % (keyStr, ss[2], ss[3])
        elif len(ss) == 5:
            createSql += "index(key=(%s),ts=%s,ttl=%s,ttl_type=%s)," % (keyStr, ss[2], ss[3], ss[4])
    if createSql.endswith(","):
        createSql = createSql[0:-1]
    createSql += ");"
    return createSql


def getIndexByColumnName(schema, columnName):
    if hasattr(schema, "GetColumnCnt"):
        count = schema.GetColumnCnt()
        for i in range(count):
            if schema.GetColumnName(i) == columnName:
                return i
    else:
        for i in range(len(schema)):
            if schema[i][0] == columnName:
                return i
    return -1;


def sqls(executor, dbName: str, sqls: list):
    fedbResult = None
    for sql in sqls:
        fedbResult = sql(executor, dbName, sql)
    return fedbResult


def sql(executor, dbName: str, sql: str):
    #useDB(executor,dbName)
    # fedbResult = None
    if sql.startswith("create") or sql.startswith("drop"):
        fedbResult = ddl(executor, dbName, sql)
    elif sql.startswith("insert"):
        fedbResult = insert(executor, dbName, sql)
    elif sql.startswith("load"):
        fedbResult = load(executor,sql)
    elif sql.startswith("deploy"):
        fedbResult = deploy(executor, dbName, sql)
    elif sql.__contains__("outfile"):
        fedbResult = outfile(executor, dbName, sql)
    # elif sql.startswith("show deployment"):
    #     fedbResult = showDeployment(executor,dbName,sql)
    else:
        fedbResult = select(executor, dbName, sql)
    return fedbResult

def outfile(executor, dbName: str, sql: str):
    log.info("outfile sql:"+sql)
    fedbResult = FedbResult()
    try:
        executor.execute(sql)
        time.sleep(4)
        fedbResult.ok = True
        fedbResult.msg = "ok"
    except Exception as e:
        log.info("select into  exception is {}".format(e))
        fedbResult.ok = False
        fedbResult.msg = str(e)
    log.info("select into result:" + str(fedbResult))
    return fedbResult

def useDB(executor,dbName:str):
    sql = "use {};".format(dbName)
    log.info("use sql:"+sql)
    executor.execute(sql)

def deploy(executor, dbName: str, sql: str):
    useDB(executor,dbName)
    log.info("deploy sql:"+sql)
    fedbResult = FedbResult()
    executor.execute(sql)
    fedbResult.ok = True
    fedbResult.msg = "ok"

def showDeployment(executor, dbName: str, sql: str):
    useDB(executor,dbName)
    log.info("show deployment sql:" + sql)
    fedbResult = FedbResult()
    try:
        rs = executor.execute(sql)
        fedbResult.ok = True
        fedbResult.msg = "ok"
        fedbResult.rs = rs
        fedbResult.count = rs.rowcount
        #fedbResult.result = rs.fetchall()
        fedbResult.result = convertRestultSetToListRS(rs)
    except Exception as e:
        log.info("select exception is {}".format(e))
        fedbResult.ok = False
        fedbResult.msg = str(e)
    log.info("select result:" + str(fedbResult))
    return fedbResult

def selectRequestMode(executor, dbName: str, selectSql: str, input):
    if selectSql is None or len(selectSql) == 0:
        log.error("fail to execute sql in request mode: select sql is empty")
        return None
    rows = None if input is None else input.get('rows')
    if rows is None or len(rows) == 0:
        log.error("fail to execute sql in request mode: fail to build insert sql for request rows")
        return None
    inserts = getInsertSqls(input)
    if inserts is None or len(inserts) == 0:
        log.error("fail to execute sql in request mode: fail to build insert sql for request rows")
        return None
    if len(rows) != len(inserts):
        log.error("fail to execute sql in request mode: rows size isn't match with inserts size")
        return None
    log.info("select sql:{}".format(selectSql))
    fedbResult = FedbResult()
    ok, requestRow = executor.getRequestBuilder(dbName, selectSql)
    if requestRow == None:
        fedbResult.ok = False
    else:
        result = []
        schema = None
        for index, row in enumerate(rows):
            ok, requestRow = executor.getRequestBuilder(dbName, selectSql)
            if not ok:
                fedbResult.ok = ok
                fedbResult.msg = requestRow
                return fedbResult
            if not buildRequestRow(requestRow, row):
                log.error("fail to execute sql in request mode: fail to build request row")
                return None
            ok, rs = executor.executeQuery(dbName, selectSql, requestRow)
            if not ok:
                fedbResult.ok = ok
                fedbResult.msg = rs
                return fedbResult
            schema = rs.GetSchema()
            result += convertRestultSetToList(rs, schema)
            insertResult = insert(executor, dbName, inserts[index])
            # ok,msg = executor.executeInsert(dbName,inserts[index])
            if not insertResult.ok:
                log.error("fail to execute sql in request mode: fail to insert request row after query")
                return insertResult
        fedbResult.resultSchema = schema
        fedbResult.result = result
        fedbResult.count = len(result)
        fedbResult.ok = True
        fedbResult.rs = rs
    log.info("select result:{}".format(fedbResult))
    return fedbResult


def sqlRequestMode(executor, dbName: str, sql: str, input):
    fesqlResult = None
    if sql.lower().startswith("select"):
        fesqlResult = selectRequestMode(executor, dbName, sql, input)
    else:
        log.error("unsupport sql: {}".format(sql))
    return fesqlResult


def insert(executor, dbName: str, sql: str):
    useDB(executor,dbName)
    log.info("insert sql:" + sql)
    fesqlResult = FedbResult()
    try:
        executor.execute(sql)
        fesqlResult.ok = True
        fesqlResult.msg = "ok"
    except Exception as e:
        fesqlResult.ok = False
        fesqlResult.msg = str(e)
    log.info("insert result:" + str(fesqlResult))
    return fesqlResult


def ddl(executor, dbName: str, sql: str):
    log.info("ddl sql:" + sql)
    fesqlResult = FedbResult()
    try:
        list = sql.split(" ")
        newtable = dbName.__add__(".").__add__(list[2])
        list[2] = newtable
        newsql = " ".join(list)
        log.info("ddl newsql:"+newsql)
        executor.execute(newsql)
        fesqlResult.ok = True
        fesqlResult.msg = "ok"
    except Exception as e:
        fesqlResult.ok = False
        fesqlResult.msg = str(e)
    log.info("ddl result:" + str(fesqlResult))
    return fesqlResult


def getColumnData(rs, schema, index):
    obj = None
    if rs.IsNULL(index):
        return obj
    dataType = DataTypeName(schema.GetColumnType(index))
    # print("BB:{}".format(dataType))
    if dataType == 'bool':
        obj = rs.GetBoolUnsafe(index)
    elif dataType == 'date':
        obj = rs.GetAsString(index)
    elif dataType == 'double':
        obj = rs.GetDoubleUnsafe(index)
    elif dataType == 'float':
        obj = rs.GetFloatUnsafe(index)
    elif dataType == 'int16':
        obj = rs.GetInt16Unsafe(index)
    elif dataType == 'int32':
        obj = rs.GetInt32Unsafe(index)
    elif dataType == 'int64':
        obj = rs.GetInt64Unsafe(index)
    elif dataType == 'string':
        obj = rs.GetStringUnsafe(index)
    elif dataType == 'timestamp':
        obj = rs.GetTimeUnsafe(index)
    return obj


def buildRequestRow(requestRow: SQLRequestRow, objects: list):
    schema = requestRow.GetSchema()
    totalSize = 0
    columnCount = schema.GetColumnCnt()
    for i in range(columnCount):
        if objects[i] == None:
            continue
        if DataTypeName(schema.GetColumnType(i)) == 'string':
            totalSize += len(str(objects[i]))
    log.info("init request row: {}".format(totalSize))
    requestRow.Init(totalSize)
    for i in range(columnCount):
        obj = objects[i]
        if obj is None:
            requestRow.AppendNULL()
            continue
        dataType = DataTypeName(schema.GetColumnType(i))
        if dataType == 'int16':
            requestRow.AppendInt16(obj)
        elif dataType == 'int32':
            requestRow.AppendInt32(obj)
        elif dataType == 'int64':
            requestRow.AppendInt64(obj)
        elif dataType == 'float':
            requestRow.AppendFloat(obj)
        elif dataType == 'double':
            requestRow.AppendDouble(obj)
        elif dataType == 'timestamp':
            requestRow.AppendTimestamp(obj)
        elif dataType == 'date':
            date = datetime.strptime(obj, '%Y-%m-%d')
            year = date.year
            month = date.month
            day = date.day
            log.info("build request row: obj: {}, append date: {},  {}, {}, {}".format(obj, date, year, month, day))
            requestRow.AppendDate(year, month, day)
        elif dataType == 'string':
            requestRow.AppendString(obj)
        else:
            log.error("fail to build request row: invalid data type {}".format(dataType))
            return False
    ok = requestRow.Build()
    return ok


def convertExpectTypes(expectTypes: list):
    for index, expectType in enumerate(expectTypes):
        ss = re.split("\\s+", expectType)
        dataType = ss[-1]
        if dataType == 'int16':
            expectTypes[index] = expectType[0:-len(dataType)] + "smallint"
        elif dataType == 'int32':
            expectTypes[index] = expectType[0:-len(dataType)] + "int"
        elif dataType == 'int64':
            expectTypes[index] = expectType[0:-len(dataType)] + "bigint"


def getColumnType(dataType: int):
    dataT = {feType.Bool: "bool", feType.Int16: "smallint", feType.Int32: "int", feType.Int64: "bigint",
             feType.Float: "float", feType.Double: "double", feType.String: "string", feType.Date: "date",
             feType.Timestamp: "timestamp"}
    return dataT.get(dataType, None)


def select(executor, dbName: str, sql: str):
    log.info("select sql:" + sql)
    fedbResult = FedbResult()
    try:
        rs = executor.execute(sql)
        fedbResult.ok = True
        fedbResult.msg = "ok"
        fedbResult.rs = rs
        fedbResult.count = rs.rowcount
        #fedbResult.result = rs.fetchall()
        fedbResult.result = convertRestultSetToListRS(rs)
    except Exception as e:
        log.info("select exception is {}".format(e))
        fedbResult.ok = False
        fedbResult.msg = str(e)
    log.info("select result:" + str(fedbResult))
    return fedbResult

def load(executor,sql: str):
    log.info("load sql:"+sql)
    fedbResult = FedbResult()
    try:
        executor.execute(sql)
        time.sleep(4)
        fedbResult.ok = True
        fedbResult.msg = "ok"
    except Exception as e:
        log.info("load data exception is {}".format(e))
        fedbResult.ok = False
        fedbResult.msg = str(e)
    log.info("load result:"+str(fedbResult))
    return fedbResult

def formatSql(sql: str, tableNames: list):
    if "{auto}" in sql:
        sql = sql.replace("{auto}", getRandomName())
    if "{tb_endpoint_0}" in sql:
        sql = sql.replace("{tb_endpoint_0}", fedb_config.tb_endpoint_0)
    if "{tb_endpoint_1}" in sql:
        sql = sql.replace("{tb_endpoint_1}", fedb_config.tb_endpoint_1)
    if "{tb_endpoint_2}" in sql:
        sql = sql.replace("{tb_endpoint_2}", fedb_config.tb_endpoint_2)
    indexs = formatSqlReg.findall(sql)
    for indexStr in indexs:
        index = int(indexStr)
        sql = sql.replace("{" + indexStr + "}", tableNames[index])
    return sql


def createAndInsert(executor, dbName, inputs, requestMode: bool = False):
    tableNames = []
    dbnames = set()
    dbnames.add(dbName)
    fedbResult = FedbResult()
    if inputs != None and len(inputs) > 0:
        # for index, input in enumerate(inputs):
        #     if input.__contains__('db') == True and dbnames.__contains__(input.get('db')) == False:
        #         db = input.get('db')
        #         log.info("db:" + db)
        #         createDB(executor,db)
        #         dbnames.add(db)
        #         log.info("create input db, dbName:"+db)


        for index, input in enumerate(inputs):
            # if input.__contains__('columns') == False:
            #     fedbResult.ok = True
            #     return fedbResult, tableNames
            tableName = input.get('name')
            if tableName == None:
                tableName = getRandomName()
                input['name'] = tableName
            tableNames.append(tableName)
            createSql = input.get('create')
            if createSql == None:
                createSql = getCreateSql(tableName, input['columns'], input['indexs'])
            createSql = formatSql(createSql, tableNames)
            if input.__contains__('db') == True:
                res = ddl(executor,input.get('db'),createSql)
            else:
                res = ddl(executor, dbName, createSql)
            if not res.ok:
                log.error("fail to create table")
                return res, tableNames
            if index == 0 and requestMode:
                continue
            insertSqls = getInsertSqls(input)
            for insertSql in insertSqls:
                insertSql = formatSql(insertSql, tableNames)
                res = insert(executor, dbName, insertSql)
                if not res.ok:
                    log.error("fail to insert table")
                    return res, tableNames
    fedbResult.ok = True
    return fedbResult, tableNames

def createDB(executor, dbName):
    sql = 'create database {}'.format(dbName)
    executor.execute(sql)

def getInsertSqls(input):
    insertSql = input.get('insert')
    if insertSql is not None and len(insertSql) > 0:
        return [insertSql]
    tableName = input.get('name')
    if input.__contains__('db')==True:
        tableName = input.get('db').__add__('.'+tableName)
    rows = input.get('rows')
    columns = input.get('columns')
    inserts = []
    if rows is not None and len(rows) > 0:
        for row in rows:
            datas = []
            datas.append(row)
            insert = buildInsertSQLFromRows(tableName, columns, datas)
            inserts.append(insert)
    return inserts


def buildInsertSQLFromRows(tableName: str, columns: list, datas: list):
    if columns is None or len(columns) == 0:
        return ""
    if datas is None or len(datas) == 0:
        return ""
    insertSql = "insert into " + tableName + " values"
    if datas is not None:
        for row_id, list in enumerate(datas):
            insertSql += "\n("
            for index, value in enumerate(list):
                columnType = re.split("\\s+", columns[index])[1]
                dataStr = "NULL"
                if value != None:
                    dataStr = str(value)
                    if columnType == 'string' or columnType == 'date':
                        dataStr = "'" + dataStr + "'"
                    elif columnType == 'timestamp':
                        dataStr = dataStr + "L"
                    elif columnType == 'bool':
                        dataStr = dataStr.lower()
                insertSql += dataStr + ","
            if insertSql.endswith(","):
                insertSql = insertSql[0:-1]
            if row_id < len(datas) - 1:
                insertSql += "),"
            else:
                insertSql += ");"
    return insertSql


def convertRestultSetToListRS(rs):
    result = []
    for r in rs:
        result.append(list(r))
    return result


def convertRestultSetToList(rs, schema):
    result = []
    while rs.Next():
        list = []
        columnCount = schema.GetColumnCnt()
        for i in range(columnCount):
            list.append(getColumnData(rs, schema, i))
        result.append(list)
    return result
