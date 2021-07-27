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


from entity.fesql_result import FesqlResult
from nb_log import LogManager
from sqlalchemy_openmldb.openmldbapi import Type as feType
import re
import random
import string
from datetime import datetime
import common.fesql_config as fesql_config
import time

log = LogManager('fesql-auto-test').get_logger_and_add_handlers()

reg = "\\{(\\d+)\\}"
formatSqlReg = re.compile(reg)

def getRandomName():
    tableName = ''.join(random.sample(string.ascii_letters + string.digits, 8))
    tableName = "auto_"+tableName
    return tableName

def getCreateSql(name:str,columns:list,inexs:list):
    time.sleep(1)
    createSql = "create table "+name+"("
    for column in columns:
        createSql+=column+","
    for index in inexs:
        ss = index.split(":")
        if len(ss)>=2:
            keyStr = ss[1]
            if("|" in ss[1]):
                keyStr = ",".join(ss[1].split("|"))
        if len(ss)==2:
            createSql+="index(key=(%s))," % (keyStr)
        elif len(ss)==3:
            createSql+="index(key=(%s),ts=%s)," % (keyStr,ss[2])
        elif len(ss)==4:
            createSql += "index(key=(%s),ts=%s,ttl=%s)," % (keyStr, ss[2],ss[3])
        elif len(ss)==5:
            createSql += "index(key=(%s),ts=%s,ttl=%s,ttl_type=%s)," % (keyStr, ss[2], ss[3],ss[4])
    if createSql.endswith(","):
        createSql = createSql[0:-1]
    createSql+=");"
    return createSql

def getIndexByColumnName(schema,columnName):
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

def sqls(executor,dbName:str,sqls:list):
    fesqlResult = None
    for sql in sqls:
        fesqlResult = sql(executor,dbName,sql)
    return fesqlResult
def sql(executor,dbName:str,sql:str):
    # fesqlResult = None
    if sql.startswith("create") or sql.startswith("drop") :
        fesqlResult = ddl(executor,dbName,sql)
    elif sql.startswith("insert") :
        fesqlResult = insert(executor,dbName,sql)
    else:
        fesqlResult = select(executor,dbName,sql)
    return fesqlResult

def selectRequestMode(executor,dbName:str,selectSql:str,input):
    if selectSql == None or len(selectSql)==0:
        log.error("fail to execute sql in request mode: select sql is empty")
        return None
    rows = None if input == None else input.get('rows')
    if rows==None or len(rows)==0:
        log.error("fail to execute sql in request mode: fail to build insert sql for request rows")
        return None
    inserts = getInsertSqls(input)
    if inserts==None or len(inserts)==0:
        log.error("fail to execute sql in request mode: fail to build insert sql for request rows")
        return None
    if len(rows)!=len(inserts):
        log.error("fail to execute sql in request mode: rows size isn't match with inserts size")
        return None
    log.info("select sql:{}".format(selectSql))
    fesqlResult = FesqlResult()
    ok,requestRow = executor.getRequestBuilder(dbName,selectSql)
    if requestRow==None:
        fesqlResult.ok = False
    else:
        result = []
        schema = None
        for index,row in enumerate(rows):
            ok, requestRow = executor.getRequestBuilder(dbName, selectSql)
            if not ok:
                fesqlResult.ok = ok
                fesqlResult.msg = requestRow
                return fesqlResult
            if not buildRequestRow(requestRow,row):
                log.error("fail to execute sql in request mode: fail to build request row")
                return None
            ok,rs = executor.executeQuery(dbName,selectSql,requestRow)
            if not ok:
                fesqlResult.ok = ok
                fesqlResult.msg = rs
                return fesqlResult
            schema = rs.GetSchema()
            result += convertRestultSetToList(rs,schema)
            insertResult = insert(executor,dbName,inserts[index])
            # ok,msg = executor.executeInsert(dbName,inserts[index])
            if not insertResult.ok :
                log.error("fail to execute sql in request mode: fail to insert request row after query")
                return insertResult
        fesqlResult.resultSchema=schema
        fesqlResult.result=result
        fesqlResult.count=len(result)
        fesqlResult.ok = True
        fesqlResult.rs = rs
    log.info("select result:{}".format(fesqlResult))
    return fesqlResult

def sqlRequestMode(executor,dbName:str,sql:str,input):
    fesqlResult = None
    if sql.lower().startswith("select") :
        fesqlResult = selectRequestMode(executor,dbName,sql,input)
    else :
        log.error("unsupport sql: {}".format(sql))
    return fesqlResult

def insert(executor,dbName:str,sql:str):
    log.info("insert sql:" + sql)
    fesqlResult = FesqlResult()
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
    log.info("ddl sql:"+sql)
    fesqlResult = FesqlResult()
    try:
        executor.execute(sql)
        fesqlResult.ok = True
        fesqlResult.msg = "ok"
    except Exception as e:
        fesqlResult.ok = False
        fesqlResult.msg = str(e)
    log.info("ddl result:"+str(fesqlResult))
    return fesqlResult

def getColumnData(rs,schema,index):
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
    # print("cc:{}".format(obj))
    return obj

def convertExpectTypes(expectTypes:list):
    for index,expectType in enumerate(expectTypes):
        ss = re.split("\\s+",expectType)
        dataType = ss[-1]
        if dataType == 'int16':
            expectTypes[index] = expectType[0:-len(dataType)]+"smallint"
        elif dataType == 'int32':
            expectTypes[index] = expectType[0:-len(dataType)]+"int"
        elif dataType == 'int64':
            expectTypes[index] = expectType[0:-len(dataType)]+"bigint"

def getColumnType(dataType:int):
    dataT = {feType.Bool:"bool", feType.Int16:"smallint", feType.Int32:"int", feType.Int64:"bigint", feType.Float:"float", feType.Double:"double", feType.String:"string", feType.Date:"date", feType.Timestamp:"timestamp"}
    return dataT.get(dataType, None)

def select(executor, dbName: str, sql: str):
    log.info("select sql:"+sql)
    fesqlResult = FesqlResult()
    try:
        rs = executor.execute(sql)
        fesqlResult.ok = True
        fesqlResult.msg = "ok"
        fesqlResult.rs = rs
        fesqlResult.count = rs.rowcount
        fesqlResult.result = convertRestultSetToListRS(rs)
    except Exception as e:
        log.info("select exception is {}".format(e))
        fesqlResult.ok = False
        fesqlResult.msg = str(e)
    log.info("select result:"+str(fesqlResult))
    return fesqlResult

def formatSql(sql:str,tableNames:list):
    if "{auto}" in sql:
        sql = sql.replace("{auto}", getRandomName())
    if "{tb_endpoint_0}" in sql:
        sql = sql.replace("{tb_endpoint_0}", fesql_config.tb_endpoint_0)
    if "{tb_endpoint_1}" in sql:
        sql = sql.replace("{tb_endpoint_1}", fesql_config.tb_endpoint_1)
    if "{tb_endpoint_2}" in sql:
        sql = sql.replace("{tb_endpoint_2}", fesql_config.tb_endpoint_2)
    indexs = formatSqlReg.findall(sql)
    for indexStr in indexs:
         index = int(indexStr)
         sql = sql.replace("{" + indexStr + "}",tableNames[index])
    return sql

def createAndInsert(executor,dbName,inputs,requestMode:bool=False):
    tableNames = []
    fesqlResult = FesqlResult()
    if inputs!=None and len(inputs)>0:
        for index,input in enumerate(inputs):
            tableName = input.get('name')
            if tableName==None:
                tableName = getRandomName()
                input['name'] = tableName
            tableNames.append(tableName)
            createSql = input.get('create')
            if createSql==None:
                createSql=getCreateSql(tableName,input['columns'],input['indexs'])
            createSql = formatSql(createSql,tableNames)
            res = ddl(executor,dbName,createSql)
            if not res.ok :
                log.error("fail to create table")
                return res,tableNames
            if index==0 and requestMode:
                continue
            insertSqls = getInsertSqls(input)
            for insertSql in insertSqls:
                insertSql = formatSql(insertSql, tableNames)
                res = insert(executor, dbName, insertSql)
                if not res.ok:
                    log.error("fail to insert table")
                    return res, tableNames
    fesqlResult.ok = True
    return fesqlResult,tableNames

def getInsertSqls(input):
    insertSql = input.get('insert')
    if insertSql!=None and len(insertSql)>0 :
        return [insertSql]
    tableName = input.get('name')
    rows = input.get('rows')
    columns = input.get('columns')
    inserts = []
    if rows!= None and len(rows)>0:
        for row in rows:
            datas = []
            datas.append(row)
            insert = buildInsertSQLFromRows(tableName,columns,datas)
            inserts.append(insert)
    return inserts

def buildInsertSQLFromRows(tableName:str,columns:list,datas:list):
    if columns==None or len(columns)==0:
        return ""
    if datas==None or len(datas)==0:
        return ""
    insertSql = "insert into " + tableName + " values"
    if datas != None:
        for row_id,list in enumerate(datas):
            insertSql+="\n("
            for index, value in enumerate(list):
                columnType = re.split("\\s+", columns[index])[1]
                dataStr="NULL"
                if value != None :
                    dataStr = str(value)
                    if columnType == 'string' or columnType == 'date':
                        dataStr = "'" + dataStr + "'"
                    elif columnType == 'timestamp':
                        dataStr = dataStr + "L"
                insertSql += dataStr + ","
            if insertSql.endswith(","):
                insertSql = insertSql[0:-1]
            if row_id < len(datas)-1:
                insertSql += "),"
            else:
                insertSql += ");"
    return insertSql
"""
def buildRequestRow(requestRow:SQLRequestRow,objects:list):
    schema = requestRow.GetSchema()
    totalSize = 0
    columnCount = schema.GetColumnCnt()
    for i in range(columnCount):
        if objects[i] == None:
            continue
        if DataTypeName(schema.GetColumnType(i))=='string':
            totalSize+=len(str(objects[i]))
    log.info("init request row: {}".format(totalSize))
    requestRow.Init(totalSize)
    for i in range(columnCount):
        obj = objects[i]
        if obj==None:
            requestRow.AppendNULL()
            continue
        dataType = DataTypeName(schema.GetColumnType(i))
        if dataType=='int16':
            requestRow.AppendInt16(obj)
        elif dataType =='int32':
            requestRow.AppendInt32(obj)
        elif dataType =='int64':
            requestRow.AppendInt64(obj)
        elif dataType == 'float':
            requestRow.AppendFloat(obj)
        elif dataType == 'double':
            requestRow.AppendDouble(obj)
        elif dataType == 'timestamp':
            requestRow.AppendTimestamp(obj)
        elif dataType == 'date':
            date = datetime.strptime(obj,'%Y-%m-%d')
            year = date.year
            month = date.month
            day = date.day
            log.info("build request row: obj: {}, append date: {},  {}, {}, {}".format(obj,date,year,month,day))
            requestRow.AppendDate(year,month,day)
        elif dataType =='string':
            requestRow.AppendString(obj)
        else:
            log.error("fail to build request row: invalid data type {}".format(dataType))
            return False
    ok = requestRow.Build()
    return ok
"""

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

