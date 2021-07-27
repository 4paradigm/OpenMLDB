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

from . import driver
import re

apilevel = '2.0'
paramstyle = 'qmark'
threadsafety = 3

class Type(object):
    Bool = driver.sql_router_sdk.kTypeBool
    Int16 = driver.sql_router_sdk.kTypeInt16
    Int32 = driver.sql_router_sdk.kTypeInt32
    Int64 = driver.sql_router_sdk.kTypeInt64
    Float = driver.sql_router_sdk.kTypeFloat
    Double = driver.sql_router_sdk.kTypeDouble
    Date = driver.sql_router_sdk.kTypeDate
    String = driver.sql_router_sdk.kTypeString
    Timestamp = driver.sql_router_sdk.kTypeTimestamp

fetype_to_py = {
    driver.sql_router_sdk.kTypeBool: Type.Bool,
    driver.sql_router_sdk.kTypeInt16: Type.Int16,
    driver.sql_router_sdk.kTypeInt32: Type.Int32,
    driver.sql_router_sdk.kTypeInt64: Type.Int64,
    driver.sql_router_sdk.kTypeFloat: Type.Float,
    driver.sql_router_sdk.kTypeDouble: Type.Double,
    driver.sql_router_sdk.kTypeDate: Type.Date,
    driver.sql_router_sdk.kTypeString: Type.String,
    driver.sql_router_sdk.kTypeTimestamp: Type.Timestamp,
}

createTableRE = re.compile("^create\s+table", re.I)
createDBRE = re.compile("^create\s+database", re.I)
createProduce = re.compile("^create\s+procedure", re.I)
insertRE = re.compile("^insert", re.I)
selectRE = re.compile("^select", re.I)
dropTable = re.compile("^drop\s+table", re.I)
dropProduce = re.compile("^drop\s+procedure", re.I)



class Error(Exception):

    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message

    def msg(self):
        return self.message

class Warning(Exception):

    def __init__(self, message):
        self.message = message

class InterfaceError(Error):

    def __init__(self, message):
        self.message = message

class DatabaseError(Error):

    def __init__(self, message):
        self.message = message

class DataError(DatabaseError):

    def __init__(self, message):
        self.message = message

class OperationalError(DatabaseError):

    def __init__(self, message):
        self.message = message

class IntegrityError(DatabaseError):

    def __init__(self, message):
        self.message = message

class InternalError(DatabaseError):

    def __init__(self, message):
        self.message = message

class ProgrammingError(DatabaseError):

    def __init__(self, message):
        self.message = message

class NotSupportedError(DatabaseError):

    def __init__(self, message):
        self.message = message

class CursorClosedException(Error):

    def __init__(self, message):
        self.message = message

    def __str__(self):
        return repr(self.message)

class Cursor(object):

    def __init__(self, db, zk, zkPath, conn):

        self.arraysize = 1
        self.description = None
        self.connection = conn
        self.db = db
        self.zk = zk
        self.zkPath = zkPath
        self._connected = True
        self._resultSet = None
        self._resultSetMetadata = None
        self._resultSetStatus = None
        self.rowcount = -1
        self._resultSet = None

    def connected(func):
        def func_wrapper(self, *args, **kwargs):
            if self._connected is False:
                raise CursorClosedException("Cursor object is closed")
            elif self.connection._connected is False:
                raise ConnectionClosedException("Connection object is closed")
            else:
                return func(self, *args, **kwargs)
        return func_wrapper

    @connected
    def close(self):
        self._connected = False

    def _pre_process_result(self, rs):
        if rs is None:
            self.rowcount = 0
            return
        self.rowcount = rs.Size()
        self._resultSet = rs
        self.__schema = rs.GetSchema()
        self.__getMap = {
          driver.sql_router_sdk.kTypeBool: self._resultSet.GetBoolUnsafe,
          driver.sql_router_sdk.kTypeInt16: self._resultSet.GetInt16Unsafe,
          driver.sql_router_sdk.kTypeInt32: self._resultSet.GetInt32Unsafe,
          driver.sql_router_sdk.kTypeInt64: self._resultSet.GetInt64Unsafe,
          driver.sql_router_sdk.kTypeFloat: self._resultSet.GetFloatUnsafe,
          driver.sql_router_sdk.kTypeDouble: self._resultSet.GetDoubleUnsafe,
          driver.sql_router_sdk.kTypeString: self._resultSet.GetStringUnsafe,
          driver.sql_router_sdk.kTypeDate: self._resultSet.GetAsString,
          driver.sql_router_sdk.kTypeTimestamp: self._resultSet.GetTimeUnsafe
        }
        self.description = [
            (
                self.__schema.GetColumnName(i),
                fetype_to_py[self.__schema.GetColumnType(i)],
                None,
                None,
                None,
                None,
                True,
            )
            for i in range(self.__schema.GetColumnCnt())
        ]

    def callproc(self, procname, parameters=()):
        if len(parameters) < 1:
            raise DataBaseError("please providate data for proc")
        ok, rs = self.connection._sdk.doProc(self.db, procname, parameters)
        if not ok:
            raise DatabaseError("execute select fail")
        self._pre_process_result(rs)
        return self


    @connected
    def getdesc(self):
        return "openmldb cursor"

    def checkCmd(cmd: str) -> bool:
        if cmd.find("select cast") == 0:
            return false
        return true

    def execute(self, operation, parameters=()):
        command = operation.strip(' \t\n\r') if operation else None
        if command is None:
            raise Exception("None operation")
        semicolonCount = command.count(';')
        escapeSemicolonCount = command.count("\;")
        if createTableRE.match(command):
            if escapeSemicolonCount > 1:
                raise Exception("invalid table name")
            ok, error = self.connection._sdk.executeDDL(self.db, command)
            if not ok:
                raise DatabaseError(error)
        elif createDBRE.match(command):
            db = command.split()[-1].rstrip(";")
            ok, error = self.connection._sdk.createDB(db)
            if not ok:
                raise DatabaseError(error)
        elif insertRE.match(command):
            questionMarkCount = command.count('?');
            if questionMarkCount > 0:
                if len(parameters) != questionMarkCount:
                    raise DatabaseError("parameters is not enough")
                ok, builder = self.connection._sdk.getInsertBuilder(self.db, command)
                if not ok:
                    raise DatabaseError("get insert builder fail")
                schema = builder.GetSchema()
                holdIdxs = builder.GetHoleIdx()
                strSize = 0
                for i in range(len(holdIdxs)):
                    idx = holdIdxs[i]
                    name = schema.GetColumnName(idx)
                    if name not in parameters:
                        return False, "col {} data not given".format(name)
                    if parameters[name] is None:
                        if schema.IsColumnNotNull(idx):
                            raise DatabaseError("column seq {} not allow null".format(idx))
                        else:
                            continue
                    colType = schema.GetColumnType(idx)
                    if colType != driver.sql_router_sdk.kTypeString:
                        continue
                    if isinstance(parameters[name], str):
                        strSize += len(parameters[name])
                    else:
                        raise DatabaseError("{} value tpye is not str".format(name))
                builder.Init(strSize)
                appendMap = {
                    driver.sql_router_sdk.kTypeBool: builder.AppendBool,
                    driver.sql_router_sdk.kTypeInt16: builder.AppendInt16,
                    driver.sql_router_sdk.kTypeInt32: builder.AppendInt32,
                    driver.sql_router_sdk.kTypeInt64: builder.AppendInt64,
                    driver.sql_router_sdk.kTypeFloat: builder.AppendFloat,
                    driver.sql_router_sdk.kTypeDouble: builder.AppendDouble,
                    driver.sql_router_sdk.kTypeString: builder.AppendString,
                    # TODO: align python and java date process, 1900 problem
                    driver.sql_router_sdk.kTypeDate: lambda x : len(x.split("-")) == 3 and builder.AppendDate(int(x.split("-")[0]), int(x.split("-")[1]), int(x.split("-")[2])),
                    driver.sql_router_sdk.kTypeTimestamp: builder.AppendTimestamp
                    }
                for i in range(len(holdIdxs)):
                    idx = holdIdxs[i]
                    name = schema.GetColumnName(idx)
                    if parameters[name] is None:
                        builder.AppendNULL()
                        continue
                    colType = schema.GetColumnType(idx)
                    ok = appendMap[colType](parameters[name])
                    if not ok:
                        raise DatabaseError("erred at append data seq {}".format(i));
                ok, error = self.connection._sdk.executeInsert(self.db, command, builder)
            else:
                ok, error = self.connection._sdk.executeInsert(self.db, command)
            if not ok:
                raise DatabaseError(error)
        elif selectRE.match(command):
            if isinstance(parameters, dict): # single element in tuple, that tuple type is dict
                ok, rs = self.connection._sdk.doQuery(self.db, command, parameters)
            else:
                ok, rs = self.connection._sdk.doQuery(self.db, command, None)
            if not ok:
                raise DatabaseError("execute select fail, msg: {}".format(rs))
            self._pre_process_result(rs)
            return self
        elif createProduce.match(command):
            ok, error = self.connection._sdk.executeDDL(self.db, command)
            if not ok:
                raise DatabaseError(error)
        elif dropTable.match(command):
            ok, error = self.connection._sdk.executeDDL(self.db, command)
            if not ok:
                raise DatabaseError(error)
        elif dropProduce.match(command):
            self.connection._sdk.executeDDL(self.db, command)
            if not ok:
                raise DatabaseError(error)
        else:
            raise DatabaseError("unsupport sql")

    @connected
    def executemany(self, operation, parameters=()):
        pass

    @staticmethod
    def substitute_in_query(string_query, parameters):
        query = string_query
        return query

    @staticmethod
    def parse_column_types(metadata):
        names = []
        types = []
        for row in metadata:
            names.append(row["column"])
            types.append(row["row"].lower())

    def fetchone(self):
        if self._resultSet is None: return "call fetchone"
        ok = self._resultSet.Next()
        if not ok:
            self.rowcount = -1
            self._resultSet = None
            self.__schema = None
            self.__getMap = None
            return None
        values = []
        for i in range(self.__schema.GetColumnCnt()):
            if self._resultSet.IsNULL(i):
                values.append(None)
            else:
                values.append(self.__getMap[self.__schema.GetColumnType(i)](i))
        return tuple(values)

    @connected
    def fetchmany(self, size=None):
        pass

    def nextset(self):
        pass

    def setinputsizes(self, size):
        pass

    def setoutputsize(self, size, columns=()):
        pass
        
    @connected
    def fetchall(self):
        pass

    @connected
    def get_query_metadata(self):
        pass

    def get_default_plugin(self):
        pass

    def __iter__(self):
        pass

    def batch_row_request(self, sql, commonCol, parameters):
        ok, rs = self.connection._sdk.doBatchRowRequest(self.db, sql, commonCol, parameters)
        if not ok:
            raise DatabaseError("execute select fail {}".format(rs))
        self._pre_process_result(rs)
        return self


class Connection(object):

    def __init__(self, db, zk, zkPath):
        self._connected = True
        self._db = db
        self._zk = zk
        self._zkPath = zkPath
        options = driver.DriverOptions(zk, zkPath)
        sdk = driver.Driver(options)
        ok = sdk.init()
        if not ok:
            raise Exception("init openmldb sdk erred")
        self._sdk = sdk

    def connected(func):
        def func_wrapper(self, *args, **kwargs):
            if self._connected is False:
                raise ConnectionClosedException("Connection object is closed")
            else:
                func(self, *args, **kwargs)

        return func_wrapper


    def execute(self):
        pass

    def close(self):
        pass

    def cursor(self):
        return Cursor(self._db, self._zk, self._zkPath, self)

    @connected
    def _cursor_execute(self, cursor, statement, parameters):
        pass

    @connected
    def do_rollback(self, dbapi_connection):
        pass

    @connected
    def rollback(self):
        pass

    def commit(self):
        """
        openmldb doesn't suppport transactions
        
        So just do nothing to support this method
        """
        pass

def connect(db, zk, zkPath):
    return Connection(db, zk, zkPath)
