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

import sys
sys.path.append("../")

import logging
from openmldb_sdk import driver
from openmldb_native import sql_router_sdk
import re

# Globals
apilevel = '2.0'
paramstyle = 'qmark'
threadsafety = 3


class Type(object):
    Bool = sql_router_sdk.kTypeBool
    Int16 = sql_router_sdk.kTypeInt16
    Int32 = sql_router_sdk.kTypeInt32
    Int64 = sql_router_sdk.kTypeInt64
    Float = sql_router_sdk.kTypeFloat
    Double = sql_router_sdk.kTypeDouble
    Date = sql_router_sdk.kTypeDate
    String = sql_router_sdk.kTypeString
    Timestamp = sql_router_sdk.kTypeTimestamp

fetype_to_py = {
    sql_router_sdk.kTypeBool: Type.Bool,
    sql_router_sdk.kTypeInt16: Type.Int16,
    sql_router_sdk.kTypeInt32: Type.Int32,
    sql_router_sdk.kTypeInt64: Type.Int64,
    sql_router_sdk.kTypeFloat: Type.Float,
    sql_router_sdk.kTypeDouble: Type.Double,
    sql_router_sdk.kTypeDate: Type.Date,
    sql_router_sdk.kTypeString: Type.String,
    sql_router_sdk.kTypeTimestamp: Type.Timestamp,
}

createTableRE = re.compile("^create\s+table", re.I)
createDBRE = re.compile("^create\s+database", re.I)
createProduce = re.compile("^create\s+procedure", re.I)
insertRE = re.compile("^insert", re.I)
selectRE = re.compile("^select", re.I)
dropTable = re.compile("^drop\s+table", re.I)
dropProduce = re.compile("^drop\s+procedure", re.I)


# Exceptions module
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

class ConnectionClosedException(Error):

    def __init__(self, message):
        self.message = message

    def __str__(self):
        return repr(self.message)


class Cursor(object):

    def __init__(self, db, zk, zkPath, conn):
        self.description = None
        self.rowcount = -1
        self.arraysize = 1
        self.connection = conn
        self.db = db
        self.zk = zk
        self.zkPath = zkPath
        self._connected = True
        self._resultSet = None
        self._resultSetMetadata = None
        self._resultSetStatus = None
        self._resultSet = None
        self.lastrowid = None

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
          sql_router_sdk.kTypeBool: self._resultSet.GetBoolUnsafe,
          sql_router_sdk.kTypeInt16: self._resultSet.GetInt16Unsafe,
          sql_router_sdk.kTypeInt32: self._resultSet.GetInt32Unsafe,
          sql_router_sdk.kTypeInt64: self._resultSet.GetInt64Unsafe,
          sql_router_sdk.kTypeFloat: self._resultSet.GetFloatUnsafe,
          sql_router_sdk.kTypeDouble: self._resultSet.GetDoubleUnsafe,
          sql_router_sdk.kTypeString: self._resultSet.GetStringUnsafe,
          sql_router_sdk.kTypeDate: self._resultSet.GetAsStringUnsafe,
          sql_router_sdk.kTypeTimestamp: self._resultSet.GetTimeUnsafe
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
            raise DatabaseError("please providate data for proc")
        ok, rs = self.connection._sdk.doProc(self.db, procname, parameters)
        if not ok:
            raise DatabaseError("execute select fail")
        self._pre_process_result(rs)
        return self

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

                    # Check parameters type, like tuple or dict
                    if type(parameters) is tuple:
                        if isinstance(parameters[i], str):
                            strSize += len(parameters[i])

                    elif type(parameters) is dict:
                        if name not in parameters:
                            raise DatabaseError("col {} data not given".format(name))

                        if parameters[name] is None:
                            if schema.IsColumnNotNull(idx):
                                raise DatabaseError("column seq {} not allow null".format(idx))
                            else:
                                continue
                        colType = schema.GetColumnType(idx)
                        if colType != sql_router_sdk.kTypeString:
                            continue
                        if isinstance(parameters[name], str):
                            strSize += len(parameters[name])
                        else:
                            raise DatabaseError("{} value type is not str".format(name))
                    else:
                        # The parameters is neither tuple or dict
                        raise DatabaseError("Parameters type {} does not support: {}, should be tuple or dict".
                                            format(type(parameters), parameters))

                builder.Init(strSize)
                appendMap = {
                    sql_router_sdk.kTypeBool: builder.AppendBool,
                    sql_router_sdk.kTypeInt16: builder.AppendInt16,
                    sql_router_sdk.kTypeInt32: builder.AppendInt32,
                    sql_router_sdk.kTypeInt64: builder.AppendInt64,
                    sql_router_sdk.kTypeFloat: builder.AppendFloat,
                    sql_router_sdk.kTypeDouble: builder.AppendDouble,
                    sql_router_sdk.kTypeString: builder.AppendString,
                    # TODO: align python and java date process, 1900 problem
                    sql_router_sdk.kTypeDate: lambda x : len(x.split("-")) == 3 and builder.AppendDate(int(x.split("-")[0]), int(x.split("-")[1]), int(x.split("-")[2])),
                    sql_router_sdk.kTypeTimestamp: builder.AppendTimestamp
                }
                for i in range(len(holdIdxs)):
                    idx = holdIdxs[i]
                    name = schema.GetColumnName(idx)
                    colType = schema.GetColumnType(idx)

                    if type(parameters) is tuple:
                        ok = appendMap[colType](parameters[i])
                        if not ok:
                            raise DatabaseError("error at append data seq {}".format(i))
                    elif type(parameters) is dict:
                        if parameters[name] is None:
                            builder.AppendNULL()
                            continue
                        ok = appendMap[colType](parameters[name])
                        if not ok:
                            raise DatabaseError("error at append data seq {}".format(i))
                    else:
                        raise DatabaseError("error at append data seq {} for unsupported type".format(i))

                ok, error = self.connection._sdk.executeInsert(self.db, command, builder)
            else:
                ok, error = self.connection._sdk.executeInsert(self.db, command)
            if not ok:
                raise DatabaseError(error)
        elif selectRE.match(command):
            logging.debug("selectRE: %s", str(parameters))
            if isinstance(parameters, tuple) and len(parameters) > 0:
                ok, rs = self.connection._sdk.doParameterizedQuery(self.db, command, parameters)
            elif isinstance(parameters, dict):
                ok, rs = self.connection._sdk.doRequestQuery(self.db, command, parameters)
            else:
                ok, rs = self.connection._sdk.doQuery(self.db, command)
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
            ok, error = self.connection._sdk.executeDDL(self.db, command)
            if not ok:
                raise DatabaseError(error)
        else:
            raise DatabaseError("unsupport sql")

    @connected
    def executemany(self, operation, parameters=()):
        raise NotSupportedError("Unsupported in OpenMLDB")
        
    def get_all_tables(self):
        return self.connection._sdk.getAllTables()

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
        raise NotSupportedError("Unsupported in OpenMLDB")

    def nextset(self):
        raise NotSupportedError("Unsupported in OpenMLDB")

    def setinputsizes(self, size):
        raise NotSupportedError("Unsupported in OpenMLDB")

    def setoutputsize(self, size, columns=()):
        raise NotSupportedError("Unsupported in OpenMLDB")
        
    @connected
    def fetchall(self):
        raise NotSupportedError("Unsupported in OpenMLDB")

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

    @connected
    def getdesc(self):
        return "openmldb cursor"

    def checkCmd(cmd: str) -> bool:
        if cmd.find("select cast") == 0:
            return False
        return True

    @connected
    def get_query_metadata(self):
        raise NotSupportedError("Unsupported in OpenMLDB")

    def get_default_plugin(self):
        raise NotSupportedError("Unsupported in OpenMLDB")

    def __iter__(self):
        raise NotSupportedError("Unsupported in OpenMLDB")

    def batch_row_request(self, sql, commonCol, parameters):
        ok, rs = self.connection._sdk.doBatchRowRequest(self.db, sql, commonCol, parameters)
        if not ok:
            raise DatabaseError("execute select fail {}".format(rs))
        self._pre_process_result(rs)
        return self

    def executeRequest(self, sql, parameter): 
        command = sql.strip(' \t\n\r') 
        if selectRE.match(command) == False:
            raise Exception("Invalid opertion for request")

        ok, rs = self.connection._sdk.doRequestQuery(self.db, sql, parameter)
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
        raise NotSupportedError("Unsupported in OpenMLDB")

    @connected
    def _cursor_execute(self, cursor, statement, parameters):
        raise NotSupportedError("Unsupported in OpenMLDB")

    @connected
    def do_rollback(self, dbapi_connection):
        raise NotSupportedError("Unsupported in OpenMLDB")

    @connected
    def rollback(self):
        pass

    def commit(self):
        """
        openmldb doesn't suppport transactions
        
        So just do nothing to support this method
        """
        pass

    def close(self):
        raise NotSupportedError("Unsupported in OpenMLDB")

    def cursor(self):
        return Cursor(self._db, self._zk, self._zkPath, self)


# Constructor for creating connection to db
def connect(db, zk, zkPath):
    return Connection(db, zk, zkPath)
