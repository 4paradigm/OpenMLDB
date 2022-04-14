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
# fmt:off
import sys
import os
from typing import Union
from typing import List

sys.path.append(os.path.dirname(__file__) + "/..")
import logging
from sdk import sdk as sdk_module
from sdk.sdk import TypeUtil
from native import sql_router_sdk
import re

# fmt:on

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

insertRE = re.compile("^insert", re.I)
selectRE = re.compile("^select", re.I)


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

    def __init__(self, db, conn):
        self.description = None
        self.rowcount = -1
        self.arraysize = 1
        self.connection = conn
        self.db = db
        self._connected = True
        self._resultSet = None
        self._resultSetMetadata = None
        self._resultSetStatus = None
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

    @classmethod
    def __add_row_to_builder(cls, row, hold_idxs, schema, builder,appendMap):
        for i in range(len(hold_idxs)):
            idx = hold_idxs[i]
            name = schema.GetColumnName(idx)
            colType = schema.GetColumnType(idx)

            if isinstance(row, tuple):
                ok = appendMap[colType](row[i])
                if not ok:
                    raise DatabaseError("error at append data seq {}".format(i))
            elif isinstance(row, dict):
                if row[name] is None:
                    builder.AppendNULL()
                    continue
                ok = appendMap[colType](row[name])
                if not ok:
                    raise DatabaseError("error at append data seq {}".format(i))
            else:
                raise DatabaseError("error at append data seq {} for unsupported type".format(i))

    def execute(self, operation, parameters=()):
        command = operation.strip(' \t\n\r') if operation else None
        if command is None:
            raise Exception("None operation")
        if insertRE.match(command):
            questionMarkCount = command.count('?')
            if questionMarkCount > 0:
                if len(parameters) != questionMarkCount:
                    raise DatabaseError("parameters is not enough")
                ok, builder = self.connection._sdk.getInsertBuilder(self.db, command)
                if not ok:
                    raise DatabaseError("get insert builder fail")
                schema = builder.GetSchema()
                holdIdxs = builder.GetHoleIdx()
                appendMap = self.__get_append_map(builder, parameters, holdIdxs, schema)
                self.__add_row_to_builder(parameters, holdIdxs, schema, builder, appendMap)
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
        else:
            ok, rs = self.connection._sdk.execute_sql(self.db, command)
            if not ok:
                raise DatabaseError(rs)
            self._pre_process_result(rs)
            return self

    @classmethod
    def __get_append_map(cls, builder, row, hold_idxs, schema):
        str_size = 0
        for i in range(len(hold_idxs)):
            idx = hold_idxs[i]
            name = schema.GetColumnName(idx)
            if isinstance(row, tuple):
                if isinstance(row[i], str):
                    str_size += len(row[i])
            elif isinstance(row, dict):
                if name not in row:
                    raise DatabaseError("col {} data not given".format(name))
                if row[name] is None:
                    if schema.IsColumnNotNull(idx):
                        raise DatabaseError("column seq {} not allow null".format(name))
                    continue
                col_type = schema.GetColumnType(idx)
                if col_type != sql_router_sdk.kTypeString:
                    continue
                if isinstance(row[name], str):
                    str_size += len(row[name])
                else:
                    raise DatabaseError("{} vale type is not str".format(name))
            else:
                raise DatabaseError(
                    "parameters type {} does not support: {}, should be tuple or dict".format(type(row), row))
        builder.Init(str_size)
        appendMap = {
            sql_router_sdk.kTypeBool: builder.AppendBool,
            sql_router_sdk.kTypeInt16: builder.AppendInt16,
            sql_router_sdk.kTypeInt32: builder.AppendInt32,
            sql_router_sdk.kTypeInt64: builder.AppendInt64,
            sql_router_sdk.kTypeFloat: builder.AppendFloat,
            sql_router_sdk.kTypeDouble: builder.AppendDouble,
            sql_router_sdk.kTypeString: builder.AppendString,
            # TODO: align python and java date process, 1900 problem
            sql_router_sdk.kTypeDate: lambda x: len(x.split("-")) == 3 and builder.AppendDate(
                int(x.split("-")[0]), int(x.split("-")[1]), int(x.split("-")[2])),
            sql_router_sdk.kTypeTimestamp: builder.AppendTimestamp
        }
        return appendMap

    def __insert_rows(self, rows: List[Union[tuple, dict]], hold_idxs, schema, rows_builder, command):
        for row in rows:
            tmp_builder = rows_builder.NewRow()
            appendMap = self.__get_append_map(tmp_builder, row, hold_idxs, schema)
            self.__add_row_to_builder(row, hold_idxs, schema, tmp_builder, appendMap)
        ok, error = self.connection._sdk.executeInsert(self.db, command, rows_builder)
        if not ok:
            raise DatabaseError(error)

    @connected
    def executemany(self, operation, parameters: Union[List[tuple], List[dict]], batch_number=200):
        parameters_length = len(parameters)
        command = operation.strip(' \t\n\r') if operation else None
        if command is None:
            raise Exception("None operation")
        if command.count("?") == 0:
            logging.warning(
                "Only {} is valid, params: {} are invalid, maybe not exists mark '?' in sql".format(operation,
                                                                                                    parameters))
            return self.execute(operation, parameters)
        if isinstance(parameters, list) and parameters_length == 0:
            return self.execute(operation, parameters)

        if insertRE.match(command):
            question_mark_count = command.count("?")
            if question_mark_count > 0:
                # Because the object obtained by getInsertBatchBuilder has no GetSchema method,
                # use the object obtained by getInsertBatchBuilder
                ok, builder = self.connection._sdk.getInsertBuilder(self.db, command)
                if not ok:
                    raise DatabaseError("get insert builder fail")
                schema = builder.GetSchema()
                hold_idxs = builder.GetHoleIdx()
                for i in range(0, parameters_length, batch_number):
                    rows = parameters[i: i + batch_number]
                    ok, batch_builder = self.connection._sdk.getInsertBatchBuilder(self.db, command)
                    if not ok:
                        raise DatabaseError("get insert builder fail")
                    self.__insert_rows(rows, hold_idxs, schema, batch_builder, command)
            else:
                ok, rs = self.connection._sdk.execute_sql(self.db, command)
                if not ok:
                    raise DatabaseError(rs)
                self._pre_process_result(rs)
                return self
        else:
            raise DatabaseError("unsupport sql")

    def is_online_mode(self):
        return self.connection._sdk.isOnlineMode()
            
    def get_tables(self, db):
        return self.connection._sdk.getTables(db)

    def get_all_tables(self):
        return self.connection._sdk.getAllTables()

    def get_databases(self):
        return self.connection._sdk.getDatabases()

    def fetchone(self):
        if self._resultSet is None: raise DatabaseError("query data failed")
        ok = self._resultSet.Next()
        if not ok:
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
        if self._resultSet is None: raise DatabaseError("query data failed")
        if size is None:
            size = self.arraysize
        elif size < 0:
            raise Exception(f"Given size should greater than zero")
        values = []
        for k in range(size):
            ok = self._resultSet.Next()
            if not ok:
                break
            row = []
            for i in range(self.__schema.GetColumnCnt()):
                if self._resultSet.IsNULL(i):
                    row.append(None)
                else:
                    row.append(self.__getMap[self.__schema.GetColumnType(i)](i))
            values.append(tuple(row))
        return values

    def nextset(self):
        raise NotSupportedError("Unsupported in OpenMLDB")

    def setinputsizes(self, size):
        raise NotSupportedError("Unsupported in OpenMLDB")

    def setoutputsize(self, size, columns=()):
        raise NotSupportedError("Unsupported in OpenMLDB")

    @connected
    def fetchall(self):
        return self.fetchmany(size=self.rowcount)

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

    def get_resultset_schema(self):
        # Return the readable schema list like [{'name': 'col1', 'type': 'int64'}, {'name': 'col2', 'type': 'date'}]

        c_schema = self._resultSet.GetSchema()
        col_num = c_schema.GetColumnCnt()

        outputSchemaList = []
        for i in range(col_num):
            col_name = c_schema.GetColumnName(i)
            col_type = c_schema.GetColumnType(i)
            col_type_str = TypeUtil.intTypeToStr(col_type)
            outputSchemaList.append({"name": col_name, "type": col_type_str})

        return outputSchemaList


class Connection(object):

    def __init__(self, db, is_cluster_mode, zk_or_host, zkPath_or_port):
        self._connected = True
        self._db = db
        if is_cluster_mode:
            options = sdk_module.OpenMLDBClusterSdkOptions(zk_or_host, zkPath_or_port)
        else:
            options = sdk_module.OpenMLDBStandaloneSdkOptions(zk_or_host, zkPath_or_port)
        sdk = sdk_module.OpenMLDBSdk(options, is_cluster_mode)
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
        return Cursor(self._db, self)


# Constructor for creating connection to db
def connect(db, zk=None, zkPath=None, host=None, port=None):
    # standalone
    if isinstance(zkPath, int):
        host, port = zk, zkPath
        return Connection(db, False, host, port)
    # cluster
    elif isinstance(zkPath, str):
        return Connection(db, True, zk, zkPath)
    elif zkPath is None:
        return Connection(db, False, host, int(port))
