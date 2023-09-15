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
from pathlib import Path

# add parent directory
sys.path.append(Path(__file__).parent.parent.as_posix())
from sdk import sdk as sdk_module
from sdk.sdk import TypeUtil
from native import sql_router_sdk

import logging
from typing import List
from typing import Union
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


def build_sorted_holes(hole_idxes):
    # hole idxes is in stmt order, we should use schema order to append
    hole_pairs = [(hole_idxes[i], i) for i in range(len(hole_idxes))]
    return sorted(hole_pairs, key=lambda elem: elem[0])


class Cursor(object):

    def __init__(self, conn):
        self.description = None
        self.rowcount = -1
        self.arraysize = 1
        self.connection = conn
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
        self.description = [(
            self.__schema.GetColumnName(i),
            fetype_to_py[self.__schema.GetColumnType(i)],
            None,
            None,
            None,
            None,
            True,
        ) for i in range(self.__schema.GetColumnCnt())]

    def callproc(self, procname, parameters=()):
        if len(parameters) < 1:
            raise DatabaseError("please providate data for proc")
        ok, rs = self.connection._sdk.doProc(None, procname, parameters)
        if not ok:
            raise DatabaseError("callproc failed, {}".format(rs))
        self._pre_process_result(rs)
        return self

    @classmethod
    def __add_row_to_builder(cls, row, hole_pairs, schema, builder, append_map):
        for i in range(len(hole_pairs)):
            idx = hole_pairs[i][0]
            name = schema.GetColumnName(idx)
            col_type = schema.GetColumnType(idx)
            row_idx = hole_pairs[i][1]
            if isinstance(row, tuple):
                ok = append_map[col_type](row[row_idx])
                if not ok:
                    raise DatabaseError(
                        "error at append data seq {}".format(row_idx))
            elif isinstance(row, dict):
                if row[name] is None:
                    builder.AppendNULL()
                    continue
                ok = append_map[col_type](row[name])
                if not ok:
                    raise DatabaseError(
                        "error at append data seq {}".format(row_idx))
            else:
                raise DatabaseError(
                    "error at append data seq {} for unsupported row type".
                    format(row_idx))

    def execute(self, operation, parameters=()):
        if not operation:
            raise Exception("None operation")

        command = operation.strip(' \t\n\r')
        
        if insertRE.match(command):
            question_mark_count = command.count('?')
            
            if question_mark_count > 0:
                if len(parameters) != question_mark_count:
                    raise DatabaseError("parameters is not enough")
                
                ok, builder = self.connection._sdk.getInsertBuilder(None, command)
                if not ok:
                    raise DatabaseError(f"get insert builder fail, error: {builder}")
                
                schema = builder.GetSchema()
                hole_idxes = builder.GetHoleIdx()
                sorted_holes = build_sorted_holes(hole_idxes)
                append_map = self.__get_append_map(builder, parameters, hole_idxes, schema)
                self.__add_row_to_builder(parameters, sorted_holes, schema, builder, append_map)
                ok, error = self.connection._sdk.executeInsert(None, command, builder)
            else:
                ok, error = self.connection._sdk.executeInsert(None, command)

            if not ok:
                raise DatabaseError(error)
        
        elif selectRE.match(command):
            if parameters:
                logging.debug("selectRE: %s", str(parameters))

            if isinstance(parameters, tuple) and len(parameters) > 0:
                ok, rs = self.connection._sdk.doParameterizedQuery(None, command, parameters)
            elif isinstance(parameters, dict):
                ok, rs = self.connection._sdk.doRequestQuery(None, command, parameters)
            else:
                ok, rs = self.connection._sdk.doQuery(None, command)

            if not ok:
                raise DatabaseError(f"execute select fail, error: {rs}")

            self._pre_process_result(rs)
            return self
        
        else:
            ok, rs = self.connection._sdk.execute(command)
            if not ok:
                raise DatabaseError(rs)
            
            self._pre_process_result(rs)
            return self

    @classmethod
    def __get_append_map(cls, builder, row, hole_idxes, schema):
        # calc str total length
        str_size = 0
        for i in range(len(hole_idxes)):
            idx = hole_idxes[i]
            name = schema.GetColumnName(idx)
            if isinstance(row, tuple):
                if isinstance(row[i], str):
                    str_size += len(row[i])
            elif isinstance(row, dict):
                if name not in row:
                    raise DatabaseError("col {} data not given".format(name))
                if row[name] is None:
                    if schema.IsColumnNotNull(idx):
                        raise DatabaseError(
                            "column seq {} not allow null".format(name))
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
                    "parameters type {} does not support: {}, should be tuple or dict"
                    .format(type(row), row))
        builder.Init(str_size)
        append_map = {
            sql_router_sdk.kTypeBool:
                builder.AppendBool,
            sql_router_sdk.kTypeInt16:
                builder.AppendInt16,
            sql_router_sdk.kTypeInt32:
                builder.AppendInt32,
            sql_router_sdk.kTypeInt64:
                builder.AppendInt64,
            sql_router_sdk.kTypeFloat:
                builder.AppendFloat,
            sql_router_sdk.kTypeDouble:
                builder.AppendDouble,
            sql_router_sdk.kTypeString:
                builder.AppendString,
            # TODO: align python and java date process, 1900 problem
            sql_router_sdk.kTypeDate:
                lambda x: len(x.split("-")) == 3 and builder.AppendDate(
                    int(x.split("-")[0]), int(x.split("-")[1]),
                    int(x.split("-")[2])),
            sql_router_sdk.kTypeTimestamp:
                builder.AppendTimestamp
        }
        return append_map

    def __insert_rows(self, rows: List[Union[tuple, dict]], hole_idxes,
                      sorted_holes, schema, rows_builder, command):
        for row in rows:
            tmp_builder = rows_builder.NewRow()
            append_map = self.__get_append_map(tmp_builder, row, hole_idxes,
                                               schema)
            self.__add_row_to_builder(row, sorted_holes, schema, tmp_builder,
                                      append_map)
        ok, error = self.connection._sdk.executeInsert(None, command,
                                                       rows_builder)
        if not ok:
            raise DatabaseError(error)

    @connected
    def executemany(self,
                    operation,
                    parameters: Union[List[tuple], List[dict]],
                    batch_number=200):
        if not operation:
            raise Exception("None operation")

        command = operation.strip(' \t\n\r')
        parameters_length = len(parameters)
        question_mark_count = command.count("?")

        if question_mark_count == 0:
            logging.warning(
                f"Only {operation} is valid, params: {parameters} are invalid, maybe not exists mark '?' in sql")
            return self.execute(operation, parameters)

        if isinstance(parameters, list) and parameters_length == 0:
            return self.execute(operation, parameters)

        if insertRE.match(command):
            if question_mark_count > 0:
                ok, builder = self.connection._sdk.getInsertBuilder(None, command)
                if not ok:
                    raise DatabaseError(f"get insert builder fail, error: {builder}")

                schema = builder.GetSchema()
                hole_idxes = builder.GetHoleIdx()
                hole_pairs = build_sorted_holes(hole_idxes)

                for i in range(0, parameters_length, batch_number):
                    rows = parameters[i:i + batch_number]
                    ok, batch_builder = self.connection._sdk.getInsertBatchBuilder(None, command)
                    if not ok:
                        raise DatabaseError("get insert builder fail")

                    self.__insert_rows(rows, hole_idxes, hole_pairs, schema, batch_builder, command)
            else:
                ok, rs = self.connection._sdk.execute(command)
                if not ok:
                    raise DatabaseError(rs)
                self._pre_process_result(rs)
                return self
        else:
            raise DatabaseError("unsupported sql")

    def is_online_mode(self):
        return self.connection._sdk.isOnlineMode()

    def get_tables(self, db):
        return self.connection._sdk.getTables(db)

    def get_all_tables(self):
        return self.connection._sdk.getAllTables()

    def get_databases(self):
        return self.connection._sdk.getDatabases()

    def fetchone(self):
        if self._resultSet is None:
            raise DatabaseError("resultset is not set")
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
        if self._resultSet is None:
            raise DatabaseError("resultset is not set")
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
        ok, rs = self.connection._sdk.doBatchRowRequest(None, sql, commonCol,
                                                        parameters)
        if not ok:
            raise DatabaseError("execute select fail {}".format(rs))
        self._pre_process_result(rs)
        return self

    def executeRequest(self, sql, parameter):
        command = sql.strip(' \t\n\r')
        if selectRE.match(command) == False:
            raise Exception("Invalid opertion for request")

        ok, rs = self.connection._sdk.doRequestQuery(None, sql, parameter)
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

    def __init__(self, **cparams):
        sdk = sdk_module.OpenMLDBSdk(**cparams)
        sdk.init()
        db = cparams.get('database', None)
        if db:
            ok, rs = sdk.execute(f'use {db}')  # set db, db must be exists
            if not ok:
                raise DatabaseError(rs)
        self._sdk = sdk
        self._connected = True

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
        self._sdk = None

    def cursor(self):
        return Cursor(self)


# Constructor for creating connection to db
def connect(*cargs, **cparams):
    return Connection(**cparams)
