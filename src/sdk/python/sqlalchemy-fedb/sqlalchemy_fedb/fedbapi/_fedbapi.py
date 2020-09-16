from . import driver
import re

apilevel = '2.0'
paramstyle = 'qmark'
threadsafety = 3

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
        self.arraysize = 1

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
        print("cursor close")
        self._connected = False

    def callproc(self, procname, parameters=()):
        pass

    @connected
    def getdesc(self):
        return "fedb cursor"

    def checkCmd(cmd: str) -> bool:
        if cmd.find("select cast") == 0:
            return false
        return true

    @connected
    def execute(self, operation, parameters=()):
        command = operation.strip(' \t\n\r') if operation else None
        if command is None:
            raise Exception("None operation")
        semicolonCount = command.count(';')
        escapeSemicolonCount = command.count("\;")
        if command.find("create table ") == 0:
            if escapeSemicolonCount > 1:
                raise Exception("invalid table name")
            ok, error = self.connection._sdk.executeDDL(self.db, command)
            if not ok:
                raise DatabaseError(error)
        elif command.find("create database ") == 0:
            db = command.split()[-1].rstrip(";")
            ok, error = self.connection._sdk.createDB(db)
            if not ok:
                raise DatabaseError(error)
        elif command.find("insert into ") == 0:
            questionMarkCount = command.count('?');
            if questionMarkCount > 0:
                if len(parameters) != questionMarkCount:
                    raise DatabaseError("parameters is not enough")
                ok, builder = self.connection._sdk.getInsertBuilder(self.db, command)
                if not ok:
                    raise DatabaseError(error)
                schema = builder.GetSchema()
                holdIdxs = builder.GetHoleIdx()
                strSize = 0
                for i in range(len(holdIdxs)):
                    idx = holdIdxs[i];
                    colType = schema.GetColumnType(idx)
                    if colType != driver.sql_router_sdk.kTypeString:
                        continue
                    if parameters[i] == None:
                        if schema.get(idx).IsColumnNotNull:
                            raise DatabaseError("column seq {} not allow null".format(idx))
                        else:
                            continue
                    if isinstance(parameters[i], str):
                        strSize += len(parameters[i])
                    else:
                        raise DatabaseError("value {} tpye is not str".format(parameters[i]))
                builder.Init(strSize)
                appendMap = {
                    driver.sql_router_sdk.kTypeBool: builder.AppendBool,
                    driver.sql_router_sdk.kTypeInt16: builder.AppendInt16,
                    driver.sql_router_sdk.kTypeInt32: builder.AppendInt32,
                    driver.sql_router_sdk.kTypeInt64: builder.AppendInt64,
                    driver.sql_router_sdk.kTypeFloat: builder.AppendFloat,
                    driver.sql_router_sdk.kTypeDouble: builder.AppendDouble,
                    driver.sql_router_sdk.kTypeString: builder.AppendString,
                    driver.sql_router_sdk.kTypeDate: builder.AppendDate,
                    driver.sql_router_sdk.kTypeTimestamp: builder.AppendTimestamp
                    }
                for i in range(len(holdIdxs)):
                    if parameters[i] == None:
                        builder.AppendNULL()
                        continue
                    idx = holdIdxs[i]
                    colType = schema.GetColumnType(idx)
                    ok = appendMap[colType](parameters[i])
                    if not ok:
                        raise DatabaseError("erred at append data seq {}".format(i));
                ok, error = self.connection._sdk.executeInsert(self.db, command, builder)
                if not ok:
                    raise DatabaseError(err)
            else:
                ok, error = self.connection._sdk.executeInsert(self.db, command)
                if not ok:
                    raise DatabaseError(err)
        else:
            pass

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

    @connected
    def fetchone(self):
        return "call fetchone"

    @connected
    def fetchmany(self, size=None):
        print("call fetchmany")

    def nextset(self):
        pass

    def setinputsizes(self, size):
        pass

    def setoutputsize(self, size, columns=()):
        pass
        
    @connected
    def fetchall(self):
        print("call fetchall")

    @connected
    def get_query_metadata(self):
        print("call get query metadata")

    def get_default_plugin(self):
        print("call get default plugin")

    def __iter__(self):
        print("call __iter__")

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
            raise Exception("init fedb sdk erred")
        self._sdk = sdk

    def connected(func):
        def func_wrapper(self, *args, **kwargs):
            if self._connected is False:
                raise ConnectionClosedException("Connection object is closed")
            else:
                func(self, *args, **kwargs)

        return func_wrapper


    def execute(self):
        print("conn execute")

    def close(self):
        pass

    def cursor(self):
        print("call cursor")
        return Cursor(self._db, self._zk, self._zkPath, self)

    def __del__(self):
        print("connection delete")

    @connected
    def _cursor_execute(self, cursor, statement, parameters):
        print("call _cursor_execute")
        print(cursor.__class__)

    @connected
    def do_rollback(self, dbapi_connection):
        pass

    @connected
    def rollback(self):
        pass

    def commit(self):
        """
        fedb doesn't suppport transactions
        
        So just do nothing to support this method
        """
        pass

def connect(db, zk, zkPath):
    print("only call connect*********")
    return Connection(db, zk, zkPath)
