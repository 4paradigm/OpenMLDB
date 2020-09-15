from . import driver

paramstyle = 'qmark'

class Error(Exception):

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

    @connected
    def getdesc(self):
        return "fedb cursor"

    @connected
    def execute(self, operation, parameters=()):
        print("-----cursor execute")
        print(operation)

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
