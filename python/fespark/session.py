
import os
from .dataframe import FesqlDataframe
from .dataframe_reader import DataframeReader
from .gateway import GlobalGateway

class SessionBuilder(object):
    # Notice that this is used to be compatible with pyspark session and the options are not used yet
    _options = {}

    def config(self, key=None, value=None):
        self._options[key] = str(value)
        return self

    def master(self, master):
        return self.config("spark.master", master)

    def appName(self, name):
        return self.config("spark.app.name", name)

    def enableHiveSupport(self):
        return self.config("spark.sql.catalogImplementation", "hive")

    def getOrCreate(self):
        return FesqlSession()

class FesqlSession(object):
    # Use to be compatible with PySpark API
    builder = SessionBuilder()

    def __init__(self, pysparkSession=None):
        self.jsession = None
        self.pysparkSession = None

        # Init global gateway so that we can access fesql jvm objects
        GlobalGateway.init()

        # Use the given pysaprk session or create the new one which will load fesql jar
        if pysparkSession is None:
            self.pysparkSession = GlobalGateway.get().getOrCreatePysparkSession()
        else:
            self.pysparkSession = pysparkSession

        # Create scala FesqlSession object
        self.jsession = GlobalGateway.get().fesql_api.FesqlSession(self.pysparkSession._jsparkSession)

    @property
    def read(self):
        return DataframeReader(self.jsession)

    @property
    def sparkContext(self):
        return self.pysparkSession.sparkContext

    def fesql(self, sqlText):
        return FesqlDataframe(self.jsession.fesql(sqlText))

    def sql(self, sqlText):
        return FesqlDataframe(self.jsession.sql(sqlText))

    def sparksql(self, sqlText):
        return FesqlDataframe(self.jsession.sparksql(sqlText))

    def version(self):
        self.jsession.version()
    
    def registerTable(self, name, jsparkdf):
        self.jsession.registerTable(name, jsparkdf)

    def __str__(self):
        return self.jsession.toString()

    def getSparkSession(self):
        return self.jsession.getSparkSession()

    def createDataFrame(self, data, schema=None, samplingRatio=None, verifySchema=True):
        return FesqlDataframe(self.pysparkSession.createDataFrame(data, schema, samplingRatio, verifySchema)._jdf)
