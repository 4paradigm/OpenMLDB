
import os
from .dataframe import FesqlDataframe
from .dataframe_reader import DataframeReader
from .backend import GlobalBackend

class SessionBuilder(object):
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
    # Compatible with PySpark API
    builder = SessionBuilder()

    def __init__(self, pySparkSession=None):
        self.jsession = None

        # Init existing JVM for spark-submit and start new JVM for local run
        GlobalBackend.init()

        if "PYSPARK_GATEWAY_PORT" not in os.environ: # Run with spark-submit
            from pyspark.sql import SparkSession
            pySparkSession = SparkSession.builder.getOrCreate()
            self.jsession = GlobalBackend.get().fesql_api.FesqlSession(pySparkSession._jsparkSession)
        else: # Run with local script
            # Always create new SparkSession with Scala API, notice that do \
            # not use pySparkSession._jsparkSession which is in another JVM
            self.jsession = GlobalBackend.get().fesql_api.FesqlSession()

    @property
    def read(self):
        return DataframeReader(self.jsession)

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
