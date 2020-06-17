
import os
from .dataframe import FesqlDataframe
from .dataframe_reader import DataframeReader
from .backend import GlobalBackend

class FesqlSession(object):

    def __init__(self, pySparkSession=None, jsession=None):
        self.jsession = None

        # Init the py4j jvm from local or spark-submit
        GlobalBackend.init()

        if pySparkSession is not None:
            self.jsession = GlobalBackend.get().fesql_api.FesqlSession(pySparkSession._jsparkSession)

        # Use the jsession if given, which may be used when Java creates the session and pass to Python
        if jsession is not None:
            self.jsession = jsession
            return

        if "PYSPARK_GATEWAY_PORT" not in os.environ: # Run with spark-submit
            from pyspark.sql import SparkSession
            pySparkSession = SparkSession.builder.getOrCreate()
            self.jsession = GlobalBackend.get().fesql_api.FesqlSession(pySparkSession._jsparkSession)
        else: # Run with local script
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