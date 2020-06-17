
import os
import sys
import logging
from py4j import java_gateway
from py4j.java_gateway import JavaGateway, CallbackServerParameters
from .exception import FesqlException

logging.basicConfig(
    format='%(asctime)s [%(levelname)s] %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

"""
The global backend to call JVM methods. 
"""
class GlobalBackend(object):
    _backend = None

    @staticmethod
    def init():
        if GlobalBackend._backend is None:
            GlobalBackend._backend = FesqlBackend()

    @staticmethod
    def get():
        return GlobalBackend._backend
    

"""
The backend to call JVM methods with py4j.
"""
class FesqlBackend(object):
    FESQL_PACKAGE_NAME = "com._4paradigm.fesql.offline.api"
    DEFAULT_ROOT_DIR = os.environ.get("FESQL_HOME", os.path.abspath("./"))
    FESQL_JAR_PATH = os.path.join(DEFAULT_ROOT_DIR, "fesql-spark-0.0.1-SNAPSHOT-with-dependencies.jar")

    def __init__(self):
        self.java_gateway = None
        self.fesql_api = None

        self.init_py4j()

    def init_py4j(self):
        if self.java_gateway is not None:
            raise FesqlException("The gateway has been initialized")

        if "PYSPARK_GATEWAY_PORT" in os.environ: # Run with spark-submit
            logging.info("Run with spark-submit to re-use jvm gateway")
            # Get the port of running jvm from pyspark
            jvm_port = os.environ.get("PYSPARK_GATEWAY_PORT")
            logging.info("Get PySpark JVM port: {}".format(jvm_port))

            # Get the local or cluster session from pyspark environment
            from pyspark.sql import SparkSession
            pySparkSession = SparkSession.builder.getOrCreate()
            self.java_gateway = pySparkSession._sc._gateway

        else: # Run with local script
            logging.info("Run as local script to create jvm gateway")
            if "JAVA_HOME" not in os.environ:
                raise FerrariException("Make sure to set JAVA_HOME before running")
            if "SPARK_HOME" not in os.environ:
                raise FerrariException("Make sure to set SPARK_HOME before running")

            java_path = os.path.join(os.environ["JAVA_HOME"], "bin/java")
            classpath_list = [
                os.path.join(self.DEFAULT_ROOT_DIR, "conf"),
                self.FESQL_JAR_PATH,
                os.path.join(os.environ.get("HADOOP_CONF_DIR", "/etc/hadoop/conf/")),
                os.path.join(os.environ.get("YARN_CONF_DIR", "/etc/hadoop/conf/"))
            ]
            for jar_name in os.listdir(os.path.join(os.environ["SPARK_HOME"], "jars")):
                classpath_list.append(os.path.join(os.environ["SPARK_HOME"], "jars", jar_name))
            classpath = ":".join(classpath_list)
            logging.debug("Use the ferrari jar: {}".format(self.FESQL_JAR_PATH))
            logging.debug("Use the classpath: {}".format(classpath))

            # fileno() should be available to redirect stderr
            try:
                sys.stderr.fileno()
                redirect_stderr = sys.stderr
            except Exception as _:
                redirect_stderr = None

            # Start local jvm process and return port
            jvm_port = java_gateway.launch_gateway(
                classpath=classpath,
                java_path=java_path,
                redirect_stdout=sys.stdout,
                redirect_stderr=redirect_stderr,
                die_on_exit=True)
            gateway_params = java_gateway.GatewayParameters(port=jvm_port, auto_convert=True)
            self.java_gateway = java_gateway.JavaGateway(gateway_parameters=gateway_params)

        # Load fesql jvm classes
        self.fesql_api = self.load_package(self.FESQL_PACKAGE_NAME)

    def load_package(self, name):
        parts = [_.strip() for _ in name.split(".") if _.strip() != ""]
        cur = self.java_gateway.jvm
        for p in parts:
            cur = getattr(cur, p)
        return cur

