#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# gateway.py
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


import os
import sys
import logging
from py4j import java_gateway
from py4j.java_gateway import JavaGateway, CallbackServerParameters
from pyspark.sql import SparkSession
from .exception import FesqlException

logging.basicConfig(
    format='%(asctime)s [%(levelname)s] %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

"""
The global gateway to call JVM methods. 
"""
class GlobalGateway(object):
    _global_gatway = None

    @staticmethod
    def init():
        if GlobalGateway._global_gatway is None:
            GlobalGateway._global_gatway = FesqlGateway()

    @staticmethod
    def get():
        return GlobalGateway._global_gatway

"""
The gateway to call JVM methods with py4j.
"""
class FesqlGateway(object):
    FESQL_PACKAGE_NAME = "com._4paradigm.fesql.offline.api"
    FESQL_HOME_DIR = os.environ.get("FESQL_HOME", os.path.abspath("./"))
    FESQL_JAR_NAME = os.environ.get("FESQL_JAR_FILE", "fesql-spark-0.0.1-SNAPSHOT-with-dependencies.jar")
    FESQL_JAR_PATH = os.path.join(FESQL_HOME_DIR, FESQL_JAR_NAME)

    def load_package(self, name):
        parts = [_.strip() for _ in name.split(".") if _.strip() != ""]
        cur = self.java_gateway.jvm
        for p in parts:
            cur = getattr(cur, p)
        return cur

    def __init__(self):
        # Use pyspark api to getOrCreate pyspark session which can be used for local run and spakr-submit
        self.java_gateway = FesqlGateway.getOrCreatePysparkSession()._sc._gateway

        # Load fesql jvm classes
        self.fesql_api = self.load_package(FesqlGateway.FESQL_PACKAGE_NAME)

    """
    This method will create 
    """
    @staticmethod
    def getOrCreatePysparkSession():
        if "PYSPARK_GATEWAY_PORT" in os.environ: # Run with spark-submit
            return SparkSession.builder.getOrCreate()
        else: # Run with local script
            if not os.path.isfile(FesqlGateway.FESQL_JAR_PATH):
                raise FesqlException("Fail to load fesql jar in {}".format(FesqlGateway.FESQL_JAR_PATH))

            logging.info("Load the fesql jar in {}".format(FesqlGateway.FESQL_JAR_PATH))
            return SparkSession.builder.config("spark.jars", FesqlGateway.FESQL_JAR_PATH).getOrCreate()
