#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# dataframe.py
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


from .dataframe_writer import DataframeWriter
from . import utils

class FesqlDataframe(object):

    def __init__(self, jdf):
        self.jdf = jdf

    def createOrReplaceTempView(self, name):
        self.jdf.createOrReplaceTempView(name)

    @property
    def write(self):
        return DataframeWriter(self.jdf)

    def run(self):
        self.jdf.run()

    def show(self):
        self.jdf.show()

    def count(self):
        return self.jdf.count()

    def sample(self, fraction, seed=None):
        if seed is None:
            return FesqlDataframe(self.jdf.sample(fraction))
        else:
            return FesqlDataframe(self.jdf.sample(fraction, seed))

    def describe(self, *cols):
        return FesqlDataframe(self.jdf.describe(cols))

    def explain(self, extended=False):
        self.jdf.explain(extended)

    def summary(self):
        return FesqlDataframe(self.jdf.summary())

    def cache(self):
        return FesqlDataframe(self.jdf.cache())

    def __str__(self):
        return self.jdf.toString()

    def schema(self):
      try:
        return utils._parse_datatype_json_string(self.jdf.schemaJson())
      except AttributeError as e:
        raise Exception("Unable to parse datatype from schema. %s" % e)

    def columns(self):
        return [f.name for f in self.schema().fields]

    def collect(self):
        from pyspark.rdd import _load_from_socket
        from pyspark.serializers import BatchedSerializer, PickleSerializer

        port = self.jdf.getSparkDf().collectToPython()
        return list(_load_from_socket(port, BatchedSerializer(PickleSerializer())))

    def toPandas(self):
        import pandas as pd
        from pyspark.sql.types import IntegralType

        pdf = pd.DataFrame.from_records(self.collect(), columns=self.columns())
        dtype = {}
        for field in self.schema():
            pandas_type = utils._to_corrected_pandas_type(field.dataType)
        if pandas_type is not None and \
                not(isinstance(field.dataType, IntegralType) and field.nullable and
                        pdf[field.name].isnull().any()):
            dtype[field.name] = pandas_type

        for f, t in dtype.items():
            pdf[f] = pdf[f].astype(t, copy=False)
        return pdf

    def toNumpy(self):
        return self.toPandas().to_numpy()

    def printCodegen(self):
        self.jdf.printCodegen()
