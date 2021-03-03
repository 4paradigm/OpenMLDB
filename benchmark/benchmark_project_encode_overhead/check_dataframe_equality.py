#!/usr/bin/env python
# -*- coding: utf-8 -*-

# check_dataframe_equality.py
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

from pyspark.sql import SparkSession
import os


def main():
    spark = SparkSession.builder.appName("sparksql_app").getOrCreate()

    parquet_path1 = "file:///tmp/test1/"
    parquet_path2 = "file:///tmp/pyspark_output/"
    current_path = os.getcwd()
    parquet_path1 = "file://{}/../data/taxi_tour_all/".format(current_path)
    parquet_path2 = "file://{}/../data/taxi_tour_all/".format(current_path)

    parquet1 = spark.read.parquet(parquet_path1)
    parquet2 = spark.read.parquet(parquet_path2)

    except1 = parquet1.subtract(parquet2).count()
    except2 = parquet2.subtract(parquet1).count()

    if except1 == except2: 
        print("DataFrames are equal")
    else:
        print("DataFrames are not equal, except1: {}, except2: {}".format(except1, except2))
        
    spark.stop()

if __name__ == "__main__":
    main()
