#!/usr/bin/env python
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

from pyspark.sql import SparkSession
import os


def main():
    spark = SparkSession.builder.appName("sparksql_app").getOrCreate()

    current_path = os.getcwd()
    parquet_file_path = "file://{}/../data/taxi_tour_all/".format(current_path)

    train = spark.read.parquet(parquet_file_path)
    train.createOrReplaceTempView("t1")

    with open("compare_project.sql", "r") as f:
        sparksqlText = f.read()

    spark_df = spark.sql(sparksqlText)

    output_path = "file:///tmp/pyspark_output/"
    spark_df.write.mode('overwrite').parquet(output_path)

    spark.stop()


if __name__ == "__main__":
    main()
