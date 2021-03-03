#!/usr/bin/python
import os
# benchmark/offline/multi_column_sum/run_sparksql.py
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

import json
import time
import sys
import argparse
from pyspark.sql import SparkSession


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", action="append", help="Input table specs")
    parser.add_argument("-o", "--output", type=str, help="Output data path")
    parser.add_argument("-s", "--sql", type=str, required=True, help="SQL string or path")
    parser.add_argument("-c", "--conf", type=str, action="append", help="Spark configurations")
    parser.add_argument("--master", type=str, default="local", help="Spark master mode")
    return parser.parse_args(sys.argv[1:])


def load_feql(path):
    with open(path) as input_f:
        return input_f.read()


def main(args):
    builder = SparkSession.builder.master(args.master)

    if args.conf is None:
        args.conf = []
    for conf in args.conf:
        parts = conf.split("=")
        if len(parts) == 2:
            key, value = parts[0], parts[1]
            if key.startswith("spark."):
                builder = builderr.config(key, value)
    spark = builder.getOrCreate()

    if args.input is None:
        args.input = []
    for s in args.input:
        name, path = s[0: s.find("=")], s[s.find("=") + 1:]
        if not (path.startswith("hdfs://") or path.startswith("file://")):
            path = "file://" + os.path.abspath(path)
        print("Find input table %s: %s" % (name, path))
        df = spark.read.parquet(path)
        df.registerTempTable(name)

    sql = args.sql
    if os.path.exists(sql):
        with open(sql) as sql_file:
            sql = sql_file.read()
    print("Test SQL script:")
    print(sql)

    output_df = spark.sql(sql)

    st = time.time()
    if args.output is None:
        count = output_df._jdf.queryExecution().toRdd().count()
        print("Output count: %d" % count)
    else:
        output_path = args.output
        print("Write result to: %s" % output_path)
        output_df.write.format("parquet").mode("overwrite").save(output_path)
    et = time.time()
    print("Time cost: %d seconds" % (et - st))


if __name__ == "__main__":
    main(parse_args())

