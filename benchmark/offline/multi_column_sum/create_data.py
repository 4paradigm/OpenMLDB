#!/usr/bin/env python3
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

import sys
import argparse
import time
import random
from pyspark.sql import SparkSession


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, required=True, help="Approximate final output rows")
    parser.add_argument("--cols", type=int, required=True, help="Final output columns")
    parser.add_argument("--ids", type=int, required=True, help="Primary key number")
    parser.add_argument("--skew", action="store_true", help="Whether generate skewed data")
    parser.add_argument("--output", type=str, required=True, help="Output data path")
    parser.add_argument("--time", type=str, default="2018/01/01,2019/01/01", help="Time range")
    parser.add_argument("--master", type=str, default="local", help="Spark master")
    return parser.parse_args(sys.argv[1:])


def to_time(s):
    return time.mktime(time.strptime(s, "%Y/%m/%d"))


def main(args):
    (time_min, time_max) = map(to_time, args.time.split(","))

    if args.skew:
        id_prob = [1.0 / (x + 1) for x in range(args.ids)]
        id_prob_sum = sum(id_prob)
        id_count = [int(args.rows * x / id_prob_sum) for x in id_prob]
    else:
        id_count = int(args.rows / args.ids)

    def make_rows(idx):
        if isinstance(id_count, int):
            num = id_count
        else:
            num = int(id_count[idx])
        for _ in range(0, num):
            result = [idx, random.randint(time_min, time_max) * 1000]
            for _ in range(args.cols):
                result.append(random.randint(0, 1000))
            yield result

    spark = SparkSession.builder.master(args.master).getOrCreate()
    sc = spark.sparkContext
    rdd = sc.parallelize(range(args.ids)).repartition(max(2, args.ids / 100))
    rows = rdd.flatMap(lambda i: make_rows(i))

    schema = ["id", "time"]
    for k in range(args.cols):
        schema.append("c" + str(k))

    df = spark.createDataFrame(rows, schema).repartition(max(1, args.rows / 1000000))
    df.write.mode("overwrite").parquet(args.output)


if __name__ == "__main__":
    main(parse_args())

