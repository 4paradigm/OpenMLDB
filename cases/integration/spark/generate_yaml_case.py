#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright 2021 4Paradigm
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# -*- coding: utf-8 -*-

# pip3 install -U ruamel.yaml pyspark first
import argparse
from datetime import date
import random
import string
import time
import sys

import pyspark
import pyspark.sql
from pyspark.sql.types import *
import ruamel.yaml as yaml
from ruamel.yaml import RoundTripDumper, RoundTripLoader

from ruamel.yaml.scalarstring import LiteralScalarString, DoubleQuotedScalarString

YAML_TEST_TEMPLATE = """
db: test_db
cases:
  - id: 1
    desc: yaml 测试用例模版
    inputs: []
    sql: |
        select * from t1
    expect:
      success: true
"""

INPUT_TEMPLATE = """
        columns: []
        indexs: []
        rows: []
"""


def random_string(prefix, n):
    return "{}_{}".format(prefix, ''.join(random.choices(string.ascii_letters + string.digits, k=n)))

# random date in current year
def random_date():
    start_dt = date.today().replace(day=1, month=1).toordinal()
    end_dt = date.today().toordinal()
    random_day = date.fromordinal(random.randint(start_dt, end_dt))
    return random_day

def to_column_str(field):
    tp = '{unknown_type}'
    if isinstance(field.dataType, BooleanType):
        tp = 'bool'
    elif isinstance(field.dataType, ShortType):
        tp = 'int16'
    elif isinstance(field.dataType, IntegerType):
        tp = 'int32'
    elif isinstance(field.dataType, LongType):
        tp = 'int64'
    elif isinstance(field.dataType, StringType):
        tp = 'string'
    elif isinstance(field.dataType, TimestampType):
        tp = 'timestamp'
    elif isinstance(field.dataType, DateType):
        tp = 'date'
    elif isinstance(field.dataType, DoubleType):
        tp = 'double'
    elif isinstance(field.dataType, FloatType):
        tp = 'float'

    return "%s %s" % (field.name, tp)

def random_row(schema):
    row = []
    for field_schema in schema.fields:
        field_type = field_schema.dataType
        if isinstance(field_type, BooleanType):
            row.append(random.choice([True, False]))
        elif isinstance(field_type, ShortType):
            row.append(random.randint(- (1 << 15), 1 << 15 - 1))
        elif isinstance(field_type, IntegerType):
            row.append(random.randint(- (1 << 31), 1 << 31 - 1))
        elif isinstance(field_type, LongType):
            row.append(random.randint(-(1 << 63), 1 << 63 - 1))
        elif isinstance(field_type, StringType):
            row.append(random_string(field_schema.name, 10))
        elif isinstance(field_type, TimestampType):
            # in milliseconds
            row.append(int(time.time()) * 1000)
        elif isinstance(field_type, DateType):
            row.append(random_date())
        elif isinstance(field_type, DoubleType):
            row.append(random.uniform(-128.0, 128.0))
        elif isinstance(field_type, FloatType):
            row.append(random.uniform(-128.0, 128.0))
        else:
            row.append('{unknown}')

    return row


def to_string(value):
    if isinstance(value, date):
        return DoubleQuotedScalarString(value.strftime("%Y-%m-%d"))
    if isinstance(value, float):
        return float("%.2f" % value)
    if isinstance(value, str):
        return DoubleQuotedScalarString(value)
    return value


sess = None
def gen_inputs_column_and_rows(parquet_file, table_name=''):
    global sess
    if sess is None:
        sess = pyspark.sql.SparkSession(pyspark.SparkContext())
    dataframe = sess.read.parquet(parquet_file)
    hdfs_schema = dataframe.schema
    schema = [DoubleQuotedScalarString(to_column_str(f)) for f in hdfs_schema.fields]

    table = yaml.load(INPUT_TEMPLATE, Loader=RoundTripLoader)

    if table_name:
        table['name'] = table_name

    table['columns'] = schema

    data_set = []
    row_cnt = random.randint(1, 10)
    for _ in range(row_cnt):
        data_set.append(random_row(hdfs_schema))

    table['rows'] = [list(map(to_string, row)) for row in data_set]
    return table


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--sql", required=True, help="sql text path")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--schema-file", help="path to hdfs content(in parquet format), used to detect table schema")
    group.add_argument("--schema-list-file", help="list file conataining a list of hdfs files, \"table_name: file path\" per line")
    parser.add_argument("--output", required=True, help="path to output yaml file")
    args = parser.parse_args()

    sql = args.sql
    schema_file = args.schema_file
    schema_list_file = args.schema_list_file
    output = args.output

    yaml_test = yaml.load(YAML_TEST_TEMPLATE, Loader=RoundTripLoader, preserve_quotes=True)

    if schema_file:
        tb = gen_inputs_column_and_rows(schema_file)
        yaml_test['cases'][0]['inputs'].append(tb)
    elif schema_list_file:
        with open(schema_list_file, 'r') as l:
            for schema_file in l:
                sf = schema_file.strip()
                if not sf:
                    continue
                table_name, parquet_file, *_ = sf.split(':')

                parquet_file = parquet_file.strip()
                if parquet_file:
                    tb = gen_inputs_column_and_rows(parquet_file, table_name)
                    yaml_test['cases'][0]['inputs'].append(tb)
    else:
        print("error")
        sys.exit(1)


    with open(sql, 'r') as f:
        yaml_test['cases'][0]['sql'] = LiteralScalarString(f.read().strip())

    with open(output, 'w') as f:
        f.write(yaml.dump(yaml_test, Dumper=RoundTripDumper, allow_unicode=True))

