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

from fesql_sql import sample_window_project
from fesql_table import ColumnsPool
from fesql_window import sample_window_def
from gen_sample_data import gen_simple_data
from fesql_param import sample_integer_config

from fesql_sql import sample_window_union_project

from fesql_const import PRIMITIVE_TYPES
from fesql_sql import sample_last_join_project, sample_subselect_project
from gen_sample_data import gen_pk_groups

import random


def gen_single_window_test(test_name, udf_pool, args):
    # collect input columns required by window project
    input_columns = ColumnsPool(args)
    input_columns.set_unique_id("id", "int64")
    input_columns.name = "{0}"

    # sample window def
    window_defs = []
    window_num = sample_integer_config(args.window_num);
    for i in range(window_num):
        window_def = sample_window_def("w"+str(i), args)
        window_defs.append(window_def)
    window_query = sample_window_project(
        mainTable=input_columns,
        window_defs=window_defs, udf_defs=udf_pool,
        args=args, downward=True, keep_index=True)

    sql_string = window_query.text.strip() + ";"

    all_columns = input_columns.get_all_columns()
    data = gen_simple_data(all_columns, args, window_defs,
                           index_column=input_columns.id_column,
                           partition_columns=input_columns.partition_columns,
                           order_columns=input_columns.order_columns)
    # 组装 yml
    input_table = {
        "columns": ["%s %s" % (c.name, c.dtype) for c in all_columns],
        "indexs": ["index1:%s:%s" % (
            index.partition_column,
            index.order_column) for index in input_columns.indexs],
        "rows": data
    }
    sql_case = {
        "id": 0,
        "desc": "single_window_test_" + test_name,
        "sql": sql_string,
        "expect": {"success": True, "order": "id"},
        "inputs": [input_table]
    }
    yaml_obj = {
        "db": "test_db",
        "cases": [sql_case]
    }
    return yaml_obj

def gen_window_union_test(test_name, udf_pool, args):
    # collect input columns required by window project
    table_num = sample_integer_config(args.table_num)
    tables = []
    for i in range(table_num):
        table = ColumnsPool(args)
        table.set_unique_id("id", "int64")
        table.name = "{%d}" % i
        tables.append(table)
    # sample window def
    window_defs = []
    window_num = sample_integer_config(args.window_num)
    for i in range(window_num):
        window_def = sample_window_def("w"+str(i), args)
        if table_num <=1:
            window_def.window_type = 0
        else:
            window_def.window_type = sample_integer_config(args.window_type)
        if window_def.window_type == 1:
            for i in range(1, table_num):
                window_def.tables.add(tables[i].name)
        window_defs.append(window_def)
    window_query = sample_window_union_project(
        tables, window_defs=window_defs, udf_defs=udf_pool,
        args=args, downward=True, keep_index=True)
    sql_string = window_query.text.strip() + ";"
    # 组装 yml
    input_tables = []
    for table in tables:
        all_columns = table.get_all_columns()
        data = gen_simple_data(all_columns, args, window_defs,
                           index_column=table.id_column,
                           partition_columns=table.partition_columns,
                           order_columns=table.order_columns)
        input_table = {
            "columns": ["%s %s" % (c.name, c.dtype) for c in all_columns],
            "indexs": ["index1:%s:%s" % (
                index.partition_column,
                index.order_column) for index in table.indexs],
            "rows": data
        }
        input_tables.append(input_table)
    sql_case = {
        "id": 0,
        "desc": "single_window_test_" + test_name,
        "sql": sql_string,
        "expect": {"success": True, "order": "id"},
        "inputs": input_tables
    }
    yaml_obj = {
        "db": "test_db",
        "cases": [sql_case]
    }
    return yaml_obj

def gen_window_lastjoin_test(test_name, udf_pool, args):
    # collect input columns required by window project
    table_num = sample_integer_config(args.table_num)
    if table_num <= 1:
        table_num=2
    tables = []
    for i in range(table_num):
        table = ColumnsPool(args)
        table.set_unique_id("id", "int64")
        table.name = "{%d}" % i
        tables.append(table)
    # sample window def
    window_defs = []
    window_num = sample_integer_config(args.window_num)
    for i in range(window_num):
        window_def = sample_window_def("w"+str(i), args)
        # window_def.window_type = sample_integer_config(args.window_type)
        # if window_def.window_type == 1:
        #     for i in range(1, table_num):
        #         window_def.tables.add(tables[i].name)
        window_defs.append(window_def)
    window_query = sample_last_join_project(
        tables, window_defs=window_defs, udf_defs=udf_pool,
        args=args, downward=True, keep_index=True)
    sql_string = window_query.text.strip() + ";"
    # 组装 yml
    input_tables = []
    pk_groups = gen_pk_groups(args, tables[0].partition_columns)
    for table in tables:
        all_columns = table.get_all_columns()
        data = gen_simple_data(all_columns, args, window_defs,
                               index_column=table.id_column,
                               partition_columns=table.partition_columns,
                               order_columns=table.order_columns,
                               pk_groups=pk_groups)
        input_table = {
            "columns": ["%s %s" % (c.name, c.dtype) for c in all_columns],
            "indexs": ["index%d:%s:%s" % (
                i,
                index.partition_column,
                index.order_column) for i,index in enumerate(table.indexs)],
            "rows": data
        }
        input_tables.append(input_table)
    sql_case = {
        "id": 0,
        "desc": "single_window_test_" + test_name,
        "sql": sql_string,
        "expect": {"success": True, "order": "id"},
        "inputs": input_tables
    }
    yaml_obj = {
        "db": "test_db",
        "cases": [sql_case]
    }
    return yaml_obj

def gen_window_subselect_test(test_name, udf_pool, args):
    # collect input columns required by window project
    table_num = sample_integer_config(args.table_num)
    # table_num = 2
    tables = []
    for i in range(table_num):
        table = ColumnsPool(args)
        table.set_unique_id("id", "int64")
        table.name = "{%d}" % i
        # table_pk_num = sample_integer_config(args.table_pk_num)
        # for j in range(table_pk_num):
        #     pk_column = table.sample_partition_column(
        #         downward=True, nullable=args.index_nullable)
        #     table.partition_columns.append(pk_column)
        # table_ts_num = sample_integer_config(args.table_ts_num)
        # for j in range(table_ts_num):
        #     order_column = table.sample_order_column(
        #         downward=True, nullable=args.index_nullable)
        #     table.order_columns.append(order_column)
        # table_normal_num = sample_integer_config(args.table_normal_num)
        # for j in range(table_normal_num):
        #     expect_dtype = random.choice(PRIMITIVE_TYPES)
        #     normal_column = table.sample_column(
        #         downward=True, dtype=expect_dtype,
        #         nullable=None, allow_const=True)
        #     table.normal_columns.append(normal_column)
        tables.append(table)
    # sample window def
    window_defs = []
    window_num = sample_integer_config(args.window_num)
    for i in range(window_num):
        window_def = sample_window_def("w"+str(i), args)
        # window_def.window_type = sample_integer_config(args.window_type)
        # if window_def.window_type == 1:
        #     for i in range(1, table_num):
        #         window_def.tables.add(tables[i].name)
        window_defs.append(window_def)
    window_query = sample_subselect_project(
        tables, window_defs=window_defs, udf_defs=udf_pool,
        args=args, downward=True, keep_index=True)
    sql_string = window_query.text.strip() + ";"
    # 组装 yml
    input_tables = []
    pk_groups = gen_pk_groups(args, tables[0].partition_columns)
    for table in tables:
        # if len(table.partition_columns) ==0:
        #     continue
        all_columns = table.get_all_columns()
        data = gen_simple_data(all_columns, args, window_defs,
                               index_column=table.id_column,
                               partition_columns=table.partition_columns,
                               order_columns=table.order_columns,
                               pk_groups=pk_groups)
        input_table = {
            "columns": ["%s %s" % (c.name, c.dtype) for c in all_columns],
            "indexs": ["index%d:%s:%s" % (
                i,
                index.partition_column,
                index.order_column) for i,index in enumerate(table.indexs)],
            "rows": data
        }
        input_tables.append(input_table)
    sql_case = {
        "id": 0,
        "desc": "single_window_test_" + test_name,
        "sql": sql_string,
        "expect": {"success": True, "order": "id"},
        "inputs": input_tables
    }
    yaml_obj = {
        "db": "test_db",
        "cases": [sql_case]
    }
    return yaml_obj


