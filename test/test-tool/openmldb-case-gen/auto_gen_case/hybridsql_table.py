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

import random
import numpy as np

from hybridsql_const import PRIMITIVE_TYPES, VALID_PARTITION_TYPES, VALID_ORDER_TYPES, BUILTIN_OP_DICT, SQL_PRESERVED_NAMES

from gen_const_data import random_literal_bool, random_literal_int32, random_literal_int64, \
    random_literal_float, random_literal_double, random_literal_string, random_literal_int16, \
    random_literal_date, random_literal_timestamp

from hybridsql_param import sample_integer_config

from hybridsql_const import LAST_JOIN_SQL, LAST_JOIN_OP
from hybridsql_param import sample_string_config


class ColumnInfo:
    def __init__(self, name, dtype, nullable=True):
        '''
        ColumnInfo 初始化方法
        :param name: 列名
        :param dtype: 列类型
        :param nullable: 是否为null，默认可以为null
        '''
        self.name = name
        self.dtype = dtype
        self.nullable = nullable

class ColumnKey:
    def __init__(self,partition_column,order_column):
        self.partition_column = partition_column
        self.order_column = order_column

    def __hash__(self) -> int:
        return hash(self.partition_column+":"+self.order_column)

    def __eq__(self, other) -> bool:
        if isinstance(other, ColumnKey):
            return ((self.partition_column == other.partition_column) and (self.order_column == other.order_column))
        else:
            return False

class ColumnsPool:
    def __init__(self, args):
        self.args = args
        self.name = None
        self.id_column = None
        self.order_columns = []
        self.partition_columns = []
        self.normal_columns = []
        self.indexs = set()
        self.expressions = []
        self.output_columns = []
        self.join_table = []
        self.join_column = []

    def get_select_all_sql(self):
        return "(SELECT * FROM {}) AS {}".format(self.name, self.name)

    def get_select_all_column_sql(self):
        return "(SELECT {} FROM {}) AS {}".format(",".join([c.name for c in self.get_all_columns()]), self.name, self.name)

    def get_select_column_sql(self, columns:list):
        return "(SELECT {} FROM {}) AS {}".format(",".join([c.name for c in columns]), self.name, self.name)

    def get_select_sql_by_all_type(self, expect_types:list):
        all_columns = self.get_all_columns()
        return "(SELECT {} FROM {}) AS {}".format(",".join([c.name for c in all_columns if c.dtype in expect_types]), self.name, self.name)

    def get_select_sql_by_type(self, expect_types:list):
        all_columns = self.get_all_columns()
        columns = []
        for c in all_columns:
            if c.dtype in expect_types:
                columns.append(c)
                expect_types.remove(c.dtype)
        return "(SELECT {} FROM {}) AS {}".format(",".join([c.name for c in columns]), self.name, self.name)

    def get_sub_sql(self):
        key = random.randint(1, 2)
        select_all_sqls = {
            0: self.name,
            1: self.get_select_all_sql(),
            2: self.get_select_all_column_sql()
        }
        return select_all_sqls[key]

    def get_select_sql_by_type_and_index(self, expect_types:list):
        columns = [c for c in self.normal_columns if c.dtype in expect_types]
        res = []
        if self.id_column is not None:
            res.append(self.id_column)
        res.extend(self.partition_columns)
        res.extend(self.order_columns)
        res.extend(columns)
        return "SELECT {} FROM {}".format(",".join([c.name for c in res]), self.name)

    def get_join_def_string(self, is_sub_select=False):
        # LAST_JOIN_SQL = "LAST JOIN ${TABLE_NAME} ORDER BY ${ORDER_COLUMN} ON ${JOIN_EXPR}"
        join_expr_op = sample_string_config(self.args.join_expr_op)
        join_sqls = []
        for table in self.join_table:
            sql_string = LAST_JOIN_SQL
            if is_sub_select:
                sql_string = sql_string.replace("${TABLE_NAME}", table.get_sub_sql())
            else:
                sql_string = sql_string.replace("${TABLE_NAME}", table.name)
            order_name = random.choice(table.order_columns).name
            sql_string = sql_string.replace("${ORDER_COLUMN}", table.name+"."+order_name)
            join_expr_num = sample_integer_config(self.args.join_expr_num)
            on_exprs = []
            for i in range(join_expr_num):
                join_pk = random.choice(self.partition_columns).name
                if i == 0:
                    op = "="
                    self.indexs.add(ColumnKey(join_pk, order_name))
                    table.indexs.add(ColumnKey(join_pk, order_name))
                else:
                    op = random.choice(join_expr_op)
                on_expr = self.name+"."+join_pk+" "+op+" "+table.name+"."+join_pk
                on_exprs.append(on_expr)
            sql_string = sql_string.replace("${JOIN_EXPR}", " and ".join(on_exprs))
            join_sqls.append(sql_string)
        return " ".join(join_sqls)

    def get_all_columns(self):
        '''
        获取所有列
        :return: ColumnInfo的list
        '''
        res = []
        if self.id_column is not None:
            res.append(self.id_column)
        res.extend(self.partition_columns)
        res.extend(self.order_columns)
        res.extend(self.normal_columns)
        return res

    def set_unique_id(self, name, dtype):
        '''
        设置索引列
        :param name: "id"
        :param dtype: "int64"
        :return: 无返回
        '''
        self.id_column = ColumnInfo(name, dtype, nullable=False)

    def add_order_column(self, name, dtype, nullable=False):
        '''
        增加排序列
        :param name: 列名
        :param dtype: 类型
        :param nullable: 是否为null，默认不为空
        :return:
        '''
        column = ColumnInfo(name, dtype, nullable=nullable)
        self.order_columns.append(column)
        return column

    def add_partition_column(self, name, dtype, nullable=False):
        column = ColumnInfo(name, dtype, nullable=nullable)
        self.partition_columns.append(column)
        return column

    @staticmethod
    def sample_index(p):
        '''
        获取落在某一个概率中的 索引位置
        :param p:
        :return:
        '''
        weight = sum(p)
        p = [_ / weight for _ in p]
        samples = np.random.multinomial(1, p)
        return list(samples).index(1)

    @staticmethod
    def do_create_new_column(prefix, cands, dtype, nullable):
        '''
        创建一个新的列
        :param prefix: 前缀
        :param cands: 列的list
        :param dtype: 列类型
        :param nullable: 是否为null
        :return:
        '''
        #如果类型为空就从pk类型中选择一个
        if dtype is None:
            dtype = random.choice(PRIMITIVE_TYPES)
        #如果是 类型的list 就从list中选择一个
        elif isinstance(dtype, list) or isinstance(dtype, set):
            dtype = random.choice(dtype)
        #如果nullable 不填，默认为true
        if nullable is None:
            nullable = True
        #生成列名
        name = prefix + "_" + str(len(cands)) + "_" + str(dtype)
        column = ColumnInfo(name, dtype=dtype, nullable=nullable)
        # 生成的列添加到集合中
        cands.append(column)
        return column

    def do_sample_column(self, prefix, column_list,
                         downward=True,
                         dtype=None,
                         nullable=None,
                         allow_const=False,
                         prob_use_existing=None,
                         prob_use_new=None,
                         prob_use_constant=None):
        '''
        生成一个列样本
        :param prefix:
        :param column_list:
        :param downward:
        :param dtype:
        :param nullable:
        :param allow_const:
        :return:
        '''
        # probabilities for random generate leaf expression
        if prob_use_existing is None:
            prob_use_existing = self.args.prob_sample_exist_column
        if prob_use_new is None:
            prob_use_new = self.args.prob_sample_new_column
        if prob_use_constant is None:
            prob_use_constant = self.args.prob_sample_const_column
        probs = [prob_use_existing]
        if downward:
            probs.append(prob_use_new)

        # some data types can not be literal const
        if allow_const and dtype not in ["int16", "date", "timestamp"]:
            probs.append(prob_use_constant)

        idx = self.sample_index(probs)
        #idx==0 表示 是prob_use_existing
        if idx == 0:
            def is_compatible_column(c):
                '''
                判断采样出的列是否满足nullable和数据类型约束
                :param c:
                :return:
                '''
                if nullable is not None and c.nullable != nullable:
                    return False
                elif dtype is not None:
                    if isinstance(dtype, list) or isinstance(dtype, set):
                        if c.dtype not in dtype:
                            return False
                    elif c.dtype != dtype:
                        return False
                return True

            candidates = list(filter(is_compatible_column, column_list))
            #如果candidates为0，则创建一个列
            if len(candidates) == 0:
                if downward:
                    return self.do_create_new_column(prefix, column_list, dtype, nullable)
                else:
                    return gen_literal_const(dtype, nullable=nullable)
                    # raise Exception("Candidates is empty, can not create new column in upward mode")
            else:
                return random.choice(candidates)
        elif idx == 1 and downward:
            return self.do_create_new_column(prefix, column_list, dtype, nullable)
        else:
            # 返回的是一个常量
            return gen_literal_const(dtype, nullable=False)

    def sample_partition_column(self, downward=True, nullable=False, new_pk=True):
        '''
        pk样本
        :param downward:
        :param nullable:
        :return:
        '''
        if new_pk:
            return self.do_sample_column("pk", self.partition_columns,
                                         downward=downward,
                                         allow_const=False,
                                         dtype=VALID_PARTITION_TYPES,
                                         nullable=nullable,
                                         prob_use_existing=0,
                                         prob_use_new=1,
                                         prob_use_constant=0)
        else:
            return self.do_sample_column("pk", self.partition_columns,
                                         downward=downward,
                                         allow_const=False,
                                         dtype=VALID_PARTITION_TYPES,
                                         nullable=nullable,
                                         prob_use_existing=1,
                                         prob_use_new=0,
                                         prob_use_constant=0)

    def sample_order_column(self, downward=True, nullable=False):
        '''
        order样本
        :param downward:
        :param nullable:
        :return:
        '''
        ts_type = sample_integer_config(self.args.ts_type)
        order_type = VALID_ORDER_TYPES[ts_type]
        return self.do_sample_column("order", self.order_columns,
                                     downward=downward,
                                     allow_const=False,
                                     dtype=order_type,
                                     nullable=nullable)

    def sample_column(self, downward=True, dtype=None, nullable=None, allow_const=False):
        '''
        普通列样本
        :param downward:
        :param dtype:
        :param nullable:
        :param allow_const:
        :return:
        '''
        return self.do_sample_column("c", self.normal_columns,
                                     downward=downward,
                                     allow_const=allow_const,
                                     dtype=dtype,
                                     nullable=nullable)
    def init_table(self, args, window_defs, udf_defs, downward=True, keep_index=True, new_pk = True):
        # sample expressions
        expr_num = sample_integer_config(args.expr_num)
        expr_depth = sample_integer_config(args.expr_depth)
        table_pk_num = sample_integer_config(args.table_pk_num)
        table_ts_num = sample_integer_config(args.table_ts_num)

        output_names = []
        pk_columns = []
        order_columns = []
        if downward:
            if len(self.partition_columns) > 0:
                pk_columns = self.partition_columns
            else:
                for i in range(table_pk_num):
                    pk_column = self.sample_partition_column(
                        downward=downward, nullable=args.index_nullable)
                    pk_columns.append(pk_column)
            if len(self.order_columns)>0:
                order_columns = self.order_columns
            else:
                for i in range(table_ts_num):
                    order_column = self.sample_order_column(
                        downward=downward, nullable=args.index_nullable)
                    if order_column not in order_columns:
                        order_columns.append(order_column)
        else:
            pk_columns = self.partition_columns
            order_columns = self.order_columns
        if keep_index:
            # unique idx
            index_column = self.id_column
            if index_column is not None:
                self.expressions.append(TypedExpr(index_column.name, index_column.dtype))
                output_names.append(index_column.name)

            # partition
            for pk_column in pk_columns:
                self.expressions.append(TypedExpr(pk_column.name, pk_column.dtype))
                output_names.append(pk_column.name)

            # order
            for order_column in order_columns:
                self.expressions.append(TypedExpr(order_column.name, order_column.dtype))
                output_names.append(order_column.name)
        if downward:
            for window_def in window_defs:
                window_order = random.choice(order_columns)
                window_def.order_column.add(window_order.name)
                window_pk_num = random.randint(1,table_pk_num)
                for i in range(window_pk_num):
                    window_pk = random.choice(pk_columns)
                    window_def.pk_columns.add(window_pk.name)
                    self.indexs.add(ColumnKey(window_pk.name, window_order.name))
        else:
            for window_def in window_defs:
                select_index = random.choice(list(self.indexs))
                window_def.order_column.add(select_index.order_column)
                window_def.pk_columns.add(select_index.partition_column)
                window_pk_num = random.randint(1, len(self.partition_columns))
                for _ in range(1, window_pk_num):
                    window_pk = random.choice(pk_columns)
                    window_def.pk_columns.add(window_pk.name)

        for i in range(expr_num):
            window_def = random.choice(window_defs)
            alias_name = None
            #生成别名
            if args.use_alias_name:
                alias_name = window_def.name + "_out_" + str(i)
            #生成一个新的表达式
            new_expr = sample_expr(udf_defs, self,
                                   is_udaf=True,
                                   over_window=window_def.name,
                                   alias_name=alias_name,
                                   allow_const=False,
                                   depth=expr_depth,
                                   downward=downward)
            if alias_name is not None:
                output_names.append(alias_name)
            else:
                output_names.append(new_expr.text)
            self.expressions.append(new_expr)
        # output schema
        out_length = 1+len(pk_columns)+len(order_columns)+expr_num
        for i in range(out_length):
            self.output_columns.append(ColumnInfo(output_names[i], self.expressions[i].dtype))

    def init_join_table(self, args, window_defs, udf_defs, downward=True, keep_index=True):
        # sample expressions
        expr_num = sample_integer_config(args.expr_num)
        expr_depth = sample_integer_config(args.expr_depth)
        table_pk_num = sample_integer_config(args.table_pk_num)
        table_ts_num = sample_integer_config(args.table_ts_num)

        output_names = []
        all_expressions = []
        pk_columns = []
        order_columns = []
        if downward:
            if len(self.partition_columns) > 0:
                pk_columns = self.partition_columns
            else:
                for i in range(table_pk_num):
                    pk_column = self.sample_partition_column(
                        downward=downward, nullable=args.index_nullable)
                    pk_columns.append(pk_column)
            if len(self.order_columns)>0:
                order_columns = self.order_columns
            else:
                for i in range(table_ts_num):
                    order_column = self.sample_order_column(
                        downward=downward, nullable=args.index_nullable)
                    order_columns.append(order_column)
        else:
            pk_columns = self.partition_columns
            order_columns = self.order_columns
        join_tables = self.join_table
        tables = [self]
        tables.extend(join_tables)
        if keep_index:
            # unique idx
            index_column = self.id_column
            if index_column is not None:
                self.expressions.append(TypedExpr(self.name+"."+index_column.name, index_column.dtype))
                all_expressions.append(TypedExpr(self.name+"."+index_column.name, index_column.dtype))
                output_names.append(index_column.name)

            # partition
            for pk_column in pk_columns:
                pk_expr_name = random.choice(tables).name+"."+pk_column.name
                self.expressions.append(TypedExpr(pk_expr_name, pk_column.dtype))
                all_expressions.append(TypedExpr(pk_expr_name, pk_column.dtype))
                output_names.append(pk_column.name)

            # order
            for order_column in order_columns:
                order_expr_name = random.choice(tables).name+"."+order_column.name
                self.expressions.append(TypedExpr(order_expr_name, order_column.dtype))
                all_expressions.append(TypedExpr(order_expr_name, order_column.dtype))
                output_names.append(order_column.name)
        if downward:
            for window_def in window_defs:
                window_order = random.choice(order_columns)
                window_def.order_column.add(window_order.name)
                window_pk_num = random.randint(1, table_pk_num)
                for i in range(window_pk_num):
                    window_pk = random.choice(pk_columns)
                    window_def.pk_columns.add(window_pk.name)
                    self.indexs.add(ColumnKey(window_pk.name, window_order.name))
        else:
            for window_def in window_defs:
                select_index = random.choice(list(self.indexs))
                window_def.order_column.add(select_index.order_column)
                window_def.pk_columns.add(select_index.partition_column)
                window_pk_num = random.randint(1, len(self.partition_columns))
                for _ in range(1, window_pk_num):
                    window_pk = random.choice(pk_columns)
                    window_def.pk_columns.add(window_pk.name)
        for join_table in join_tables:
            join_table.partition_columns = self.partition_columns
            join_table.order_columns = self.order_columns
            join_table.indexs = self.indexs
        for i in range(expr_num):
            window_def = random.choice(window_defs)
            alias_name = None
            #生成别名
            if args.use_alias_name:
                alias_name = window_def.name + "_out_" + str(i)
            #生成一个新的表达式
            table = random.choice(tables)
            new_expr = sample_expr(udf_defs, table,
                                   is_udaf=True,
                                   over_window=window_def.name,
                                   alias_name=alias_name,
                                   allow_const=False,
                                   depth=expr_depth,
                                   downward=downward)
            if alias_name is not None:
                output_names.append(alias_name)
            else:
                output_names.append(new_expr.text)
            table.expressions.append(new_expr)
            all_expressions.append(new_expr)
        # output schema
        out_length = 1+len(pk_columns)+len(order_columns)+expr_num
        for i in range(out_length):
            self.output_columns.append(ColumnInfo(output_names[i], all_expressions[i].dtype))

class SubTable(ColumnsPool):
    def __init__(self, args):
        ColumnsPool.__init__(self, args)
        self.sql = None

    def get_sub_sql(self):
        return "({}) AS {}".format(self.sql, self.name)


class TypedExpr:
    def __init__(self, text, dtype):
        self.text = text
        self.dtype = dtype

def gen_literal_const(dtype, nullable):
    '''
    根据类型生成常量，表达式
    :param dtype:
    :param nullable:
    :return:
    '''
    if dtype is None:
        dtype = random.choice(PRIMITIVE_TYPES)
    if dtype == "bool":
        res = random_literal_bool(nullable)
        res = "bool({})".format(res)
    elif dtype == "int16":
        res = random_literal_int16()
    elif dtype == "int32":
        res = random_literal_int32()
    elif dtype == "int64":
        res = random_literal_int64()
    elif dtype == "float":
        res = random_literal_float()
    elif dtype == "double":
        res = random_literal_double()
    elif dtype == "date":
        res = random_literal_date()
        res = "date('{}')".format(res)
    elif dtype == "timestamp":
        res = random_literal_timestamp()
        res = "timestamp({})".format(res)
    else:
        res = random_literal_string()
    return TypedExpr(str(res), dtype)

def sample_expr(udf_pool, column_pool,
                is_udaf=None,
                expect_dtype=None,
                over_window=None,
                allow_const=True,
                downward=True,
                alias_name=None,
                depth=1):
    '''
    生成表达式样本
    :param udf_pool:
    :param column_pool:
    :param is_udaf:
    :param expect_dtype:
    :param over_window:
    :param allow_const:
    :param downward:
    :param alias_name:
    :param depth:
    :return:
    '''

    # generate leaf expression
    if depth <= 0:
        column = column_pool.sample_column(
            downward=downward, dtype=expect_dtype,
            nullable=None, allow_const=allow_const)
        if isinstance(column, ColumnInfo):
            return TypedExpr(column_pool.name+"."+column.name, column.dtype)
        else:
            return column

    # select a udf function
    udf = udf_pool.sample_function(is_udaf=is_udaf, expect_dtype=expect_dtype)
    if udf.name == 'at':
        depth = 1
    # sample child expressions
    arg_types = udf.arg_types
    arg_exprs = []
    for dtype in arg_types:
        child_is_udaf = None
        child_allow_const = allow_const
        child_depth = random.randint(0, depth - 1)

        if dtype.startswith("list_"):
            prob_find_list_expr = 0.3
            find_list_expr = random.random() < prob_find_list_expr
            if find_list_expr and child_depth > 0:
                try:
                    child = sample_expr(
                        udf_pool, column_pool, is_udaf=child_is_udaf,
                        expect_dtype=dtype, over_window=None, allow_const=False,
                        downward=downward, alias_name=None, depth=child_depth)
                    arg_exprs.append(child)
                    continue
                except ValueError:
                    pass

            # uplift primitive typed expr as list
            child_is_udaf = False
            child_allow_const = False
            dtype = dtype[5:]

        child = sample_expr(
            udf_pool, column_pool, is_udaf=child_is_udaf,
            expect_dtype=dtype, over_window=None, allow_const=child_allow_const,
            downward=downward, alias_name=None, depth=child_depth)
        arg_exprs.append(child)

    # add variadic arguments
    if udf.is_variadic:
        if udf.name == "concat_ws":  # concat_ws take at least one argument
            variadic_num = random.randint(1, 10)
        else:
            variadic_num = random.randint(0, 10)
        for i in range(variadic_num):
            child_depth = random.randint(0, depth - 1)
            arg_exprs.append(sample_expr(
                udf_pool, column_pool, is_udaf=None,
                expect_dtype="string", over_window=None, allow_const=allow_const,
                downward=downward, alias_name=None, depth=child_depth))

    # do generate
    if udf.name in BUILTIN_OP_DICT and 0 < len(arg_exprs) <= 2:
        if len(arg_exprs) == 1:
            text = "(%s %s)" % (BUILTIN_OP_DICT[udf.name],
                                arg_exprs[0].text)
        else:
            text = "(%s %s %s)" % (arg_exprs[0].text,
                                   BUILTIN_OP_DICT[udf.name],
                                   arg_exprs[1].text)
    else:
        if udf.name in SQL_PRESERVED_NAMES:
            udf.name = "`" + udf.name + '`'
        text = "%s(%s)" % (udf.name, ", ".join([_.text for _ in arg_exprs]))
    if over_window is not None:
        text += " OVER " + str(over_window)
    if alias_name is not None:
        text += " AS " + alias_name
    return TypedExpr(text, udf.return_type)
