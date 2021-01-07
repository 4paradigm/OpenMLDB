import datetime
import sys
import os
import argparse
import random
import numpy as np
import yaml
import logging
import uuid
import time
import subprocess
import multiprocessing
import traceback
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s: %(message)s')


# common process exitcode for sql test
SQL_ENGINE_SUCCESS = 0
SQL_ENGINE_CASE_ERROR = 1
SQL_ENGINE_COMPILE_ERROR = 2
SQL_ENGINE_RUN_ERROR = 3

# mapping from function name to literal op
BUILTIN_OP_DICT = {
    "add": "+",
    "minus": "-",
    "multiply": "*",
    "div": "DIV",
    "fdiv": "/",
    "mod": "%",
    "and": "AND",
    "or": "OR",
    "xor": "XOR",
    "not": "NOT",
    "eq": "=",
    "neq": "!=",
    "lt": "<",
    "le": "<=",
    "gt": ">",
    "ge": ">="
}

PRIMITIVE_TYPES = [
    "bool",
    "int16",
    "int32",
    "int64",
    "float",
    "double",
    "date",
    "timestamp",
    "string"
]

VALID_PARTITION_TYPES = [
    "int64",
    "int32",
    "string",
    "timestamp",
    "date"
]

VALID_ORDER_TYPES = [
    "int64",
    "timestamp"
]

# sql preserved names which should be wrapped in ``
SQL_PRESERVED_NAMES = {
    "string",
}

current_time = datetime.datetime.now()

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--udf_path", required=True,
                        help="UDF information yaml filepath")
    parser.add_argument("--bin_path", default="./build",
                        help="FeSQL bin path, default to ./build")
    parser.add_argument("--workers", default=1, type=int,
                        help="Num of sub-processes to run cases")

    parser.add_argument("--log_dir", default="logs",
                        help="Logging directory")

    parser.add_argument("--gen_direction", default="down",
                        help="Generate query upward or downward")

    parser.add_argument("--expr_num", default=4, help="""
                        Specify expression num in a select list,
                        can be integer, integer range or integer list""")

    parser.add_argument("--expr_depth", default=3, help="""
                        Specify expression depth in a select list,
                        can be integer, integer range or integer list""")

    parser.add_argument("--index_nullable", default="false", type=parse_bool,
                        help="Whether generate null in index columns")

    parser.add_argument("--use_alias_name", default="true", type=parse_bool,
                        help="Whether add \"AS ALIAS_NAME\" to expr")

    parser.add_argument("--rows_between", default="0,1", help="""
                        用于指定窗口的类型，1为rows，0为rows_range""")

    parser.add_argument("--rows_preceding", default=3, help="""
                        Specify window row preceding,
                        can be integer, integer range or integer list,
                        -1 denote UNBOUND and 0 denote CURRENT""")

    parser.add_argument("--rows_following", default=0, help="""
                        Specify window row following,
                        can be integer, integer range or integer list,
                        -1 denote UNBOUND and 0 denote CURRENT""")

    parser.add_argument("--rows_range_unit", default="0,s,m,h,d", help="""
                        rows_range窗口的单位，0：表示无单位""")

    parser.add_argument("--begin_time", default="0", help="""
                        生成数据ts列的时间范围的开始时间,传入毫秒级时间戳，0表示1970-01-01""")

    parser.add_argument("--end_time", default="0", help="""
                        生成数据ts列的时间范围的结束时间,传入毫秒级时间戳，0表示当前时间""")

    parser.add_argument("--gen_time_mode", default="auto", help="""
                        生成时间的模式，提供三种模式：
                        auto：根据时间窗口的大小扩展2倍生成时间，例如时间窗口从6s前到2s前，则生成从12s前到0s前的时间，
                            如果是rows窗口，则根据be生成时间
                        be：根据执行的begin和end来生成时间
                        自己指定时间范围：例如 1d，生成1天内的时间，
                            目前支持的时间范围：S 毫秒，s 秒，m 分，h 小时，d 天""")

    parser.add_argument("--auto_time_multiple", default="2", help="""
                        根据窗口自动生成time时，根据窗口大小扩展的倍数，默认为2，为1时生成的时间均在窗口内""")

    parser.add_argument("--ts_type", default="0,1", help="""
                        ts列的类型，1表示timestamp 0表示bigint 默认随机重timestamp和bigint中随机选一个""")

    parser.add_argument("--prob_sample_exist_column", default=0.4, type=float,
                        help="Probability weight to sample an existing column")
    parser.add_argument("--prob_sample_new_column", default=0.5, type=float,
                        help="Probability weight to sample a new column")
    parser.add_argument("--prob_sample_const_column", default=0.1, type=float,
                        help="Probability weight to sample a const column")
    parser.add_argument("--table_ts_num", default="[1,3]",
                        help="生成表时的ts列的个数")
    parser.add_argument("--window_pk", default="[1,3]",
                        help="生成窗口时的pk数，创建表时生成的索引数")
    parser.add_argument("--num_pk", default=5,
                        help="根据pk列生成数据时，一个pk列生成pk的数量")
    parser.add_argument("--size_of_pk", default="[2, 20]",
                        help="生成数据时每个pk生成多少条数据")

    parser.add_argument("--udf_filter", default=None,
                        help="UDF list to generate select list from")

    parser.add_argument("--udf_error_strategy", default=None, help="""
                        UDF sample strategy consider history errors,
                        can be \"drop\" or \"down_sample\", \"drop\"
                        means the udf is never sampled again when 
                        error happen,while `down_sample` will decrease
                        possibility the error udf get sampled.""")

    parser.add_argument("--show_udf_summary", default="false", type=parse_bool,
                        help="Show udf summary information")
    parser.add_argument("--max_cases", default=None, type=int,
                        help="Maximum num cases to run, default no limit")
    parser.add_argument("--yaml_count", default=10, type=int,
                        help="生成yaml的数量默认为10")
    return parser.parse_args(sys.argv[1:])


def parse_bool(arg):
    if isinstance(arg, str):
        return arg.lower() == "true"
    else:
        return bool(str)


def parse_range(range_str):
    if range_str.startswith("["):
        beg_open = False
    elif range_str.startswith("("):
        beg_open = True
    else:
        raise ValueError("Illegal range: %s" % range_str)
    if range_str.endswith("]"):
        end_open = False
    elif range_str.endswith(")"):
        end_open = True
    else:
        raise ValueError("Illegal range: %s" % range_str)
    content = [_.strip() for _ in range_str[1:-1].split(",") if _.strip() != ""]
    if len(content) != 2:
        raise ValueError("Illegal range: %s" % range_str)
    beg = int(content[0])
    if beg_open:
        beg += 1
    end = int(content[1])
    if not end_open:
        end += 1
    return int(content[0]), int(content[1])

def sample_string_config(spec:str):
    items = spec.split(",")
    items = [_.strip() for _ in items if _.strip() != ""]
    res = random.choice(items)
    if res=="0":
        res = ""
    return res

def sample_integer_config(spec):
    """
    Sample an integer from spec string, which can be:
        - An integer literal:  "9"
        - A list or integers:  "1, 3, 5"
        - Range specification: "[4, 9)"
    """
    try:
        res = int(spec)
    except ValueError:
        if spec.startswith("[") or spec.startswith("("):
            beg, end = parse_range(spec)
            res = random.randint(beg, end - 1)
        else:
            items = spec.split(",")
            items = [int(_.strip()) for _ in items if _.strip() != ""]
            res = random.choice(items)
    return res


class UDFDesc:
    def __init__(self, name, arg_types, return_type, is_variadic=False):
        self.name = name
        self.arg_types = arg_types
        self.return_type = return_type
        self.is_variadic = is_variadic
        self.unique_id = -1

    def __str__(self):
        return "%s %s(%s%s)" % (
            self.return_type, self.name,
            ", ".join(self.arg_types),
            ", ..." if self.is_variadic else "")


class UDFPool:
    '''
        UDFPool 初始化方法
        :param udf_config_file: yml文件路径
        :param args: 脚本传入的参数列表，Namespace
        '''
    def __init__(self, udf_config_file, args):
        self.args = args
        # 根据用户输入的参数 获取需要进行测试的函数名列表
        filter_names = None
        if args.udf_filter is not None:
            filter_names = frozenset(args.udf_filter.split(","))

        self.udfs = []
        self.udafs = []
        # 单行函数字典 key为函数名，value为list 是该函数名下为udf的所有函数声明
        self.udfs_dict = {}
        # 聚合函数字典 key为函数名，value为list 是该函数名下为udaf的所有函数声明
        self.udafs_dict = {}
        self.picked_functions = []
        # 函数唯一id 与 UDFDesc 对应字典
        self.udf_by_unique_id = {}
        self.history_failures = []
        self.history_successes = []

        with open(os.path.join(udf_config_file)) as yaml_file:
            udf_defs = yaml.load(yaml_file.read())
        unique_id_counter = 0
        for name in udf_defs:
            if filter_names is not None and name not in filter_names:
                continue

            items = udf_defs[name]
            for item in items:
                is_variadic = item["is_variadic"]
                for sig in item["signatures"]:
                    arg_types = sig["arg_types"]
                    return_type = sig["return_type"]
                    # row类型是内部类型，sql里不支持
                    if any([_.endswith("row") for _ in arg_types]):
                        continue

                    is_udaf = False
                    for ty in arg_types:
                        if ty.startswith("list_"):
                            is_udaf = True
                            break
                    if return_type.startswith("list_"):
                        is_udaf = False
                    desc = UDFDesc(name, arg_types, return_type, is_variadic)

                    desc.unique_id = unique_id_counter
                    unique_id_counter += 1
                    self.history_failures.append(0)
                    self.history_successes.append(0)
                    self.udf_by_unique_id[desc.unique_id] = desc

                    if is_udaf:
                        if name not in self.udafs_dict:
                            flist = []
                            self.udafs_dict[name] = flist
                            self.udafs.append(flist)
                        flist = self.udafs_dict[name]
                        flist.append(desc)
                    else:
                        if name not in self.udfs_dict:
                            flist = []
                            self.udfs_dict[name] = flist
                            self.udfs.append(flist)
                        flist = self.udfs_dict[name]
                        flist.append(desc)

    def filter_functions(self, flist, filter_func):
        cands = []
        for item in flist:
            if isinstance(item, UDFDesc):
                if filter_func(item):
                    cands.append(item)
            else:
                cands.extend(self.filter_functions(item, filter_func))
        return cands

    def sample_function(self, is_udaf=None, expect_dtype=None):
        '''
        挑选一个函数
        :param is_udaf: 是否是udaf
        :param expect_dtype: 期望的函数返回类型
        :return: UDFDesc
        '''
        # 创建一个函数 用来判断返回类型是否符合预期
        filter_func = lambda x: expect_dtype is None or x.return_type == expect_dtype
        if is_udaf:
            cands = self.filter_functions(self.udafs, filter_func)
        elif is_udaf is None:
            cands = self.filter_functions(self.udafs, filter_func) + \
                    self.filter_functions(self.udfs, filter_func)
        else:
            cands = self.filter_functions(self.udfs, filter_func)

        if len(cands) == 0:
            raise ValueError("No function found for type: " + expect_dtype)

        strategy = self.args.udf_error_strategy
        # 根据错误策略选择函数，如果某一个函数出现了错误，减少函数的概率
        if strategy is None:
            picked = random.choice(cands)
        else:
            failures = []
            successes = []
            for udf in cands:
                failures.append(self.history_failures[udf.unique_id])
                successes.append(self.history_successes[udf.unique_id])

            weights = []
            if strategy == "drop":
                for k in range(len(cands)):
                    w = (successes[k] + 1) ** 3
                    if failures[k] > 0:
                        w = 0.00001
                    else:
                        w = 100.0 / (w + (failures[k] + 1) ** 2)
                    weights.append(w)
            else:  # down_sample
                for k in range(len(cands)):
                    w = (successes[k] + 1) ** 3
                    w = 100.0 / (w + (failures[k] + 1) ** 2)
                    weights.append(w)

            total_weight = sum(weights)
            weights = [_ / total_weight for _ in weights]
            samples = list(np.random.multinomial(1, weights))
            picked = cands[samples.index(1)]

        self.picked_functions.append(picked)
        return picked


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


class ColumnsPool:
    def __init__(self, args):
        self.args = args
        self.id_column = None
        self.order_columns = []
        self.partition_columns = []
        self.normal_columns = []
    
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
                    raise Exception("Candidates is empty, can not create new column in upward mode")
            else:
                return random.choice(candidates)
        elif idx == 1 and downward:
            return self.do_create_new_column(prefix, column_list, dtype, nullable)
        else:
            # 返回的是一个常量
            return gen_literal_const(dtype, nullable=False)

    def sample_partition_column(self, downward=True, nullable=False):
        '''
        pk样本
        :param downward:
        :param nullable:
        :return:
        '''
        return self.do_sample_column("pk", self.partition_columns,
                                     downward=downward,
                                     allow_const=False,
                                     dtype=VALID_PARTITION_TYPES,
                                     nullable=nullable,
                                     prob_use_existing=0,
                                     prob_use_new=1,
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


class WindowDesc:
    '''
        定义WindowDesc，ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        :param name: 窗口的名字
        :param preceding:
        :param following:
        :param rows_between: 窗口类型 0 为RANGE 1为 ROWS
        '''
    def __init__(self, name, preceding, following, rows_between=True, rows_range_unit=""):
        self.name = name
        self.preceding = str(preceding)
        self.following = str(following)
        self.rows_between = rows_between
        self.rows_range_unit = rows_range_unit

    def get_frame_def_string(self):
        '''
        获取定义窗口的字符串，暂不支持单位
        :return: ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
                ROWS_RANGE BETWEEN
        '''
        res = ""
        if self.rows_between:
            res += "ROWS BETWEEN "
        else:
            res += "ROWS_RANGE BETWEEN "
        if self.preceding.lower().startswith("current"):
            res += "CURRENT "
            # if self.rows_between:
            res += "ROW"
        else:
            if self.rows_between:
                res += self.preceding + " PRECEDING"
            else:
                res += self.preceding + self.rows_range_unit + " PRECEDING"
        res += " AND "
        if self.following.lower().startswith("current"):
            res += "CURRENT "
            # if self.rows_between:
            res += "ROW"
        else:
            # 判断如果 following > preceding 则进行重置，随机从0-preceding 生成一个数字
            # if not self.preceding.lower().startswith("current"):
            #     preceding = int(self.preceding)
            #     following = int(self.following)
            #     if following > preceding:
            #         following = random.randint(0,preceding)
            #         self.following=str(following)

            if self.rows_between:
                res += self.following + " PRECEDING"
            else:
                res += self.following + self.rows_range_unit + " PRECEDING"
        return res


class TypedExpr:
    def __init__(self, text, dtype):
        self.text = text
        self.dtype = dtype


class QueryDesc:
    def __init__(self, text, columns):
        self.text = text
        self.columns = columns


def random_literal_bool(nullable=True):
    if nullable:
        return random.choice(["true", "false", "NULL"])
    else:
        return random.choice(["true", "false"])


def random_literal_int32():
    return random.randint(-2 ** 32, 2 ** 32 - 1)


def random_literal_int64():
    if random.randint(0, 1) == 0:
        return random.randint(2 ** 32, 2 ** 64 - 1)
    else:
        return random.randint(-2 ** 64, -2 ** 32 - 1)


def random_literal_float():
    lower = np.finfo(np.float32).min
    upper = np.finfo(np.float32).max
    return str(round(np.random.uniform(lower, upper), 5)) + "f"


def random_literal_double():
    lower = np.finfo(np.float64).min
    upper = np.finfo(np.float64).max
    return np.cast(np.random.uniform(lower, upper), np.float64)


def random_literal_string():
    strlen = random.randint(0, 128)
    lower_letters = "abcdefghijklmnopqrstuvwxyz"
    upper_letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    digits = "0123456789"
    cands = lower_letters + upper_letters + digits
    return "".join(random.choice(cands) for _ in range(strlen))


def gen_literal_const(dtype, nullable):
    '''
    根据类型生成常量
    :param dtype:
    :param nullable:
    :return:
    '''
    if dtype is None:
        dtype = random.choice(PRIMITIVE_TYPES)
    if dtype == "bool":
        res = random_literal_bool(nullable)
    elif dtype == "int16":
        raise ValueError("Can not generate literal int16")
    elif dtype == "int32":
        res = random_literal_int32()
    elif dtype == "int64":
        res = random_literal_int64()
    elif dtype == "float":
        res = random_literal_float()
    elif dtype == "double":
        res = random_literal_double()
    elif dtype == "date":
        raise ValueError("Can not generate literal date")
    elif dtype == "timestamp":
        raise ValueError("Can not generate literal timestamp")
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
            return TypedExpr(column.name, column.dtype)
        else:
            return column

    # select a udf function
    udf = udf_pool.sample_function(is_udaf=is_udaf, expect_dtype=expect_dtype)

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


def sample_bool(nullable=True):
    if nullable:
        return random.choice([True, False, None])
    else:
        return random.choice([True, False])


def sample_int16(nullable=True):
    categories = [0, -1, 1, 2**15 - 1, -2**15]
    if nullable:
        categories = categories + [None]
    idx = random.randint(0, len(categories))
    if idx < len(categories):
        return categories[idx]
    else:
        return random.randint(-2**15, 2**15 - 1)


def sample_int32(nullable=True):
    categories = [0, -1, 1, 2 ** 31 - 1, -2 ** 31]
    if nullable:
        categories = categories + [None]
    idx = random.randint(0, len(categories))
    if idx < len(categories):
        return categories[idx]
    else:
        return random.randint(-2 ** 31, 2 ** 31 - 1)


def sample_int64(nullable=True):
    categories = [0, -1, 1, 2 ** 63 - 1, -2 ** 63]
    if nullable:
        categories = categories + [None]
    idx = random.randint(0, len(categories))
    if idx < len(categories):
        return categories[idx]
    else:
        return random.randint(-2 ** 63, 2 ** 63 - 1)


def sample_float(nullable=True):
    finfo = np.finfo(np.float32)
    categories = [
        0.0, 1.0, -1.0,
        "nan", "+inf", "-inf",
        float(finfo.tiny),
        float(finfo.min),
        float(finfo.max)
    ]
    if nullable:
        categories = categories + [None]
    idx = random.randint(0, len(categories))
    if idx < len(categories):
        return categories[idx]
    else:
        lower = round(float(finfo.min), 5)
        upper = round(float(finfo.max), 5)
        return random.uniform(lower, upper)


def sample_double(nullable=True):
    finfo = np.finfo(np.float64)
    categories = [
        0.0, 1.0, -1.0,
        "nan", "+inf", "-inf",
        float(finfo.tiny),
        float(finfo.min),
        float(finfo.max)
    ]
    if nullable:
        categories = categories + [None]
    idx = random.randint(0, len(categories))
    if idx < len(categories):
        return categories[idx]
    else:
        lower = round(float(finfo.min) / 2, 15)
        upper = round(float(finfo.max) / 2, 15)
        return random.uniform(lower, upper)


def sample_date(args, nullable=True):
    res = sample_timestamp(args, nullable=nullable)
    if res is not None:
        res = time.strftime("%Y-%m-%d", time.localtime(res/1000))
    return res

def compute_pre_time(gen_time_mode:str):
    '''
    根据字符串计算当前时间之前的时间
    :param gen_time_mode:
    :return: 毫秒级时间戳，int
    '''
    time_unit = gen_time_mode[-1]
    time_num = int(gen_time_mode[0:-1])
    time_compute = {
        'S':current_time-datetime.timedelta(milliseconds=time_num),
        's':current_time-datetime.timedelta(seconds=time_num),
        'm':current_time-datetime.timedelta(minutes=time_num),
        'h':current_time-datetime.timedelta(hours=time_num),
        'd':current_time-datetime.timedelta(days=time_num)
    }
    if time_unit not in time_compute:
        raise Exception("不支持的时间单位")
    pre_time=int(time_compute[time_unit].timestamp()*1000)
    return pre_time

def gen_time_unit(gen_time_mode:str):
    begin_time=compute_pre_time(gen_time_mode)
    end_time=int(current_time.timestamp()*1000)
    gen_time = random.randint(begin_time, end_time)
    return gen_time

def gen_time_be(args):
    begin_time = int(args.begin_time)
    end_time = int(args.end_time)
    if end_time == 0:
        end_time = int(current_time.timestamp()*1000)
    gen_time = random.randint(begin_time, end_time)
    return gen_time

def gen_time_auto(w:WindowDesc,args):
    # 0 ：时间窗口
    multiple = int(args.auto_time_multiple)
    if w.rows_between == 0:
        rows_range_unit = w.rows_range_unit
        if rows_range_unit == "":
            rows_range_unit = "S"
        if w.preceding.lower().startswith("current"):
            begin_num = 0
        else:
            begin_num = int(w.preceding)*multiple
        begin_time = compute_pre_time(str(begin_num)+rows_range_unit)
        if w.following.lower().startswith("current"):
            end_num = 0
        else:
            end_num = int(w.following)//multiple
        end_time = compute_pre_time(str(end_num)+rows_range_unit)
        gen_time = random.randint(begin_time, end_time)
        return gen_time
    else:
        return gen_time_be(args)

def sample_ts_timestamp(args,w_def:WindowDesc, nullable=True):
    if nullable:
        if random.randint(0, 3) == 0:
            return None
    gen_time_mode = args.gen_time_mode.strip()
    if gen_time_mode == "auto":
        return gen_time_auto(w_def,args)
    elif gen_time_mode == "be":
        return gen_time_be(args)
    else:
        return gen_time_unit(gen_time_mode)

def sample_timestamp(args, nullable=True):
    if nullable:
        if random.randint(0, 3) == 0:
            return None
    # lower = int(time.mktime(time.strptime("2000-01-01", "%Y-%m-%d")))
    # upper = int(time.mktime(time.strptime("2020-01-01", "%Y-%m-%d")))
    # return random.randint(lower, upper)
    return gen_time_be(args)

def sample_ts_value(dtype, args, w_def:WindowDesc, nullable=True):
    if dtype == "int64":
        return sample_int64(nullable=nullable)
    else:
        return sample_ts_timestamp(args, w_def, nullable)


def sample_string(nullable=True):
    if nullable:
        if random.randint(0, 3) == 0:
            return None
    return random_literal_string()


def sample_window_def(name, args):
    '''
    得到WindowDesc
    :param name: 窗口名字
    :param args:
    :return: WindowDesc
    '''
    # 根据规则随机生成一个整数，默认值为3
    preceding = sample_integer_config(args.rows_preceding)
    if preceding == 0:
        preceding = "CURRENT"
    elif preceding == -1:
        preceding = "UNBOUNDED"
    following = sample_integer_config(args.rows_following)
    if following == 0:
        following = "CURRENT"
    elif following == -1:
        following = "UNBOUNDED"
    rows_between = sample_integer_config(args.rows_between)
    rows_range_unit = sample_string_config(args.rows_range_unit)
    return WindowDesc(name, preceding, following, rows_between=rows_between, rows_range_unit=rows_range_unit)

def sample_value(dtype, args, nullable=True):
    if dtype == "bool":
        return sample_bool(nullable=nullable)
    elif dtype == "int16":
        return sample_int16(nullable=nullable)
    elif dtype == "int32":
        return sample_int32(nullable=nullable)
    elif dtype == "int64":
        return sample_int64(nullable=nullable)
    elif dtype == "float":
        return sample_float(nullable=nullable)
    elif dtype == "double":
        return sample_double(nullable=nullable)
    elif dtype == "date":
        return sample_date(args, nullable=nullable)
    elif dtype == "timestamp":
        return sample_timestamp(args, nullable=nullable)
    elif dtype == "string":
        return sample_string(nullable=nullable)
    else:
        raise ValueError("Can not sample from dtype: " + str(dtype))


def gen_simple_data(columns, args, w_def:WindowDesc,
                    index_column=None,
                    partition_columns=None,
                    order_columns=None):
    '''
    生成样本数据
    :param columns:
    :param args:
    :param index_column:
    :param partition_columns:
    :param order_columns:
    :return:
    '''
    columns_dict = {}
    for idx, col in enumerate(columns):
        columns_dict[col.name] = idx
    column_num = len(columns)

    index_column_idx = -1
    if index_column is not None:
        index_column_idx = columns_dict[index_column.name]

    partition_idxs = []
    if partition_columns is not None:
        partition_idxs = [columns_dict[_.name] for _ in partition_columns]

    order_idxs = []
    if order_idxs is not None:
        order_idxs = [columns_dict[_.name] for _ in order_columns]

    normal_column_idxs = []
    for i in range(column_num):
        if i == index_column_idx or i in partition_idxs or i in order_idxs:
            continue
        normal_column_idxs.append(i)

    pk_groups = []
    if partition_columns is not None:
        for col in partition_columns:
            group = []
            num_pk = sample_integer_config(args.num_pk)
            for _ in range(num_pk):
                pk = sample_value(col.dtype, args, nullable=False)
                group.append(pk)
            pk_groups.append(group)

    # output rows
    rows = []

    def gen_under_pk(cur_pks):
        for _ in range(sample_integer_config(args.size_of_pk)):
            row = [None for _ in range(column_num)]
            if index_column_idx >= 0:
                row[index_column_idx] = len(rows)
            for k, pk_val in enumerate(cur_pks):
                row[partition_idxs[k]] = pk_val
            for k in order_idxs:
                order_col = columns[k]
                order_val = sample_ts_value(order_col.dtype, args, w_def, order_col.nullable)
                row[k] = order_val
            for k in normal_column_idxs:
                normal_col = columns[k]
                value = sample_value(normal_col.dtype, args, normal_col.nullable)
                row[k] = value
            rows.append(row)

    def recur_pk(pk_idx, cur_pks):
        if pk_idx == len(cur_pks):
            gen_under_pk(cur_pks)
        else:
            pk_grp = pk_groups[pk_idx]
            for pk_val in pk_grp:
                cur_pks[pk_idx] = pk_val
                recur_pk(pk_idx + 1, cur_pks)

    pks = [None for _ in range(len(partition_idxs))]
    recur_pk(0, pks)

    return rows


def sample_window_project(input_name, input_columns,
                          window_def, udf_defs, args,
                          downward=True, keep_index=True):
    # sample expressions
    expr_num = sample_integer_config(args.expr_num)
    expr_depth = sample_integer_config(args.expr_depth)
    window_pk_num = sample_integer_config(args.window_pk)
    table_ts_num = sample_integer_config(args.table_ts_num)

    # output expressions
    expressions = []
    output_names = []

    pk_columns = []
    for i in range(window_pk_num):
        pk_column = input_columns.sample_partition_column(
            downward=downward, nullable=args.index_nullable)
        pk_columns.append(pk_column)
    order_column = input_columns.sample_order_column(
        downward=downward, nullable=args.index_nullable)
    if keep_index:
        # unique idx
        index_column = input_columns.id_column
        if index_column is not None:
            expressions.append(TypedExpr(index_column.name, index_column.dtype))
            output_names.append(index_column.name)

        # partition
        for pk_column in pk_columns:
            expressions.append(TypedExpr(pk_column.name, pk_column.dtype))
            output_names.append(pk_column.name)

        # order
        expressions.append(TypedExpr(order_column.name, order_column.dtype))
        output_names.append(order_column.name)

    for i in range(expr_num):
        alias_name = None
        #生成别名
        if args.use_alias_name:
            alias_name = window_def.name + "_out_" + str(i)
        #生成一个新的表达式
        new_expr = sample_expr(udf_defs, input_columns,
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
        expressions.append(new_expr)

    sql_string = """
        SELECT ${WINDOW_EXPRS} FROM ${INPUT_NAME}
        WINDOW ${WINDOW_NAME} AS (PARTITION BY ${PK_NAME} ORDER BY ${ORDER_NAME} ${FRAME_DEF})
    """
    sql_string = sql_string.replace("${WINDOW_EXPRS}", ", ".join([_.text for _ in expressions]))
    sql_string = sql_string.replace("${INPUT_NAME}", input_name)
    sql_string = sql_string.replace("${WINDOW_NAME}", window_def.name)
    xx = ",".join(map(lambda x:x.name,pk_columns))
    sql_string = sql_string.replace("${PK_NAME}", ",".join(map(lambda x:x.name,pk_columns)))
    sql_string = sql_string.replace("${ORDER_NAME}", order_column.name)
    sql_string = sql_string.replace("${FRAME_DEF}", window_def.get_frame_def_string())

    # output schema
    output_columns = []
    for i in range(expr_num):
        output_columns.append(ColumnInfo(output_names[i], expressions[i].dtype))
    return QueryDesc(sql_string, output_columns)


def gen_single_window_test(test_name, udf_pool, args):
    # collect input columns required by window project
    input_columns = ColumnsPool(args)
    input_columns.set_unique_id("id", "int64")

    # sample window def
    window_def = sample_window_def("w", args)
    window_query = sample_window_project(
        input_name="{0}", input_columns=input_columns,
        window_def=window_def, udf_defs=udf_pool,
        args=args, downward=True, keep_index=True)

    sql_string = window_query.text.strip() + ";"

    all_columns = input_columns.get_all_columns()
    data = gen_simple_data(all_columns, args, window_def,
                           index_column=input_columns.id_column,
                           partition_columns=input_columns.partition_columns,
                           order_columns=input_columns.order_columns)
    # 组装 yml
    input_table = {
        "columns": ["%s %s" % (c.name, c.dtype) for c in all_columns],
        "indexs": ["index1:%s:%s" % (
            pk.name,
            input_columns.order_columns[0].name) for pk in input_columns.partition_columns],
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


def worker_run(udf_pool, args, shared_states):
    # extract shared states
    total_cases, total_failures, history_udf_failures, history_udf_successes = shared_states

    while True:
        try:
            with total_cases.get_lock():
                total_cases.value += 1
            udf_pool.history_failures = history_udf_failures
            udf_pool.history_successes = history_udf_successes

            test_name = str(uuid.uuid1())
            case = gen_single_window_test(test_name, udf_pool, args)
            case_dir = os.path.join(args.log_dir, test_name)
            if not os.path.exists(case_dir):
                os.makedirs(case_dir)
            with open(os.path.join(case_dir, "case.yaml"), "w") as yaml_file:
                yaml_file.write(yaml.dump(case))

            test_bin = os.path.abspath(os.path.join(args.bin_path, "src/fesql_run_engine"))
            log_file = open(os.path.join(case_dir, "case.log"), "w")
            exitcode = subprocess.call([test_bin, "--yaml_path=case.yaml"],
                                       cwd=case_dir, stdout=log_file, stderr=log_file)
            log_file.close()
            if exitcode == 0:
                # logging.info("Success: " + test_name)
                subprocess.call(["rm", "-r", case_dir])
                with history_udf_successes.get_lock():
                    for udf in udf_pool.picked_functions:
                        udf_idx = udf.unique_id
                        history_udf_successes[udf_idx] += 1
                        if history_udf_failures[udf_idx] > 2 ** 30:
                            history_udf_successes[udf_idx] = 1
                udf_pool.picked_functions = []
                continue

            # increment failure count for sampled functions
            with history_udf_failures.get_lock():
                for udf in udf_pool.picked_functions:
                    udf_idx = udf.unique_id
                    history_udf_failures[udf_idx] += 1
                    if history_udf_failures[udf_idx] > 2 ** 30:
                        history_udf_failures[udf_idx] = 1
            with total_failures.get_lock():
                total_failures.value += 1
            udf_pool.picked_functions = []

            # error handling
            if exitcode == SQL_ENGINE_CASE_ERROR:
                logging.error("Invalid case in " + test_name)
            elif exitcode == SQL_ENGINE_COMPILE_ERROR:
                logging.error("SQL compile error in " + test_name)
            elif exitcode == SQL_ENGINE_RUN_ERROR:
                logging.error("Engine run error in " + test_name)
            elif exitcode >= 128:
                logging.error("Core (signal=%d) in " % (exitcode - 128) + test_name)
            else:
                logging.error("Unknown error in " + test_name)
            with open(os.path.join(case_dir, "exitcode"), "w") as f:
                f.write(str(exitcode))

        except Exception as e:
            logging.error(e)
            traceback.print_exc()


def run(args):
    # 加载yml文件 封装成UDFDesc 并进行分类 创建了 UDFPool
    udf_pool = UDFPool(args.udf_path, args)
    logging.info("Successfully load udf information from %s", args.udf_path)

    if not os.path.exists(args.log_dir):
        os.makedirs(args.log_dir)

    total_cases = multiprocessing.Value("i", 0)
    total_failures = multiprocessing.Value("i", 0)
    history_udf_failures = multiprocessing.Array("i", udf_pool.history_failures)
    history_udf_successes = multiprocessing.Array("i", udf_pool.history_successes)
    shared_states = [total_cases, total_failures,
                     history_udf_failures, history_udf_successes]

    workers = []
    for i in range(args.workers):
        worker_process = multiprocessing.Process(
            target=worker_run,
            args=[udf_pool, args, shared_states])
        worker_process.daemon = True
        workers.append(worker_process)
    for worker in workers:
        worker.start()

    while True:
        time.sleep(5)
        logging.info("Run %d cases, get %d failures" %
                     (total_cases.value, total_failures.value))
        if args.max_cases is not None and total_cases.value > args.max_cases:
            break
        if args.show_udf_summary:
            def top_udfs(arr):
                items = []
                arr = sorted([_ for _ in enumerate(arr)], key=lambda x: -x[1])
                for udf_id, cnt in arr:
                    if cnt <= 0:
                        continue
                    items.append(str(udf_pool.udf_by_unique_id[udf_id]) + ": " + str(cnt))
                return items
            logging.info("Frequent udf in success cases: [" +
                         ", ".join(top_udfs(history_udf_successes))) + "]"
            logging.info("Frequent udf in failure cases: [" +
                         ", ".join(top_udfs(history_udf_failures))) + "]"
    logging.info("Wait workers to exit...")

def gen_case_yaml(case_dir=None):
    args = parse_args()
    udf_pool = UDFPool(args.udf_path, args)
    begin = time.time()
    case_num = args.yaml_count
    if case_dir == None:
        case_dir = args.log_dir
    if not os.path.exists(case_dir):
        os.makedirs(case_dir)
    for i in range(case_num):
        test_name = str(uuid.uuid1())
        case = gen_single_window_test(test_name, udf_pool, args)
        yamlName = "auto_gen_case_"+str(i)+".yaml"
        with open(os.path.join(case_dir, yamlName), "w") as yaml_file:
            yaml_file.write(yaml.dump(case))
    end = time.time()
    print("use time:"+str(end-begin))

def main(args):
    run(args)

if __name__ == "__main__":
    main(parse_args())
