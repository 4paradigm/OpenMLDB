# tools/autotest/fesql_param.py
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
import argparse
import sys

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

    parser.add_argument("--table_num", default="[1,3]", help="""
                        表的个数""")

    parser.add_argument("--join_expr_num", default="[1,3]", help="""
                        表的个数""")

    parser.add_argument("--sql_type", default="[0,3]", help="""
                        sql的类型: 0 表示单表，1 表示多表union，2 表示lastjoin，3 表示子查询""")

    parser.add_argument("--sub_query_type", default="[1,2]", help="""
                        sql的类型: 0 表示单表单窗口，1 表示多表union，2 表示lastjoin，3 表示简单子查询""")

    parser.add_argument("--join_expr_op", default="=,>,>=,<,<=", help="""
                        sql的类型: 0 表示单表，1 表示多表union""")

    parser.add_argument("--window_type", default="[0,1]", help="""
                        sql的类型: 0 表示普通窗口，1 表示union窗口""")

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
    parser.add_argument("--table_normal_num", default="[5,10]",
                        help="生成表时的普通列的个数")
    parser.add_argument("--window_num", default="[1,3]",
                        help="生成窗口的个数")
    parser.add_argument("--table_pk_num", default="[1,3]",
                        help="生成表时的pk数")
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
    return beg, end

def sample_string_config(spec:str):
    items = spec.split(",")
    items = [_.strip() for _ in items if _.strip() != ""]
    res = random.choice(items)
    if res == "0":
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
