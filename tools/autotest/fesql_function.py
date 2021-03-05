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


import os
import yaml
import random
import numpy as np

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
