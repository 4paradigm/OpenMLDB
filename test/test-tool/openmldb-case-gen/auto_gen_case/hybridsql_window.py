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
from hybridsql_param import sample_integer_config, sample_string_config

from hybridsql_const import WINDOW_SQL

from hybridsql_const import WINDOW_UNION_SQL


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
        self.pk_columns = set()
        self.order_column = set()
        self.tables = set()
        # 窗口类型， 0 普通窗口， 1 union窗口
        self.window_type = 0

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

    def get_window_string(self):
        flag = random.randint(0,1)
        if flag:
            return self.get_window_union_def_string()
        else:
            return self.get_window_def_string()
    def get_window_def_string(self):
        # WINDOW_SQL = "{} AS (PARTITION BY {} ORDER BY {} {})"
        return WINDOW_SQL.format(self.name,",".join(self.pk_columns),",".join(self.order_column),self.get_frame_def_string())

    def get_window_union_def_string(self):
        # {} AS (UNION {} PARTITION BY {} ORDER BY {} {} {})
        if len(self.tables) ==0:
            return self.get_window_def_string()
        instance_not_in_window = ""
        if random.randint(0,1):
            instance_not_in_window = "INSTANCE_NOT_IN_WINDOW"
        return WINDOW_UNION_SQL.format(self.name,
                                 ",".join(self.tables),
                                 ",".join(self.pk_columns),
                                 ",".join(self.order_column),
                                 self.get_frame_def_string(),
                                 instance_not_in_window)

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
