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


import math
import util.tools as tool

def check_rows(actual:list,expect:list):
    assert len(actual) == len(expect)
    for index,value in enumerate(actual):
        expectValue = expect[index]
        check_list(value,expectValue)

def check_list(actual:list,expect:list):
    assert len(actual) == len(expect),'actual:{},expect:{}'.format(len(actual),len(expect))
    for index,value in enumerate(actual):
        expectValue = expect[index]
        # print(str(value))
        if str(value) == 'nan':
            assert str(expectValue)== 'nan'
        elif type(value) == float and type(expectValue) == float :
            assert tool.equalsFloat(value,expectValue),'actual:{},expect:{}'.format(value,expectValue)
        else:
            assert value == expectValue,'actual:{},expect:{}'.format(value,expectValue)+";actual_type:{},expect_type:{}".format(type(actual),type(expect))
