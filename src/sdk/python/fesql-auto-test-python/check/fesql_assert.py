#! /usr/bin/env python
# -*- coding: utf-8 -*-

import math

def check_rows(actual:list,expect:list):
    assert len(actual) == len(expect)
    for index,value in enumerate(actual):
        expectValue = expect[index]
        check_list(value,expectValue)

def check_list(actual:list,expect:list):
    assert len(actual) == len(expect),'actual:{},expect:{}'.format(len(actual),len(expect))
    for index,value in enumerate(actual):
        expectValue = expect[index]
        print(str(value))
        if str(value) == 'nan':
            assert str(expectValue)== 'nan'
        else:
            if(type(value) == float and type(expectValue) == float):
                value = round(value,2)
                expectValue = round(expectValue,2)
            assert value == expectValue,'actual:{},expect:{}'.format(value,expectValue)+";actual_type:{},expect_type:{}".format(type(actual),type(expect))