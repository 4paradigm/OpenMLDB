#! /usr/bin/env python
# -*- coding: utf-8 -*-

import math
import util.tools as tool


def check_rows(actual: list, expect: list):
    assert len(actual) == len(expect)
    for index, value in enumerate(actual):
        expectValue = expect[index]
        check_list(value, expectValue)


def check_list(actual: list, expect: list):
    assert len(actual) == len(expect), 'actual:{},expect:{}'.format(len(actual), len(expect))
    for index, value in enumerate(actual):
        expectValue = expect[index]
        # print(str(value))
        if str(value) == 'nan':
            assert str(expectValue) == 'nan'
        elif str(value) == '-inf':
            assert str(expectValue) == '-inf'
        elif str(value) == 'inf':
            assert str(expectValue) == 'inf'
        elif type(value) == float and type(expectValue) == float:
            assert tool.equalsFloat(value, expectValue), 'actual:{},expect:{},actual-expect={}'.format(value, expectValue, (value - expectValue))
        else:
            assert value == expectValue, 'actual:{},expect:{}'.format(value,
                                                                      expectValue) + ";actual_type:{},expect_type:{}".format(
                type(actual), type(expect))
