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

# -*- coding: utf-8 -*-
import unittest
from libs.logger import infoLogger
import libs.conf as conf
import time


def multi_dimension(md_open):
    if md_open == conf.multidimension:
        return lambda func: func
    else:
        return unittest.skip('multi_dimension closed, case skipped.')


def data_provider(data_list):
    def p(f):
        def fn(arg_self):
            for data in data_list:
                if isinstance(data, list):
                    try:
                        f(arg_self, *data)
                    except Exception, e:
                        infoLogger.error('{}({}) failed: {}'.format(f.__name__, data, e))
                        print '{}({}) failed: {}'.format(f.__name__, data, e)
                        infoLogger.info(dir(f))
                else:
                    infoLogger.error('data_list type must be a list')
        return fn
    return p


def perf(f):
    def fn(*args, **kw):
        start = time.time()
        f(*args, **kw)
        escape = 1000 * (time.time() - start)
        infoLogger.info('method execute time: {} {}.'.format(escape, 'ms'))
    return fn
