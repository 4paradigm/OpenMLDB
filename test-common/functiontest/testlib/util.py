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

# -*- coding:utf-8 -*-
import sys
import os
from common_utils.process import Process
from common_utils.file_util import FileUtils
import common_utils.jsonpath as jsonpath
import setting as main_conf
sys.dont_write_bytecode = True


def get(key):
    """

    :param key:
    :param attrMap:
    :return:
    """
    mainMap = main_conf.ATTRS_MAP
    value = mainMap[key] % mainMap
    return value


def start_tablet_server(workDir):
    """

    :param workDir:
    :return:
    """
    binDir = get('BIN_DIR')
    logfile = os.path.join(workDir, 'run.log')
    cmd = 'cd %(binDir)s && ./rtidb --role=tablet >%(logfile)s 2>&1 &' % locals()
    p = Process()
    data, error, retCode = p.run(cmd)
    if 0 != retCode:
        raise Exception('Start tablet server error: %s' % FileUtils.read(logfile))


def inspect_get(data, type):
    """

    :param data:
    :param type:
    :return:
    """
    if 'dictionary' == type:
        return data[1][1]
    elif 'class' == type:
        className = str(data[1][0].f_locals["self"].__class__)
        className = className.split('.')[-1]
        className = className.replace("'>", "")
        return className
    elif 'function' == type:
        return data[1][3]
    else:
        raise Exception("Invalid inspect type:%(data)s, %(type)s" % locals())

