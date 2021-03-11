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

import os
TESTLIB_DIR = os.path.dirname(os.path.abspath(__file__))

RTIDB_ROOT_DIR = os.path.abspath(os.path.join(TESTLIB_DIR, '../../..'))
BIN_DIR = os.path.join(RTIDB_ROOT_DIR, 'build/bin')
RTIDB_CLIENT_PY = os.path.join(RTIDB_ROOT_DIR, 'python/rtidb_client.py')
RUNENV_DIR = os.path.join(RTIDB_ROOT_DIR, 'test-output/functiontest/runenv')
RTIDB_LOG = os.path.join(RUNENV_DIR, 'rtidb.log')

ENDPOINT = '0.0.0.0:5555'
CMD_BUILD = 'cd %(RTIDB_ROOT_DIR)s; sh build.sh' % locals()
CMD_BUILD_JAVA_CLIENT = 'cd %(RTIDB_ROOT_DIR)s;   sh build_java_client.sh' % locals()
CMD_BUILD_PYTHON_CLIENT = 'cd %(RTIDB_ROOT_DIR)s; sh build_python_client.sh' % locals()
CMD_TABLE_START = ' cd %(BIN_DIR)s ; ./rtidb --log_level=debug --endpoint=%(ENDPOINT)s --role=tablet > %(RTIDB_LOG)s  2>&1 &' % locals()
CMD_TABLE_STOP =  " ps -ef |grep '%(ENDPOINT)s' |grep -v grep |awk '{print $2}' |xargs kill -9 " % locals()
CMD_TABLE_PS =  " ps -ef |grep '%(ENDPOINT)s' |grep -v grep " % locals()

ATTRS_MAP = locals()
