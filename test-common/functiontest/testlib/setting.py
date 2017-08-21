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