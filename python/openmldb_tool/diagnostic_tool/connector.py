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

import sqlalchemy as db
from absl import flags
from prettytable import PrettyTable
import logging
from .util import Singleton

# most sub cmds only need cluster addr
flags.DEFINE_string(
    'cluster', '127.0.0.1:2181/openmldb', 'Cluster addr, format: <zk_endpoint>[,<zk_endpoint>]/<zkPath>.',
    short_name='c')
flags.DEFINE_bool('sdk_log', False, 'print sdk log(pysdk&zk&glog), default is False.')
flags.DEFINE_string('user', 'root', 'the username to connect OpenMLDB')
flags.DEFINE_string('password', '', 'config the password')

FLAGS = flags.FLAGS


class Connector(metaclass=Singleton):
    """OpenMLDB Python SDK wrapper, how about standalone?"""
    def __init__(self):
        assert FLAGS.cluster
        self.addr = FLAGS.cluster
        zk, zk_path = self.addr.split('/')
        url = f'openmldb:///?zk={zk}&zkPath=/{zk_path}'
        # other options
        if not FLAGS.sdk_log:
            url += '&zkLogLevel=0&glogLevel=2'
            logging.getLogger('OpenMLDB_sdk').setLevel(logging.WARNING)
        url += '&user=' + FLAGS.user
        if FLAGS.password != '':
            url += '&password=' + FLAGS.password
        self.engine = db.create_engine(url)
        self.conn = self.engine.connect()

    def address(self):
        return self.addr

    def get_conn(self):
        return self.conn

    def execute(self, sql):
        """ddl won't return resultset, can not fetchall"""
        return self.conn.execute(sql)

    def execfetch(self, sql, show=False):
        cr = self.conn.execute(sql)
        res = cr.fetchall()
        if show:
            t = PrettyTable(cr.keys())
            for row in res:
                t.add_row(row)
            print(t)
        return res
