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

from absl import flags
import os
import logging

log = logging.getLogger(__name__)

FLAGS = flags.FLAGS
flags.DEFINE_string('dist_conf', '', 'the path of yaml conf')
flags.DEFINE_string('data_dir', '/tmp/diagnose_tool_data', 'the dir of data')
flags.DEFINE_string('check', 'ALL', 'the item should be check. one of ALL/CONF/LOG/SQL/VERSION')
flags.DEFINE_string('exclude', '', 'one of CONF/LOG/SQL/VERSION')
flags.DEFINE_string('env', '', 'startup environment. set onebox if started with start-all.sh')
flags.DEFINE_string('log_level', 'INFO', 'the level of log')
flags.DEFINE_bool('sdk_log', False, 'print cxx sdk log, default is False. Only support zk log now')

LOG_FORMAT = '%(levelname)s: %(message)s'

class ConfOption:
    def __init__(self):
        self.all_items = ['ALL', 'CONF', 'LOG', 'SQL', 'VERSION']
        self.check_items = []

    def set_log(self):
        if self.log_dir != '':
            logfile = os.path.join(self.log_dir, 'log.txt')
            handler = logging.FileHandler(logfile, mode='w')
        else:
            handler = logging.StreamHandler()
        root_logger = logging.getLogger()
        for h in root_logger.handlers:
            root_logger.removeHandler(h)
        logging.basicConfig(level=self.log_level, format=LOG_FORMAT, handlers=[handler])

    def init(self) -> bool:
        self.log_dir = FLAGS.log_dir
        if self.log_dir != '' and not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir)
        log_map = {'debug': logging.DEBUG, 'info': logging.INFO, 'warn': logging.WARN}
        log_level = FLAGS.log_level.lower()
        if log_level not in log_map:
            print(f'invalid log_level {FLAGS.log_level}. log_level should be info/warn/debug')
            return False
        self.log_level = log_map[log_level]
        self.set_log()
        if FLAGS.dist_conf == '':
            log.warn('dist_conf option should be setted')
            return False
        if not os.path.exists(FLAGS.dist_conf):
            log.warn(f'{FLAGS.dist_conf} is not exist')
            return False
        self.dist_conf = FLAGS.dist_conf
        self.data_dir = FLAGS.data_dir
        check = FLAGS.check.upper()
        if check not in self.all_items:
            log.warn('the value of check should be ALL/CONF/LOG/SQL/VERSION')
            return False
        exclude = FLAGS.exclude.upper()
        if exclude != '' and exclude not in self.all_items[1:]:
            log.warn('the value of exclude should be CONF/LOG/SQL/VERSION')
            return False
        if check != 'ALL' and exclude != '':
            log.warn('cannot set exclude if the value of check is not \'ALL\'')
            return False
        if check == 'ALL':
            self.check_items = self.all_items[1:]
        else:
            self.check_items.append(check)
        if exclude != '':
            self.check_items = list(filter(lambda x : x != exclude, self.check_items))
        self.env = FLAGS.env;


        return True

    def check_version(self) -> bool:
        return 'VERSION' in self.check_items

    def check_conf(self) -> bool:
        return 'CONF' in self.check_items

    def check_log(self) -> bool:
        return 'LOG' in self.check_items

    def check_sql(self) -> bool:
        return 'SQL' in self.check_items

    def print_sdk_log(self) -> bool:
        return FLAGS.sdk_log
