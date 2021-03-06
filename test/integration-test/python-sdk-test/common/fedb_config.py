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

import configparser
import util.tools as tool

config = configparser.ConfigParser()
confPath = tool.getAbsolutePath("conf/fedb.conf")
config.read(confPath)
lists_header = config.sections()  # 配置组名, ['test', 'mysql'] # 不含'DEFAULT'
env = config['global']['env']
default_db_name = config['global']['default_db_name']
levels = config['global']['levels'].split(",")
levels = list(map(lambda l: int(l), levels))

zk_cluster = config['fedb'][env + '_zk_cluster']
zk_root_path = config['fedb'][env + '_zk_root_path']
tb_endpoint_0 = config['fedb'][env + '_tb_endpoint_0']
tb_endpoint_1 = config['fedb'][env + '_tb_endpoint_1']
tb_endpoint_2 = config['fedb'][env + '_tb_endpoint_2']
