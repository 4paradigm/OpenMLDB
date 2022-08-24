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

#import yaml

from common import fedb_config
from common.fedb_client import FedbClient
from util import tools

rootPath = tools.getRootPath()
from nb_log import LogManager

log = LogManager('python-sdk-test').get_logger_and_add_handlers()


class FedbTest:

    def setup_class(self):
        self.client = FedbClient(fedb_config.zk_cluster, fedb_config.zk_root_path, fedb_config.default_db_name)
        self.connect = self.client.getConnect()
        # try:
        #     self.connect.execute("create database {};".format(fedb_config.default_db_name))
        #     log.info("create db:" + fedb_config.default_db_name + ",success")
        # except Exception as e:
        #     log.info("create db:" + fedb_config.default_db_name + ",failed . msg:"+str(e))

if __name__ == "__main__":
    f = FedbTest()
    f.setup_class()
