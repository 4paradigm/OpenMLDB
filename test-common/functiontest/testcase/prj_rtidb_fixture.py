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
import atest.log as log
from framework.fixture_base import ProjectFixtureBase
from common_utils.file_util import FileUtils
from common_utils.process import Process
import util as util

class RtidbFixture(ProjectFixtureBase):
    """
    Main class of functiontest environment.
    """
    def setUp(self):
        """
        environment setup
        :return:
        """
        #build && start tablet
        FileUtils.mkdir(util.get('RUNENV_DIR'))
        FileUtils.rm(util.get('RTIDB_LOG'))
        for cmd in ['CMD_BUILD',
                    'CMD_BUILD_JAVA_CLIENT',
                    'CMD_BUILD_PYTHON_CLIENT',
                    'CMD_TABLE_STOP',
                    'CMD_TABLE_START']:
            log.info(cmd)
            cmd = util.get(cmd)
            proc = Process()
            proc.run(cmd)

    def tearDown(self):
        """
        environment clear
        :return:
        """
        cmd = util.get('CMD_TABLE_STOP')
        proc = Process()
        proc.run(cmd)

