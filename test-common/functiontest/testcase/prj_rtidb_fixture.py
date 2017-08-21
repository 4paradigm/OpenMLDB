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

