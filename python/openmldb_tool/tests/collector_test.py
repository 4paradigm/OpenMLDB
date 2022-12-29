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

import logging
import os.path
import unittest
from unittest.mock import patch

from diagnostic_tool.collector import Collector
from diagnostic_tool.dist_conf import DistConfReader, ServerInfoMap, ALL_SERVER_ROLES, ServerInfo

logging.basicConfig(level=logging.DEBUG,
                    format='{%(filename)s:%(lineno)d} %(levelname)s - %(message)s', )

class TestCollector(unittest.TestCase):
    def mock_path(self):
        self.conns.dist_conf.server_info_map = ServerInfoMap(
            self.conns.dist_conf.map(ALL_SERVER_ROLES,
                                     lambda role, s: ServerInfo(role, s['endpoint'],
                                                                self.current_path + '/' + s['path'], s['is_local'] if 'is_local' in s else False)))

    def setUp(self) -> None:
        self.current_path = os.path.dirname(__file__)
        dist_conf = DistConfReader(self.current_path + '/cluster_dist.yml').conf()
        # zk log path is missing
        self.conns = Collector(dist_conf)

        # for test
        self.mock_path()

    def test_ping(self):
        logging.debug('hw test')
        self.assertTrue(self.conns.ping_all())

    def test_pull_config(self):
        self.assertTrue(self.conns.pull_config_files('/tmp/conf_copy_to'))

    def test_pull_logs(self):
        # no logs in tablet1
        with self.assertLogs() as cm:
            self.assertFalse(self.conns.pull_log_files('/tmp/log_copy_to'))
        for log_str in cm.output:
            logging.info(log_str)
        self.assertTrue(any(['no file in' in log_str for log_str in cm.output]))

    @unittest.skip
    @patch('diagnostic_tool.collector.parse_config_from_properties')
    def test_version(self, mock_conf):
        mock_conf.return_value = os.path.dirname(__file__) + '/work/spark_home'
        self.assertTrue(self.conns.collect_version())


if __name__ == '__main__':

    unittest.main()
