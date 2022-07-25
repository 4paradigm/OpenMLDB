import logging
import os.path
import sys
import unittest

from diagnostic_tool.collector import Collector
from diagnostic_tool.dist_conf import DistConfReader


class MyTestCase(unittest.TestCase):
    def mock_path(self, servers, path_conf_name='path'):
        if type(servers) is not list:
            servers = [servers]
        for server in servers:
            server[path_conf_name] = self.current_path + server[path_conf_name]

    def setUp(self) -> None:
        self.current_path = os.path.dirname(__file__)
        dist_conf = DistConfReader(self.current_path + '/cluster_dist.yml').conf()
        # for test
        self.mock_path(dist_conf.nameservers)
        self.mock_path(dist_conf.tabletservers)
        self.mock_path(dist_conf.taskmanagers)
        # zk log path is missing
        self.conns = Collector(dist_conf)

    def test_pull_config(self):
        self.assertTrue(self.conns.pull_config_files('/tmp/conf_copy_to'))

    def test_pull_logs(self):
        self.assertTrue(self.conns.pull_log_files('/tmp/log_copy_to'))


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stderr, level=logging.INFO,
                        format='[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s', )
    unittest.main()
