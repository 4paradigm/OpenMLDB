import logging
import os.path
import sys
import unittest

from diagnostic_tool.collector import Collector
from diagnostic_tool.dist_conf import DistConfReader, ServerInfoMap, ALL_SERVER_ROLES, ServerInfo

logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format='{%(filename)s:%(lineno)d} %(levelname)s - %(message)s', )


class MyTestCase(unittest.TestCase):
    def mock_path(self):
        self.conns.dist_conf.server_info_map = ServerInfoMap(
            self.conns.dist_conf.map(ALL_SERVER_ROLES,
                                     lambda role, s: ServerInfo(role, s['endpoint'],
                                                                self.current_path + '/' + s['path'])))

    def setUp(self) -> None:
        self.current_path = os.path.dirname(__file__)
        dist_conf = DistConfReader(self.current_path + '/cluster_dist.yml').conf()
        # zk log path is missing
        self.conns = Collector(dist_conf)

        # for test
        self.mock_path()

    def test_ping(self):
        self.assertTrue(self.conns.ping_all())

    def test_pull_config(self):
        self.assertTrue(self.conns.pull_config_files('/tmp/conf_copy_to'))

    def test_pull_logs(self):
        # no logs in tablet1
        with self.assertLogs() as cm:
            self.assertFalse(self.conns.pull_log_files('/tmp/log_copy_to'))
        self.assertTrue(any(['no logs in' in log_str for log_str in cm.output]))

    @unittest.skip
    def test_version(self):
        self.conns.collect_version()


if __name__ == '__main__':
    unittest.main()
