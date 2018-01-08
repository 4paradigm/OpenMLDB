# -*- coding: utf-8 -*-
import time
from testcasebase import TestCaseBase
from libs.deco import *
from libs.test_loader import load
import libs.utils as utils
from libs.logger import infoLogger


class TestShowTablet(TestCaseBase):

    def test_showtablet_healthy(self):
        """
        健康的节点，状态为kTabletHealthy
        :return:
        """
        name = 't{}'.format(int(time.time() * 1000000 % 10000000000))
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        m = utils.gen_table_metadata(
            '"{}"'.format(name), 144000, 8,
            ('"{}"'.format(self.leader), '"1-3"', 'true'),
            ('"{}"'.format(self.slave1), '"1-2"', 'false'),
            ('"{}"'.format(self.slave2), '"2-3"', 'false'))
        utils.gen_table_metadata_file(m, metadata_path)
        rs = self.run_client(self.ns_leader, 'create ' + metadata_path, 'ns_client')
        self.assertTrue('Create table ok' in rs)
        rs1 = self.showtablet(self.ns_leader)
        infoLogger.info(rs1)
        self.assertTrue(rs1[self.leader][0] == 'kTabletHealthy')
        self.assertTrue(rs1[self.slave1][0] == 'kTabletHealthy')
        self.assertTrue(rs1[self.slave2][0] == 'kTabletHealthy')


    def test_showtablet_offline(self):
        """
        挂掉的节点，状态为kTabletOffline
        :return:
        """
        self.start_client(self.slave1path)
        name = 't{}'.format(int(time.time() * 1000000 % 10000000000))
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        m = utils.gen_table_metadata(
            '"{}"'.format(name), 144000, 8,
            ('"{}"'.format(self.leader), '"1-3"', 'true'),
            ('"{}"'.format(self.slave1), '"1-2"', 'false'),
            ('"{}"'.format(self.slave2), '"2-3"', 'false'))
        utils.gen_table_metadata_file(m, metadata_path)
        rs = self.run_client(self.ns_leader, 'create ' + metadata_path, 'ns_client')
        self.assertTrue('Create table ok' in rs)
        self.stop_client(self.slave1)
        time.sleep(10)
        rs1 = self.showtablet(self.ns_leader)
        infoLogger.info(rs1)
        self.assertTrue(rs1[self.leader][0] == 'kTabletHealthy')
        self.assertTrue(rs1[self.slave1][0] == 'kTabletOffline')
        self.assertTrue(rs1[self.slave2][0] == 'kTabletHealthy')
        self.start_client(self.slave1path)


if __name__ == "__main__":
    load(TestShowTablet)
