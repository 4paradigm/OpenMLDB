# -*- coding: utf-8 -*-
import unittest
from testcasebase import TestCaseBase
import time
import threading
import xmlrunner
from libs.test_loader import load
from libs.logger import infoLogger
import libs.utils as utils


class TestMakeSnapshotNsClient(TestCaseBase):

    def test_makesnapshot_normal_success(self):
        """

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

        rs3 = self.makesnapshot(self.ns_leader, name, 1, 'ns_client')
        self.assertTrue('MakeSnapshot ok' in rs3)

        mf = self.get_manifest(self.leaderpath, self.tid, self.pid)
        self.assertEqual(mf['offset'], '10000')
        self.assertTrue(mf['name'])
        self.assertEqual(mf['count'], '10000')


if __name__ == "__main__":
    import libs.test_loader
    load(TestMakeSnapshotNsClient)
