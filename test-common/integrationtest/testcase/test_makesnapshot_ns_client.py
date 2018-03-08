# -*- coding: utf-8 -*-
from testcasebase import TestCaseBase
import time
from libs.test_loader import load
import libs.utils as utils


class TestMakeSnapshotNsClient(TestCaseBase):

    def test_makesnapshot_normal_success(self):
        """

        :return:
        """
        old_last_op_id = max(self.showopstatus(self.ns_leader).keys()) if self.showopstatus(self.ns_leader) != {} else 1
        name = 't{}'.format(int(time.time() * 1000000 % 10000000000))
        metadata_path = '{}/metadata.txt'.format(self.testpath)

        pid_group = '"{}-{}"'.format(0, 2)
        m = utils.gen_table_metadata(
            '"{}"'.format(name), 144000, 8,
            ('"{}"'.format(self.leader), pid_group, 'true'),
            ('"{}"'.format(self.slave1), pid_group, 'false'),
            ('"{}"'.format(self.slave2), pid_group, 'false'))
        utils.gen_table_metadata_file(m, metadata_path)
        rs = self.run_client(self.ns_leader, 'create ' + metadata_path, 'ns_client')
        self.assertTrue('Create table ok' in rs)

        table_info = self.showtable(self.ns_leader)
        tid = table_info[name][0]
        pid = table_info[name][1]

        rs3 = self.makesnapshot(self.ns_leader, name, pid, 'ns_client')
        self.assertTrue('MakeSnapshot ok' in rs3)
        time.sleep(2)

        mf = self.get_manifest(self.leaderpath, tid, pid)
        self.assertEqual(mf['offset'], '0')
        self.assertTrue(mf['name'])
        self.assertEqual(mf['count'], '0')
        last_op_id = max(self.showopstatus(self.ns_leader).keys())
        print old_last_op_id, last_op_id
        self.assertFalse(old_last_op_id == last_op_id)
        last_opstatus = self.showopstatus(self.ns_leader)[last_op_id]
        self.assertTrue('kAddReplicaOP', last_opstatus)


if __name__ == "__main__":
    load(TestMakeSnapshotNsClient)
