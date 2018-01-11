# -*- coding: utf-8 -*-
from testcasebase import TestCaseBase
import time
from libs.test_loader import load
import libs.utils as utils
from libs.logger import infoLogger
from libs.deco import multi_dimension


class TestAddReplicaNs(TestCaseBase):

    @multi_dimension(False)
    def test_addreplica_normal(self):
        rs = self.showopstatus(self.ns_leader)
        old_last_op_id = max(rs.keys()) if rs != {} else 1
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        name = 'tname{}'.format(int(time.time() * 1000000 % 10000000000))
        m = utils.gen_table_metadata(
            '"{}"'.format(name), 144000, 2,
            ('"{}"'.format(self.leader), '"1-3"', 'true'))
        utils.gen_table_metadata_file(m, metadata_path)
        rs = self.run_client(self.ns_leader, 'create ' + metadata_path, 'ns_client')
        self.assertTrue('Create table ok' in rs)

        pid = 1
        rs3 = self.makesnapshot(self.ns_leader, name, pid, 'ns_client')
        self.assertTrue('MakeSnapshot ok' in rs3)
        time.sleep(2)
        rs1 = self.showtablet(self.ns_leader)
        infoLogger.info(rs1)
        rs2 = self.addreplica(self.ns_leader, name, pid, 'ns_client', self.slave1)
        self.assertTrue('AddReplica ok' in rs2)
        infoLogger.info(self.showopstatus(self.ns_leader))
        last_op_id = max(self.showopstatus(self.ns_leader).keys())
        self.assertTrue(old_last_op_id != last_op_id)
        last_opstatus = self.showopstatus(self.ns_leader)[last_op_id]
        self.assertTrue('kAddReplicaOP', last_opstatus)

        rs4 = self.showtable(self.ns_leader)
        tid = rs4[name][0]
        rs2 = self.put(self.leader,
                       tid,
                       pid,
                       'testkey0',
                       self.now(),
                       'testvalue0')
        self.assertTrue('Put ok' in rs2)
        time.sleep(20)
        self.assertTrue(
            'testvalue0' in self.scan(self.slave1, tid, pid, 'testkey0', self.now(), 1))


if __name__ == "__main__":
    load(TestAddReplicaNs)
