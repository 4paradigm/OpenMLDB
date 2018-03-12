# -*- coding: utf-8 -*-
from testcasebase import TestCaseBase
import time
import os
from libs.test_loader import load
import libs.utils as utils
from libs.logger import infoLogger
from libs.deco import multi_dimension
import libs.ddt as ddt
from libs.clients.ns_cluster import NsCluster
import libs.conf as conf


@ddt.ddt
class TestNameserverHa(TestCaseBase):

    def confset_createtable_put(self):
        self.confset(self.ns_leader, 'auto_failover', 'true')
        self.confset(self.ns_leader, 'auto_recover_table', 'true')
        self.tname = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        m = utils.gen_table_metadata(
            '"{}"'.format(self.tname), '"kAbsoluteTime"', 144000, 8,
            ('table_partition', '"{}"'.format(self.leader), '"0-3"', 'true'),
            ('table_partition', '"{}"'.format(self.slave1), '"0-3"', 'false'),
            ('table_partition', '"{}"'.format(self.slave2), '"2-3"', 'false'),
            ('column_desc', '"k1"', '"string"', 'true'),
            ('column_desc', '"k2"', '"string"', 'false'),
            ('column_desc', '"k3"', '"string"', 'false'))
        utils.gen_table_metadata_file(m, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertEqual('Create table ok' in rs, True)

        self.multidimension_vk = {'k1': ('string:index', 'testvalue0'),
                                  'k2': ('string', 'testvalue1'),
                                  'k3': ('string', 1.1)}
        self.multidimension_scan_vk = {'k1': 'testvalue0'}
        table_info = self.showtable(self.ns_leader)
        self.tid = int(table_info.keys()[0][1])
        self.pid = 3
        for _ in range(10):
            self.put(self.leader, self.tid, self.pid, 'testkey0', self.now() + 90000, 'testvalue0')


    def get_new_ns_leader(self):
        nsc = NsCluster(conf.zk_endpoint, *(i[1] for i in conf.ns_endpoints))
        nsc.get_ns_leader()
        infoLogger.info([x[1] for x in conf.ns_endpoints])
        nss = [x[1] for x in conf.ns_endpoints]
        self.ns_leader = utils.exe_shell('head -n 1 {}/ns_leader'.format(self.testpath))
        self.node_path_dict[self.ns_leader] = utils.exe_shell('tail -n 1 {}/ns_leader'.format(self.testpath))
        nss.remove(self.ns_leader)
        self.ns_slaver = nss[0]
        infoLogger.info("*"*88)
        infoLogger.info(self.ns_leader)
        infoLogger.info(self.ns_slaver)
        infoLogger.info("*"*88)


    def get_latest_op(self):
        rs = self.showopstatus(self.ns_leader)
        latest_ley = max(rs.keys())
        return latest_ley, rs[latest_ley][0]


    def put_data(self, endpoint, n=1):
        for _ in range(n):
            self.put(endpoint, self.tid, self.pid, "testkey0", self.now() + 1000, "testvalue0")


    @staticmethod
    def get_steps_dict():
        return {
            -1: 'time.sleep(3)',
            0: 'time.sleep(10)',
            1: 'self.confset_createtable_put()',
            2: 'self.stop_client(self.ns_leader)',
            3: 'self.disconnectzk(self.ns_leader, "ns_client")',
            4: 'self.put_large_datas(500, 7)',
            5: 'self.put_data(self.leader)',
            6: 'self.makesnapshot(self.ns_leader, self.tname, self.pid, \'ns_client\', 0)',
            7: 'self.start_client(self.ns_leader, "nameserver")',
            8: 'self.connectzk(self.ns_leader, "ns_client")',
            9: 'self.get_new_ns_leader()',
            10: 'None',
            12: 'self.assertEqual(self.get_manifest(self.leaderpath, self.tid, self.pid)["offset"], "3510")',
            13: 'self.assertEqual("15", self.get_table_status(self.slave1, self.tid, self.pid)[0])',
            14: 'self.assertEqual("drop ok", self.ns_drop(self.ns_leader, self.tname))',
            15: 'self.assertFalse(self.showtable(self.ns_leader) is {})',
        }


    @ddt.data(
        (9,8,9),  # RTIDB-223
        (9,1,3,8,5,5,5,5,5,-1,9,13,14),  # ns_leader断网，可以继续put及同步数据
        (9,1,2,7,5,5,5,5,5,-1,9,13,14),  # ns_leader挂掉，可以继续put及同步数据
        (9,1,4,6,3,0,8,12,9),  # ns_leader断网，可以makesnapshot成功
        (9,1,4,6,2,0,7,12,9),  # ns_leader挂掉，可以makesnapshot成功
        (9,1,2,0,7,9,14,15,-1),  # ns_leader挂掉，可以drop表
        (9,1,3,0,8,9,14,15,-1),  # ns_leader断网，可以drop表
        (9,1,2,0,7,9,1,15,-1),  # ns_leader挂掉，可以create并put
        (9,1,3,0,8,9,1,15,-1),  # ns_leader断网，可以create并put
    )
    @ddt.unpack
    def test_ns_ha(self, *steps):
        steps_dict = self.get_steps_dict()
        for i in steps:
            infoLogger.info('*' * 10 + ' Executing step {}: {}'.format(i, steps_dict[i]))
            eval(steps_dict[i])
        self.assertEqual(['msg:', 'nameserver', 'is', 'not', 'leader'],
                         self.showtable(self.ns_slaver).values()[0])


if __name__ == "__main__":
    load(TestNameserverHa)
