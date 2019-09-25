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
from libs.clients.tb_cluster import TbCluster
import libs.conf as conf


@ddt.ddt
class TestNameserverHa(TestCaseBase):

    def createtable_put(self):
        self.tname = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        # m = utils.gen_table_metadata(
        #     '"{}"'.format(self.tname), '"kAbsoluteTime"', 144000, 8,
        #     ('table_partition', '"{}"'.format(self.leader), '"0-3"', 'true'),
        #     ('table_partition', '"{}"'.format(self.slave1), '"0-3"', 'false'),
        #     ('table_partition', '"{}"'.format(self.slave2), '"2-3"', 'false'),
        #     ('column_desc', '"k1"', '"string"', 'true'),
        #     ('column_desc', '"k2"', '"string"', 'false'),
        #     ('column_desc', '"k3"', '"string"', 'false'))
        # utils.gen_table_metadata_file(m, metadata_path)

        table_meta = {
            "name": self.tname,
            "ttl": 144000,
            "storage_mode": "kSSD",
            "table_partition": [
                {"endpoint": self.leader,"pid_group": "0-3","is_leader": "true"},
                {"endpoint": self.slave1,"pid_group": "0-3","is_leader": "false"},
                {"endpoint": self.slave2,"pid_group": "2-3","is_leader": "false"},
            ],
            "column_desc":[
                {"name": "k1", "type": "string", "add_ts_idx": "true"},
                {"name": "k2", "type": "string", "add_ts_idx": "false"},
                {"name": "k3", "type": "string", "add_ts_idx": "false"},
            ],
        }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertTrue('Create table ok', rs)

        self.multidimension_vk = {'k1': ('string:index', 'testvalue0'),
                                  'k2': ('string', 'testvalue1'),
                                  'k3': ('string', 1.1)}
        self.multidimension_scan_vk = {'k1': 'testvalue0'}
        table_info = self.showtable(self.ns_leader, self.tname)
        self.tid = int(table_info.keys()[0][1])
        self.pid = 3
        for _ in range(10):
            self.put(self.leader, self.tid, self.pid, 'testkey0', self.now() + 90000, 'testvalue0')


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
            1: 'self.createtable_put()',
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
            14: 'self.assertIn("drop ok", self.ns_drop(self.ns_leader, self.tname))',
            15: 'self.assertFalse(self.showtable(self.ns_leader) is {})',
            16: 'self.confset(self.ns_leader, "auto_failover", "true")',
            17: 'self.confset(self.ns_leader, "auto_failover", "false")',
            20: 'self.stop_client(self.ns_slaver)',
            21: 'self.start_client(self.ns_slaver, "nameserver")',
        }

    @ddt.data(
        (16,9,1,3,8,5,5,5,5,5,-1,2,7,0,9,13,14,17),  # ns_leader断网，可以继续put及同步数据
        (16,9,1,2,7,5,5,5,5,5,0,9,13,14,17),  # ns_leader挂掉，可以继续put及同步数据
        (16,9,1,4,6,3,0,8,12,2,7,0,9,17),  # ns_leader断网，可以makesnapshot成功
        (16,9,1,4,6,2,0,7,12,9,17),  # ns_leader挂掉，可以makesnapshot成功
        (16,9,1,2,0,7,9,14,15,-1,17),  # ns_leader挂掉，可以drop表
        (16,9,1,3,0,8,2,7,0,9,14,15,-1,17),  # ns_leader断网，可以drop表
        (16,9,1,2,0,7,9,1,15,-1,17),  # ns_leader挂掉，可以create并put
        (16,9,1,3,0,8,2,7,0,9,1,15,-1,17),  # ns_leader断网，可以create并put
    )
    @ddt.unpack
    def test_ns_ha(self, *steps):
        """
        ns节点故障切换测试
        :param steps:
        :return:
        """
        steps_dict = self.get_steps_dict()
        for i in steps:
            infoLogger.info('*' * 10 + ' Executing step {}: {}'.format(i, steps_dict[i]))
            eval(steps_dict[i])
        infoLogger.info(self.ns_slaver, self.tname)
        rs = self.showtable(self.ns_slaver)
        for repeat in range(10):
            if rs == 'failed to showtable:':
                time.sleep(2)
                rs = self.showtable(self.ns_slaver, self.tname)
                continue
            break
        self.assertIn('nameserver is not leader', rs)
        self.ns_drop(self.ns_leader, self.tname)


    @ddt.data(
        (16,9,20,-1,3,8,0,9,17),  # 唯一一个ns_leader闪断后，可以正确判断节点状态  # RTIDB-246
        (16,9,20,-1,2,7,0,9,17),  # 唯一一个ns_leader重启后，可以正确判断节点状态
    )
    @ddt.unpack
    def test_ns_unique_leader(self, *steps):
        """
        唯一一个ns节点故障恢复
        :param steps:
        :return:
        """
        steps_dict = self.get_steps_dict()
        for i in steps:
            infoLogger.info('*' * 10 + ' Executing step {}: {}'.format(i, steps_dict[i]))
            eval(steps_dict[i])
        self.stop_client(self.leader)
        time.sleep(10)
        rs = self.showtablet(self.ns_leader)
        self.start_client(self.leader)
        self.start_client(self.ns_slaver, "nameserver")
        time.sleep(5)
        self.get_new_ns_leader()
        self.assertEqual(rs[self.leader][0], 'kTabletOffline')

    @ddt.data(
        (16,9,3,8,0,9,17),  # ns_leader断网重启后，新的ns_leader可以正确判断节点状态
        (16,9,2,7,0,9,17),  # ns_leader重启后，新的ns_leader可以正确判断节点状态
        (16,9,3,8,0,9,2,7,0,9,17),  # ns_leader断网后，新的ns_leader重启，切回原leader后可以正确判断节点状态
    )
    @ddt.unpack
    def test_ns_after_failover(self, *steps):
        """
        ns故障切换后，新主可以判断节点状态
        :param steps:
        :return:
        """
        self.createtable_put()
        rs1 = self.showtable(self.ns_leader, self.tname)
        steps_dict = self.get_steps_dict()
        for i in steps:
            infoLogger.info('*' * 10 + ' Executing step {}: {}'.format(i, steps_dict[i]))
            eval(steps_dict[i])
        rs2 = self.showtable(self.ns_leader, self.tname)
        self.stop_client(self.leader)
        self.updatetablealive(self.ns_leader, self.tname, '*', self.leader, 'no')
        time.sleep(10)
        rs3 = self.showtablet(self.ns_leader)
        rs4 = self.showtable(self.ns_leader, self.tname)
        self.start_client(self.leader)
        self.stop_client(self.ns_leader)
        self.start_client(self.ns_leader, 'nameserver')
        time.sleep(10)
        self.get_new_ns_leader()
        self.assertEqual(rs1, rs2)
        self.assertEqual(rs3[self.leader][0], 'kTabletOffline')
        self.assertEqual([v[-2] for k, v in rs4.items() if k[-1] == self.leader], ['no'] * 4)
        self.ns_drop(self.ns_leader, self.tname)


    @ddt.data(
        (17,9,1,16,2,0,7,0,9),  # ns_leader confset之后挂掉，新ns_leader在confget时新的conf  # RTIDB-197
    )
    @ddt.unpack
    def test_ns_slaver_conf_sync(self, *steps):
        """
        ns_leader confset之后挂掉，新ns_leader在confget时新的conf
        :param steps:
        :return:
        """
        steps_dict = self.get_steps_dict()
        for i in steps:
            infoLogger.info('*' * 10 + ' Executing step {}: {}'.format(i, steps_dict[i]))
            eval(steps_dict[i])
        rs = self.showtable(self.ns_slaver, self.tname)
        rs1 = self.confget(self.ns_leader, "auto_failover")
        nsc = NsCluster(conf.zk_endpoint, *(i for i in conf.ns_endpoints))
        nsc.kill(*nsc.endpoints)
        nsc.start(*nsc.endpoints)
        # time.sleep(5)
        self.get_new_ns_leader()
        self.confset(self.ns_leader, 'auto_failover', 'false')
        self.assertIn('nameserver is not leader', rs)
        self.assertIn('true', rs1)
        self.ns_drop(self.ns_leader, self.tname)


    #@TestCaseBase.skip('FIXME')
    @ddt.data(
        (9,3,8,0,9),  # ns 闪断，RTIDB-223
    )
    @ddt.unpack
    def test_ns_flashbreak(self, *steps):
        """
        ns闪断
        :param steps:
        :return:
        """
        steps_dict = self.get_steps_dict()
        for i in steps:
            infoLogger.info('*' * 10 + ' Executing step {}: {}'.format(i, steps_dict[i]))
            eval(steps_dict[i])
        rs = self.showtable(self.ns_slaver)
        nsc = NsCluster(conf.zk_endpoint, *(i for i in conf.ns_endpoints))
        nsc.kill(*nsc.endpoints)
        nsc.start(*nsc.endpoints)
        time.sleep(3)
        nsc.get_ns_leader()
        self.assertIn('nameserver is not leader', rs)


    def test_ha_cluster(self):
        """
        zk没挂，集群机房挂掉，重启后可正常加载table信息
        :return:
        """
        self.createtable_put()
        rs1 = self.showtable(self.ns_leader, self.tname)
        nsc = NsCluster(conf.zk_endpoint, *(i for i in conf.ns_endpoints))
        tbc = TbCluster(conf.zk_endpoint, conf.tb_endpoints)
        nsc.kill(*nsc.endpoints)
        tbc.kill(*tbc.endpoints)
        nsc.start(*nsc.endpoints)
        tbc.start(tbc.endpoints)
        time.sleep(3)
        self.get_new_ns_leader()
        rs2 = self.showtable(self.ns_leader, self.tname)
        self.assertEqual(rs1.keys(), rs2.keys())
        self.ns_drop(self.ns_leader, self.tname)


if __name__ == "__main__":
    load(TestNameserverHa)
