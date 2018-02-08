# -*- coding: utf-8 -*-
from testcasebase import TestCaseBase
import time
from libs.test_loader import load
import libs.utils as utils
from libs.logger import infoLogger
from libs.deco import multi_dimension
import libs.ddt as ddt
import libs.conf as conf

@ddt.ddt
class TestDelReplicaNs(TestCaseBase):

    def get_base_attr(attr):
        TestCaseBase.setUpClass()
        return TestCaseBase.__getattribute__(TestCaseBase, attr)


    def test_delreplica_scenario(self):
        """
        addreplica之前和delreplica之后，put到主节点的数据无法同步给副本
        addreplica和再次addreplica之后，put到主节点的数据可以同步给副本
        :return:
        """
        rs1 = self.showopstatus(self.ns_leader)
        old_last_op_id = max(rs1.keys()) if rs1 != {} else 1
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        name = 'tname{}'.format(int(time.time() * 1000000 % 10000000000))
        if conf.multidimension is False:
            m = utils.gen_table_metadata(
                '"{}"'.format(name), None, 144000, 2,
                ('table_partition', '"{}"'.format(self.leader), '"1-3"', 'true'))
        else:
            m = utils.gen_table_metadata(
                '"{}"'.format(name), None, 144000, 2,
                ('table_partition', '"{}"'.format(self.leader), '"1-3"', 'true'),
                ('column_desc', '"merchant"', '"string"', 'true'),
                ('column_desc', '"amt"', '"double"', 'false'),
                ('column_desc', '"card"', '"string"', 'true'),
            )
        utils.gen_table_metadata_file(m, metadata_path)
        rs2 = self.ns_create(self.ns_leader, metadata_path)
        self.assertTrue('Create table ok' in rs2)

        rs3 = self.showtable(self.ns_leader)
        tid = rs3.keys()[0][1]
        pid = 1

        # put before addreplica
        self.multidimension_scan_vk = {'card': 'testkey0'}
        self.multidimension_vk = {'card': ('string:index', 'testkey0'),
                                  'merchant': ('string:index', 'testvalue0'), 'amt': ('double', 1.1)}
        rs4 = self.put(self.leader, tid, pid, 'testkey0', self.now() + 10000, 'testvalue0')
        self.assertTrue('Put ok' in rs4)

        # makesnapshot
        rs5 = self.makesnapshot(self.ns_leader, name, pid, 'ns_client')
        self.assertTrue('MakeSnapshot ok' in rs5)
        time.sleep(2)
        self.showtablet(self.ns_leader)

        # put after addreplica
        rs6 = self.addreplica(self.ns_leader, name, pid, 'ns_client', self.slave1)
        self.assertTrue('AddReplica ok' in rs6)
        last_op_id = max(self.showopstatus(self.ns_leader).keys())
        self.assertTrue(old_last_op_id != last_op_id)
        last_opstatus = self.showopstatus(self.ns_leader)[last_op_id]
        self.assertTrue('kAddReplicaOP', last_opstatus)
        self.multidimension_vk = {'card': ('string:index', 'testkey0'),
                                  'merchant': ('string:index', 'testvalue1'), 'amt': ('double', 1.1)}
        rs7 = self.put(self.leader, tid, pid, 'testkey0', self.now() + 90000, 'testvalue1')
        self.assertTrue('Put ok' in rs7)
        time.sleep(5)

        # put after delreplica
        rs8 = self.delreplica(self.ns_leader, name, pid, 'ns_client', self.slave1)
        self.assertTrue('DelReplica ok' in rs8)
        time.sleep(3)
        self.multidimension_vk = {'card': ('string:index', 'testkey0'),
                                  'merchant': ('string:index', 'testvalue2'), 'amt': ('double', 1.1)}
        rs9 = self.put(self.leader, tid, pid, 'testkey0', self.now() + 90000, 'testvalue2')
        self.assertTrue('Put ok' in rs9)

        # put after re-addreplica
        rs10 = self.addreplica(self.ns_leader, name, pid, 'ns_client', self.slave1)
        self.assertTrue('AddReplica ok' in rs10)
        self.multidimension_vk = {'card': ('string:index', 'testkey0'),
                                  'merchant': ('string:index', 'testvalue3'), 'amt': ('double', 1.1)}
        rs11 = self.put(self.leader, tid, pid, 'testkey0', self.now() + 90000, 'testvalue3')
        self.assertTrue('Put ok' in rs11)
        time.sleep(5)

        rs12 = self.scan(self.slave1, tid, pid, 'testkey0', self.now() + 90000, 1)
        self.assertEqual('testvalue0' in rs12, True)
        self.assertEqual('testvalue1' in rs12, True)
        self.assertEqual('testvalue2' in rs12, False)
        self.assertEqual('testvalue3' in rs12, True)


    @ddt.data(
        ('notexsit', None, None, 'Fail to delreplica. error msg:table is not  exist!'),
        (None, 10, None, 'Fail to delreplica. error msg:create op failed'),
        (None, None, get_base_attr('leader'), 'Fail to delreplica. error msg:create op failed'),
        (None, None, '127.1.1.1:6666', 'Fail to delreplica. error msg:create op failed'),
    )
    @ddt.unpack
    def test_delreplica_args_invalid(self, tname, pid, endpoint, exp_msg):
        """
        建表时带副本，然后删掉副本时，参数异常检查
        :return:
        """
        name = 't{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        m = utils.gen_table_metadata('"{}"'.format(name), '"kLatestTime"', 100, 8,
                                     ('table_partition', '"{}"'.format(self.leader), '"1-3"', 'true'),
                                     ('table_partition', '"{}"'.format(self.slave1), '"1-2"', 'false'),
                                     ('table_partition', '"{}"'.format(self.slave2), '"2-3"', 'false'))
        utils.gen_table_metadata_file(m, metadata_path)
        rs1 = self.ns_create(self.ns_leader, metadata_path)
        self.assertTrue('Create table ok' in rs1)

        table_name = name if tname is None else tname
        tpid = 1 if pid is None else pid
        tendpoint = self.slave1 if endpoint is None else endpoint
        self.showtable(self.ns_leader)
        rs3 = self.delreplica(self.ns_leader, table_name, tpid, 'ns_client', tendpoint)
        self.assertTrue(exp_msg in rs3)


    def test_delreplica_not_alive(self):
        """
        建表时带副本，然后删掉副本，showtable时不会再出现删掉的副本
        :return:
        """
        self.start_client(self.slave1path)
        name = 't{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        m = utils.gen_table_metadata('"{}"'.format(name), '"kLatestTime"', 100, 8,
                                     ('table_partition', '"{}"'.format(self.leader), '"1-3"', 'true'),
                                     ('table_partition', '"{}"'.format(self.slave1), '"1-2"', 'false'),
                                     ('table_partition', '"{}"'.format(self.slave2), '"2-3"', 'false'),
                                     ('column_desc', '"merchant"', '"string"', 'true'),
                                     ('column_desc', '"amt"', '"double"', 'false'),
                                     ('column_desc', '"card"', '"string"', 'true'),)
        utils.gen_table_metadata_file(m, metadata_path)
        rs1 = self.ns_create(self.ns_leader, metadata_path)
        self.assertTrue('Create table ok' in rs1)

        rs2 = self.showtable(self.ns_leader)
        tid = rs2.keys()[0][1]

        self.stop_client(self.slave1)
        time.sleep(10)

        self.showtable(self.ns_leader)

        rs3 = self.delreplica(self.ns_leader, name, 1, 'ns_client', self.slave1)
        self.assertTrue('DelReplica ok' in rs3)
        time.sleep(5)

        rs4 = self.showtable(self.ns_leader)
        self.start_client(self.slave1path)
        self.assertEqual(rs4[(name, tid, '1', self.slave1)], ['follower', '8', '100', 'no'])


if __name__ == "__main__":
    load(TestDelReplicaNs)
