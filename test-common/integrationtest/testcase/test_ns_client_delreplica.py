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

    leader, slave1, slave2 = (i[1] for i in conf.tb_endpoints)

    def test_delreplica_scenario(self):
        """
        addreplica之前和delreplica之后，put到主节点的数据无法同步给副本
        addreplica和再次addreplica之后，put到主节点的数据可以同步给副本
        :return:
        """
        rs1 = self.showopstatus(self.ns_leader)
        old_last_op_id = max(rs1.keys()) if rs1 != {} else 1
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        name = 'tname{}'.format(time.time())
        if conf.multidimension is False:
            m = utils.gen_table_metadata(
                '"{}"'.format(name), None, 144000, 2,
                ('table_partition', '"{}"'.format(self.leader), '"0-2"', 'true'))
        else:
            m = utils.gen_table_metadata(
                '"{}"'.format(name), None, 144000, 2,
                ('table_partition', '"{}"'.format(self.leader), '"0-2"', 'true'),
                ('column_desc', '"merchant"', '"string"', 'true'),
                ('column_desc', '"amt"', '"double"', 'false'),
                ('column_desc', '"card"', '"string"', 'true'),
            )
        utils.gen_table_metadata_file(m, metadata_path)
        rs2 = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs2)

        rs3 = self.showtable(self.ns_leader, name)
        tid = rs3.keys()[0][1]
        pid = 0

        # put and addreplica
        self.multidimension_scan_vk = {'card': 'testkey0'}
        self.multidimension_vk = {'card': ('string:index', 'testkey0'),
                                  'merchant': ('string:index', 'testvalue0'), 'amt': ('double', 1.1)}
        rs4 = self.put(self.leader, tid, pid, 'testkey0', self.now() + 10000, 'testvalue0')
        if rs4 == 'Put failed':
            infoLogger.error(' ')
            rs = self.showtable(self.ns_leader)
            tablestatus = self.parse_tb(rs, ' ', [0, 1, 2, 3], [4, 5, 6, 7, 8])
            for status in tablestatus:
                infoLogger.info('{} =  {}'.format(status, tablestatus[status]))
            infoLogger.error(' ')

            rs4 = self.put(self.leader, tid, pid, 'testkey0', self.now() + 10000, 'testvalue0')
            self.assertIn('Put ok', rs4)
        self.assertIn('Put ok', rs4)

        # makesnapshot
        rs5 = self.makesnapshot(self.ns_leader, name, pid, 'ns_client')
        self.assertIn('MakeSnapshot ok', rs5)
        time.sleep(2)
        self.showtablet(self.ns_leader)

        # addreplica by ns_client and put
        rs6 = self.addreplica(self.ns_leader, name, pid, 'ns_client', self.slave1)
        self.assertIn('AddReplica ok', rs6)
        last_op_id = max(self.showopstatus(self.ns_leader).keys())
        self.assertTrue(old_last_op_id != last_op_id)
        last_opstatus = self.showopstatus(self.ns_leader)[last_op_id]
        self.assertIn('kAddReplicaOP', last_opstatus)
        self.multidimension_vk = {'card': ('string:index', 'testkey0'),
                                  'merchant': ('string:index', 'testvalue1'), 'amt': ('double', 1.1)}
        rs7 = self.put(self.leader, tid, pid, 'testkey0', self.now() + 90000, 'testvalue1')
        self.assertIn('Put ok', rs7)
        time.sleep(5)

        # delreplica by ns_client and put
        rs8 = self.delreplica(self.ns_leader, name, pid, 'ns_client', self.slave1)
        self.assertIn('DelReplica ok', rs8)
        edps = ''
        for repeat in range(10):
            time.sleep(2)
            rs13 = self.showtable(self.ns_leader, name)
            flag = False
            for x in rs13:
                if x[3] == self.slave1:
                    flag = True
                    break
            if flag == False:
                edps = [x[3] for x in rs13]
                break
        self.assertFalse(self.slave1 in edps)
        self.multidimension_vk = {'card': ('string:index', 'testkey0'),
                                  'merchant': ('string:index', 'testvalue2'), 'amt': ('double', 1.1)}
        rs9 = self.put(self.leader, tid, pid, 'testkey0', self.now() + 90000, 'testvalue2')
        self.assertIn('Put ok', rs9)

        # put after re-addreplica by client
        rs10 = self.addreplica(self.ns_leader, name, pid, 'ns_client', self.slave1)
        self.assertIn('AddReplica ok', rs10)
        self.multidimension_vk = {'card': ('string:index', 'testkey0'),
                                  'merchant': ('string:index', 'testvalue3'), 'amt': ('double', 1.1)}
        rs11 = self.put(self.leader, tid, pid, 'testkey0', self.now() + 90000, 'testvalue3')
        self.assertIn('Put ok', rs11)
        time.sleep(5)

        rs12 = self.scan(self.slave1, tid, pid, 'testkey0', self.now() + 90000, 1)
        self.assertIn('testvalue0', rs12)
        self.assertIn('testvalue1', rs12)
        self.assertIn('testvalue2', rs12)
        self.assertIn('testvalue3', rs12)
        self.ns_drop(self.ns_leader, name)

    def test_delreplica_drop_table(self):
        """
        delreplica后droptable
        :return:
        """
        name = 't{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        m = utils.gen_table_metadata('"{}"'.format(name), '"kLatestTime"', 100, 8,
                                     ('table_partition', '"{}"'.format(self.leader), '"0-2"', 'true'),
                                     ('table_partition', '"{}"'.format(self.slave1), '"0-2"', 'false'),
                                     ('table_partition', '"{}"'.format(self.slave2), '"0-2"', 'false'))
        utils.gen_table_metadata_file(m, metadata_path)
        rs1 = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs1)
        rs2 = self.get_table_status(self.slave1)
        rs3 = self.showtable(self.ns_leader, name)
        tid = tid = int(rs3.keys()[0][1])
        rs4 = self.delreplica(self.ns_leader, name, 0, 'ns_client', self.slave1)
        time.sleep(10)
        rs5 = self.showtable(self.ns_leader, name)
        rs6 = self.get_table_status(self.slave1)
        self.assertIn((name, str(tid), str(0), self.slave1), rs3)
        self.assertNotIn((name, str(tid), str(0), self.slave1), rs5)
        self.assertIn((tid, 0), rs2.keys())
        self.assertNotIn((tid, 0), rs6.keys())
        self.ns_drop(self.ns_leader, name)

    @ddt.data(
        ('notexsit', None, None, 'Fail to delreplica'),
        (None, 10, None, 'Fail to delreplica'),
        (None, None, leader, 'Fail to delreplica'),
        (None, None, '127.1.1.1:6666', 'Fail to delreplica'),
    )
    @ddt.unpack
    def test_delreplica_args_invalid(self, tname, pid, endpoint, exp_msg):
        """
        建表时带副本，然后删掉副本时，参数异常检查
        :return:
        """
        name = 't{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        name = '"{}"'.format(name)
        m = utils.gen_table_metadata(name, '"kLatestTime"', 100, 8,
                                     ('table_partition', '"{}"'.format(self.leader), '"0-2"', 'true'),
                                     ('table_partition', '"{}"'.format(self.slave1), '"0-1"', 'false'),
                                     ('table_partition', '"{}"'.format(self.slave2), '"1-2"', 'false'))
        utils.gen_table_metadata_file(m, metadata_path)
        rs1 = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs1)

        table_name = name if tname is None else tname
        tpid = 0 if pid is None else pid
        tendpoint = self.slave1 if endpoint is None else endpoint
        self.showtable(self.ns_leader, name)
        rs3 = self.delreplica(self.ns_leader, table_name, tpid, 'ns_client', tendpoint)
        self.assertIn(exp_msg, rs3)
        self.ns_drop(self.ns_leader, name)


    def test_delreplica_not_alive(self):  # RTIDB-201
        """
        建表时带副本，然后删掉副本，showtable时不会再出现删掉的副本
        :return:
        """
        self.start_client(self.slave1)
        name = 't{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        m = utils.gen_table_metadata('"{}"'.format(name), '"kLatestTime"', 100, 8,
                                     ('table_partition', '"{}"'.format(self.leader), '"0-2"', 'true'),
                                     ('table_partition', '"{}"'.format(self.slave1), '"0-1"', 'false'),
                                     ('table_partition', '"{}"'.format(self.slave2), '"1-2"', 'false'),
                                     ('column_desc', '"merchant"', '"string"', 'true'),
                                     ('column_desc', '"amt"', '"double"', 'false'),
                                     ('column_desc', '"card"', '"string"', 'true'),)
        utils.gen_table_metadata_file(m, metadata_path)
        rs1 = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs1)

        rs2 = self.showtable(self.ns_leader, name)
        tid = rs2.keys()[0][1]

        self.stop_client(self.slave1)
        self.updatetablealive(self.ns_leader, name, '*', self.slave1, 'no')
        time.sleep(10)

        self.showtable(self.ns_leader, name)
        rs3 = self.delreplica(self.ns_leader, name, 0, 'ns_client', self.slave1)
        time.sleep(5)
        rs4 = self.showtable(self.ns_leader, name)
        self.start_client(self.slave1)
        time.sleep(10)
        self.assertIn('Fail to delreplica', rs3)
        self.assertEqual(rs4[(name, tid, '1', self.slave1)], ['follower', '100', 'no', 'kNoCompress'])
        self.ns_drop(self.ns_leader, name)

if __name__ == "__main__":
    load(TestDelReplicaNs)
