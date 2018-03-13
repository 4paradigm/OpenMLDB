# -*- coding: utf-8 -*-
from testcasebase import TestCaseBase
import threading
import time
from libs.deco import multi_dimension
from libs.logger import infoLogger
import libs.ddt as ddt
from libs.test_loader import load
import ctypes
import libs.utils as utils


@ddt.ddt
class TestPut(TestCaseBase):

    def test_put_normal(self):
        """
        put成功后可以scan出来
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid, 144000, 2)
        self.assertTrue('Create table ok' in rs1)
        rs2 = self.put(self.leader,
                       self.tid,
                       self.pid,
                       'testkey0',
                       self.now(),
                       'testvalue0')
        self.assertTrue('Put ok' in rs2)
        time.sleep(1)
        self.assertTrue(
            'testvalue0' in self.scan(self.leader, self.tid, self.pid, 'testkey0', self.now(), 1))


    @multi_dimension(False)
    def test_put_slave_sync(self):
        """
        put到leader后，slave同步成功
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid, 144000, 2, 'true', self.slave1, self.slave2)
        self.assertTrue('Create table ok' in rs1)
        rs2 = self.create(self.slave1, 't', self.tid, self.pid, 144000, 2, 'false', self.slave1, self.slave2)
        self.assertTrue('Create table ok' in rs2)
        rs2 = self.put(self.leader,
                       self.tid,
                       self.pid,
                       'testkey0',
                       self.now(),
                       'testvalue0')
        self.assertTrue('Put ok' in rs2)
        time.sleep(1)
        self.assertTrue(
            'testvalue0' in self.scan(self.slave1, self.tid, self.pid, 'testkey0', self.now(), 1))


    @multi_dimension(True)
    def test_put_slave_sync_md(self):
        """
        put到leader后，slave同步成功
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid, 144000, 2, 'true')
        self.assertTrue('Create table ok' in rs1)
        rs2 = self.create(self.slave1, 't', self.tid, self.pid, 144000, 2, 'false')
        self.assertTrue('Create table ok' in rs2)
        rs3 = self.addreplica(self.leader, self.tid, self.pid, 'client', self.slave1)
        self.assertTrue('AddReplica ok' in rs3)
        rs4 = self.put(self.leader,
                       self.tid,
                       self.pid,
                       '',
                       self.now(),
                       'testvalue0', '1.1', 'testkey0')
        self.assertTrue('Put ok' in rs4)
        time.sleep(1)
        self.assertTrue(
            'testvalue0' in self.scan(self.slave1, self.tid, self.pid, {'card': 'testkey0'}, self.now(), 1))


    def test_put_slave_cannot_put(self):
        """
        slave不允许put
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid, 144000, 8, 'false', self.leader)
        self.assertTrue('Create table ok' in rs1)
        rs2 = self.put(self.leader,
                       self.tid,
                       self.pid,
                       'testkey0',
                       self.now(),
                       'testvalue0')
        self.assertTrue('Put failed' in rs2)


    @multi_dimension(False)
    def test_put_slave_killed_while_leader_putting(self):
        """
        写数据过程中从节点挂掉，不影响主节点
        重新启动后可以loadtable成功，数据与主节点一致
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid, 144000, 8, 'true', self.slave1)
        self.assertTrue('Create table ok' in rs1)
        rs2 = self.create(self.slave1, 't', self.tid, self.pid, 144000, 8, 'false', self.slave1)
        self.assertTrue('Create table ok' in rs2)

        def put(count):
            for i in range(0, count):
                self.put(self.leader,
                         self.tid,
                         self.pid,
                         'testkey',
                         self.now() - 1,
                         'testvalue{}'.format(i))

        def stop_client(endpoint):
            self.stop_client(endpoint)

        threads = [threading.Thread(
            target=put, args=(20,)), threading.Thread(
            target=stop_client, args=(self.slave1,))]

        # 写入数据1s后节点挂掉
        for t in threads:
            t.start()
            time.sleep(2)
        for t in threads:
            t.join()
        time.sleep(10)

        self.start_client(self.slave1)
        utils.exe_shell('rm -rf {}/db/{}_{}/binlog'.format(self.slave1path, self.tid, self.pid))
        self.cp_db(self.leaderpath, self.slave1path, self.tid, self.pid)
        rs4 = self.loadtable(self.slave1, 't', self.tid, self.pid, 144000, 8, 'false', self.slave1)
        self.assertTrue('LoadTable ok' in rs4)
        time.sleep(1)
        self.assertTrue('testvalue19' in self.scan(self.slave1, self.tid, self.pid, 'testkey', self.now(), 1))


    @multi_dimension(True)
    def test_put_slave_killed_while_leader_putting_md(self):
        """
        写数据过程中从节点挂掉，不影响主节点
        重新启动后可以loadtable成功，数据与主节点一致
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid, 144000, 8, 'true')
        self.assertTrue('Create table ok' in rs1)
        rs2 = self.create(self.slave1, 't', self.tid, self.pid, 144000, 8, 'false')
        self.assertTrue('Create table ok' in rs2)

        def put(count):
            for i in range(0, count):
                self.put(self.leader,
                         self.tid,
                         self.pid,
                         '',
                         self.now() - 1,
                         'testvalue{}'.format(i), '1.1', 'testkey')

        def stop_client(endpoint):
            self.stop_client(endpoint)

        threads = [threading.Thread(
            target=put, args=(20,)), threading.Thread(
            target=stop_client, args=(self.slave1,))]

        # 写入数据1s后节点挂掉
        for t in threads:
            t.start()
            time.sleep(2)
        for t in threads:
            t.join()
        time.sleep(10)

        self.start_client(self.slave1)
        utils.exe_shell('rm -rf {}/db/{}_{}/binlog'.format(self.slave1path, self.tid, self.pid))
        self.cp_db(self.leaderpath, self.slave1path, self.tid, self.pid)
        rs4 = self.loadtable(self.slave1, 't', self.tid, self.pid, 144000, 8, 'false', self.slave1)
        self.assertTrue('LoadTable ok' in rs4)
        time.sleep(1)
        self.assertTrue('testvalue19' in self.scan(self.slave1, self.tid, self.pid, {'card':'testkey'}, self.now(), 1))


    @multi_dimension(True)
    @ddt.data(
        ({'card': ('string:index', 'str1'), 'card2': ('int32:index', 3), 'amt': ('double', 1.1)}, 'Put ok'),
        ({'card': ('string:index', 'card0')}, 'Put ok'),
        ({'card': ('string', 'card0')}, 'Put ok'),
        ({'card': ('string:index', 'str1'), 'card2': ('int32:index', 3), 'amt': ('double', '')},
         'Input value mismatch schema'),
        ({'card': ('string:index', 'str1'), 'card2': ('int32', 3), 'amt': ('double', 1.1)}, 'Put ok'),
        ({'card': ('string', 'str1'), 'card2': ('int32', 3), 'amt': ('double', 1.1)}, 'Put ok'),
    )
    @ddt.unpack
    def test_sput_index(self, kv, rsp_msg):
        """
        创建高维表，对index进行测试
        :return:
        """
        self.create(self.leader, 't', self.tid, self.pid, 144000, 2, 'true', **{k: v[0] for k, v in kv.items()})
        rs1 = self.put(self.leader, self.tid, self.pid, '', self.now(), *[str(v[1]) for v in kv.values()])
        self.assertTrue(rsp_msg in rs1)


    @multi_dimension(True)
    @ddt.data(
        ({'card': ('string:index', '0'), 's2': ('int32', 2147483647)},
         'Put ok', {'card': '0'}, '2147483647'),
        ({'card': ('string:index', '1'), 's2': ('int32', 1.1)},
         'bad lexical cast: source type value could not be interpreted as target', {}, ''),
        ({'card': ('string:index', '2'), 's2': ('int32', 1e+5)},
         'bad lexical cast: source type value could not be interpreted as target', {}, ''),
        ({'card': ('string:index', '3'), 's2': ('int32', 'aaaa')},
         'bad lexical cast: source type value could not be interpreted as target', {}, ''),
        ({'card': ('string:index', '4'), 's2': ('int32', 2147483648)},
         'bad lexical cast: source type value could not be interpreted as target', {}, ''),
        ({'card': ('string:index', '5'), 's2': ('int32', -214)},
         'Put ok', {'card': '5'}, '-214'),
        ({'card': ('string:index', '6'), 's2': ('int64', -9223372036854775808)},
         'Put ok', {'card': '6'}, '-9223372036854775808'),
        ({'card': ('string:index', '7'), 's2': ('int64', -9223372036854775809)},
         'bad lexical cast: source type value could not be interpreted as target', {}, ''),
    )
    @ddt.unpack
    def test_sput_int(self, kv, rsp_msg, scan_kv, scan_value):
        """
        创建高维表，对int32和int64类型进行测试
        :return:
        """
        self.create(self.leader, 't', self.tid, self.pid, 144000, 2, 'true', **{k: v[0] for k, v in kv.items()})
        rs1 = self.put(self.leader, self.tid, self.pid, '', self.now(), *[str(v[1]) for v in kv.values()])
        self.assertTrue(rsp_msg in rs1)
        if scan_kv != {}:
            rs2 = self.scan(self.leader, self.tid, self.pid, scan_kv, self.now(), 1)
            self.assertTrue(' ' + str(scan_value) + ' ' in rs2)


    @multi_dimension(True)
    @ddt.data(
        ({'card': ('string:index', '0'), 's2': ('uint32', 2147483648)},
         'Put ok', {'card': '0'}, '2147483648'),
        ({'card': ('string:index', '1'), 's2': ('uint32', 1.1)},
         'bad lexical cast: source type value could not be interpreted as target', {}, ''),
        ({'card': ('string:index', '2'), 's2': ('uint32', 1e+5)},
         'bad lexical cast: source type value could not be interpreted as target', {}, ''),
        ({'card': ('string:index', '3'), 's2': ('uint32', 'aaaa')},
         'bad lexical cast: source type value could not be interpreted as target', {}, ''),
        ({'card': ('string:index', '4'), 's2': ('uint32', -2)},
         'Put ok', {'card': '4'}, ctypes.c_uint32(-2).value),
        ({'card': ('string:index', '5'), 's2': ('uint64', 1)},
         'Put ok', {'card': '5'}, 1),
        ({'card': ('string:index', '6'), 's2': ('uint64', -111111111111111111)},
         'Put ok', {'card': '6'}, ctypes.c_uint64(-111111111111111111).value),
    )
    @ddt.unpack
    def test_sput_uint(self, kv, rsp_msg, scan_kv, scan_value):
        """
        创建高维表，对uint32和uint64类型进行测试
        :return:
        """
        self.create(self.leader, 't', self.tid, self.pid, 144000, 2, 'true', **{k: v[0] for k, v in kv.items()})
        rs1 = self.put(self.leader, self.tid, self.pid, '', self.now(), *[str(v[1]) for v in kv.values()])
        self.assertTrue(rsp_msg in rs1)
        if scan_kv != {}:
            rs2 = self.scan(self.leader, self.tid, self.pid, scan_kv, self.now(), 1)
            infoLogger.info(rs2)
            self.assertTrue(' ' + str(scan_value) + ' ' in rs2)


    @multi_dimension(True)
    @ddt.data(
        ({'card': ('string:index', '0'), 's2': ('string', '\\"\\"\'\'^\\n')},
         'Put ok', {'card': '0'}, '\"\"\'\'^\\n'),
        ({'card': ('string:index', '1'), 's2': ('string', '" "')},
         'Bad put format, eg put tid pid time value', {}, ''),
        ({'card': ('string:index', '2'), 's2': ('string', 'a' * 128)},
         'Put ok', {'card': '2'}, 'a' * 128),
        ({'card': ('string:index', '3'), 's2': ('string', 'a' * 129)},
         'Failed invalid value', {}, ''),
    )
    @ddt.unpack
    def test_sput_string(self, kv, rsp_msg, scan_kv, scan_value):
        """
        创建高维表，对string类型进行测试
        :return:
        """
        self.create(self.leader, 't', self.tid, self.pid, 144000, 2, 'true', **{k: v[0] for k, v in kv.items()})
        rs1 = self.put(self.leader, self.tid, self.pid, '', self.now(), *[str(v[1]) for v in kv.values()])
        infoLogger.info(rs1)
        self.assertTrue(rsp_msg in rs1)
        infoLogger.info(self.scan(
            self.leader, self.tid, self.pid, scan_kv, self.now(), 1))
        if scan_kv != {}:
            self.assertTrue(' ' + str(scan_value) + ' ' in self.scan(
                self.leader, self.tid, self.pid, scan_kv, self.now(), 1))


    @multi_dimension(True)
    @ddt.data(
        ({'card': ('string:index', '0'), 's2': ('float', 10.0)}, 'Put ok', {'card': '0'}, '10'),
        ({'card': ('string:index', '1'), 's2': ('float', 10.01)}, 'Put ok', {'card': '1'}, '10.0100002'),
        ({'card': ('string:index', '2'), 's2': ('float', -1e-1)}, 'Put ok', {'card': '2'}, '-0.100000001'),
        ({'card': ('string:index', '3'), 's2': ('float', 1e-10)}, 'Put ok', {'card': '3'}, '1.00000001e-10'),
        ({'card': ('string:index', '4'), 's2': ('double', -10.01)}, 'Put ok', {'card': '4'}, '-10.01'),
        ({'card': ('string:index', '5'), 's2': ('double', -1e-1)}, 'Put ok', {'card': '5'}, '-0.10000000000000001'),
        ({'card': ('string:index', '6'), 's2': ('double', 1e-10)}, 'Put ok', {'card': '6'}, '1e-10'),
    )
    @ddt.unpack
    def test_sput_float_double(self, kv, rsp_msg, scan_kv, scan_value):
        """
        创建高维表，对float和double类型进行测试
        :return:
        """
        self.create(self.leader, 't', self.tid, self.pid, 144000, 2, 'true', **{k: v[0] for k, v in kv.items()})
        rs1 = self.put(self.leader, self.tid, self.pid, '', self.now(), *[str(v[1]) for v in kv.values()])
        self.assertTrue(rsp_msg in rs1)
        rs2 = self.scan(self.leader, self.tid, self.pid, scan_kv, self.now(), 1)
        infoLogger.info(rs2)
        self.assertTrue(' ' + scan_value + ' ' in rs2)


if __name__ == "__main__":
    load(TestPut)
