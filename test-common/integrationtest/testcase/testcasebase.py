# -*- coding: utf-8 -*-
import unittest
import commands
import random
import os
import time
import sys
import threading
import shlex
import subprocess
import collections
sys.path.append(os.getenv('testpath'))
from libs.logger import infoLogger
import libs.conf as conf
import libs.utils as utils
from libs.clients.ns_cluster import NsCluster
import traceback


class TestCaseBase(unittest.TestCase):
    @staticmethod
    def skip(msg):
        return unittest.skip(msg)


    @classmethod
    def setUpClass(cls):
        infoLogger.info('\n' + '|' * 50 + ' TEST {} STARTED '.format(cls) + '|' * 50 + '\n')
        cls.welcome = 'Welcome to rtidb with version {}\n'.format(os.getenv('rtidbver'))
        cls.testpath = os.getenv('testpath')
        cls.rtidb_path = os.getenv('rtidbpath')
        cls.conf_path = os.getenv('confpath')
        cls.ns_leader = utils.exe_shell('head -n 1 {}/ns_leader'.format(cls.testpath))
        cls.ns_leader_path = utils.exe_shell('tail -n 1 {}/ns_leader'.format(cls.testpath))
        cls.ns_slaver = [i[1] for i in conf.ns_endpoints if i[1] != cls.ns_leader][0]
        cls.leader, cls.slave1, cls.slave2 = (i[1] for i in conf.tb_endpoints)
        cls.multidimension = conf.multidimension
        cls.multidimension_vk = conf.multidimension_vk
        cls.multidimension_scan_vk = conf.multidimension_scan_vk
        cls.failfast = conf.failfast
        cls.ns_path_dict = {conf.ns_endpoints[0][1]: cls.testpath + '/ns1',
                            conf.ns_endpoints[1][1]: cls.testpath + '/ns2'}
        cls.node_path_dict = {cls.leader: cls.testpath + '/tablet1',
                              cls.slave1: cls.testpath + '/tablet2',
                              cls.slave2: cls.testpath + '/tablet3',
                              cls.ns_leader: cls.ns_path_dict[cls.ns_leader],
                              cls.ns_slaver: cls.ns_path_dict[cls.ns_slaver]}
        cls.leaderpath = cls.node_path_dict[cls.leader]
        cls.slave1path = cls.node_path_dict[cls.slave1]
        cls.slave2path = cls.node_path_dict[cls.slave2]
        infoLogger.info('*'*88)
        infoLogger.info([i[1] for i in conf.ns_endpoints]) 
        infoLogger.info(cls.ns_slaver)

    @classmethod
    def tearDownClass(cls):
        for edp_tuple in conf.tb_endpoints:
            edp = edp_tuple[1]
            utils.exe_shell('rm -rf {}/recycle/*'.format(cls.node_path_dict[edp]))
            utils.exe_shell('rm -rf {}/db/*'.format(cls.node_path_dict[edp]))
        infoLogger.info('\n' + '=' * 50 + ' TEST {} FINISHED '.format(cls) + '=' * 50 + '\n' * 5)

    def setUp(self):
        infoLogger.info('\nTEST CASE NAME: {} {} {}'.format(
            self, self._testMethodDoc, '\n' + '|' * 50 + ' SETUP STARTED ' + '|' * 50 + '\n'))
        try:
            self.ns_leader = utils.exe_shell('head -n 1 {}/ns_leader'.format(self.testpath))
            self.ns_leader_path = utils.exe_shell('tail -n 1 {}/ns_leader'.format(self.testpath))
            self.tid = random.randint(1, 1000)
            self.pid = random.randint(1, 1000)
            self.clear_ns_table(self.ns_leader)
            for edp_tuple in conf.tb_endpoints:
                edp = edp_tuple[1]
                self.clear_tb_table(edp)
            self.confset(self.ns_leader, 'auto_failover', 'true')
            self.confset(self.ns_leader, 'auto_recover_table', 'true')
            self.clear_lock(self.leader)
            self.clear_lock(self.slave1)
            self.clear_lock(self.slave2)
        except Exception as e:
            traceback.print_exc(file=sys.stdout)
        infoLogger.info('\n\n' + '=' * 50 + ' SETUP FINISHED ' + '=' * 50 + '\n')

    def tearDown(self):
        infoLogger.info('\n\n' + '|' * 50 + ' TEARDOWN STARTED ' + '|' * 50 + '\n')
        try:
            self.confset(self.ns_leader, 'auto_failover', 'true')
            self.confset(self.ns_leader, 'auto_recover_table', 'true')
            self.clear_ns_table(self.ns_leader)
            rs = self.showtablet(self.ns_leader)

            for edp_tuple in conf.tb_endpoints:
                edp = edp_tuple[1]
                if rs[edp][0] != 'kTabletHealthy':
                    infoLogger.info("Endpoint offline !!!! " * 10 + edp)
                    self.stop_client(edp)
                    time.sleep(1)
                    self.start_client(edp)
                    time.sleep(10)
                    self.recoverendpoint(self.ns_leader, edp)
                    time.sleep(3)
        except Exception as e:
            traceback.print_exc(file=sys.stdout)
        infoLogger.info('\n\n' + '=' * 50 + ' TEARDOWN FINISHED ' + '=' * 50 + '\n' * 5)

    def now(self):
        return int(time.time() * 1000000 / 1000)

    def start_client(self, endpoint, role='tablet'):
        client_path = self.node_path_dict[endpoint]
        if role == 'tablet':
            conf = 'rtidb'
        elif role == 'nameserver':
            conf = 'nameserver'
        else:
            pass
        cmd = '{}/rtidb --flagfile={}/conf/{}.flags'.format(self.testpath, client_path, conf)
        infoLogger.info(cmd)
        args = shlex.split(cmd)
        need_start = False
        for _ in range(10):
            rs = utils.exe_shell('lsof -i:{}|grep -v "PID"'.format(endpoint.split(':')[1]))
            if 'rtidb' not in rs:
                need_start = True
                time.sleep(1)
                subprocess.Popen(args, stdout=open('{}/info.log'.format(client_path), 'a'),
                                 stderr=open('{}/warning.log'.format(client_path), 'a'))
            else:
                return True, need_start
        return False, need_start

    def stop_client(self, endpoint):
        port = endpoint.split(':')[1]
        cmd = "lsof -i:{}".format(port) + "|grep '(LISTEN)'|awk '{print $2}'|xargs kill"
        utils.exe_shell(cmd)
        rs = utils.exe_shell('lsof -i:{}|grep -v "PID"'.format(port))
        if 'CLOSE_WAIT' in rs:
            infoLogger.error('Kill failed because of CLOSE_WAIT !!!!!!!!!!!!!!!!')
            cmd = "lsof -i:{}".format(port) + "|grep '(CLOSE_WAIT)'|awk '{print $2}'|xargs kill -9"
            utils.exe_shell(cmd)

    def clear_lock(self, endpoint):
        infoLogger.info('\n ' + 'clear lock ' + endpoint + '\n')
        utils.exe_shell("sh {}/bin/zkCli.sh -server {} delete /onebox/offline_endpoint_lock/{}".format(os.getenv('zkpath'), conf.zk_endpoint, endpoint))

    def get_new_ns_leader(self):
        nsc = NsCluster(conf.zk_endpoint, *(i[1] for i in conf.ns_endpoints))
        nsc.get_ns_leader()
        infoLogger.info([x[1] for x in conf.ns_endpoints])
        nss = [x[1] for x in conf.ns_endpoints]
        self.ns_leader = utils.exe_shell('head -n 1 {}/ns_leader'.format(self.testpath))
        self.ns_leader_path = utils.exe_shell('tail -n 1 {}/ns_leader'.format(self.testpath))
        self.node_path_dict[self.ns_leader] = utils.exe_shell('tail -n 1 {}/ns_leader'.format(self.testpath))
        nss.remove(self.ns_leader)
        self.ns_slaver = nss[0]
        infoLogger.info("*" * 88)
        infoLogger.info("ns_leader: " + self.ns_leader)
        infoLogger.info("ns_slaver: " + self.ns_slaver)
        infoLogger.info("*" * 88)

    def run_client(self, endpoint, cmd, role='client'):
        cmd = cmd.strip()
        rs = utils.exe_shell('{} --endpoint={} --role={} --interactive=false --cmd="{}"'.format(
            self.rtidb_path, endpoint, role, cmd))
        return rs.replace(self.welcome, '').replace('>', '')

    @staticmethod
    def get_manifest(nodepath, tid, pid):
        manifest_dict = {}
        with open('{}/db/{}_{}/snapshot/MANIFEST'.format(nodepath, tid, pid)) as f:
            for l in f:
                if 'offset: ' in l:
                    manifest_dict['offset'] = l.split(':')[1].strip()
                elif 'name: ' in l:
                    manifest_dict['name'] = l.split(':')[1][2:-2].strip()
                elif 'count: ' in l:
                    manifest_dict['count'] = l.split(':')[1].strip()
                elif 'term: ' in l:
                    manifest_dict['term'] = l.split(':')[1].strip()
        return manifest_dict

    def create(self, endpoint, tname, tid, pid, ttl=144000, segment=8, isleader='true', *slave_endpoints, **schema):
        if not schema:
            if self.multidimension:
                infoLogger.debug('create with default multi dimension')
                cmd = 'screate'
                schema = {k: v[0] for k, v in self.multidimension_vk.items()}  # default schema
            else:
                cmd = 'create'
        else:
            cmd = 'screate'
        return self.run_client(endpoint, '{} {} {} {} {} {} {} {} {}'.format(
            cmd, tname, tid, pid, ttl, segment, isleader, ' '.join(slave_endpoints),
            ' '.join(['{}:{}'.format(k, v) for k, v in schema.items() if k != ''])))

    def ns_create(self, endpoint, metadata_path):
        return self.run_client(endpoint, 'create ' + metadata_path, 'ns_client')

    def ns_create_cmd(self, endpoint, name, ttl, partition_num, replica_num, schema):
        cmd = 'create ' + name + ' ' + ttl + ' ' + partition_num + ' ' + replica_num + ' ' + schema
        return self.run_client(endpoint, cmd, 'ns_client')

    def ns_drop(self, endpoint, tname):
        return self.run_client(endpoint, 'drop {}'.format(tname), 'ns_client')

    def put(self, endpoint, tid, pid, key, ts, *values):
        if len(values) == 1:
            if self.multidimension and key is not '':
                infoLogger.debug('put with default multi dimension')
                default_values = [str(v[1]) for v in self.multidimension_vk.values()]
                return self.run_client(endpoint, 'sput {} {} {} {}'.format(
                    tid, pid, ts, ' '.join(default_values)))
            elif not self.multidimension and key is not '':
                return self.run_client(endpoint, 'put {} {} {} {} {}'.format(
                    tid, pid, key, ts, values[0]))
        return self.run_client(endpoint, 'sput {} {} {} {}'.format(
            tid, pid, ts, ' '.join(values)))

    def scan(self, endpoint, tid, pid, vk, ts_from, ts_to):
        """

        :param endpoint:
        :param tid:
        :param pid:
        :param vk: e.g. {'card': 0001, 'merchant': 0002} or 'naysakey'
        :param ts_from:
        :param ts_to:
        :return:
        """
        if not isinstance(vk, dict):
            if self.multidimension:
                infoLogger.debug('scan with default multi dimension')
                default_vk = self.multidimension_scan_vk
                value_key = ['{} {}'.format(v, k) for k, v in default_vk.items()]
                return self.run_client(endpoint, 'sscan {} {} {} {} {}'.format(
                    tid, pid, ' '.join(value_key), ts_from, ts_to))
            else:
                return self.run_client(endpoint, 'scan {} {} {} {} {}'.format(
                    tid, pid, vk, ts_from, ts_to))
        else:
            value_key = ['{} {}'.format(v, k) for k, v in vk.items()]
            return self.run_client(endpoint, 'sscan {} {} {} {} {}'.format(
                tid, pid, ' '.join(value_key), ts_from, ts_to))

    def get(self, endpoint, tid, pid, vk, ts):
        """

        :param endpoint:
        :param tid:
        :param pid:
        :param vk: e.g. {'card': 0001, 'merchant': 0002} or 'naysakey'
        :param ts:
        :return:
        """
        if self.multidimension:
            print(self.multidimension_scan_vk.keys()[0])
            print(self.multidimension_scan_vk.values()[0])
            return self.run_client(endpoint, 'sget {} {} {} {} {}'.format(
                tid, pid, self.multidimension_scan_vk.values()[0], self.multidimension_scan_vk.keys()[0], ts))
        else:
            return self.run_client(endpoint, 'get {} {} {} {}'.format(
                tid, pid, vk, ts))

    def drop(self, endpoint, tid, pid):
        return self.run_client(endpoint, 'drop {} {}'.format(tid, pid))

    def makesnapshot(self, endpoint, tid_or_tname, pid, role='client', wait=2):
        rs = self.run_client(endpoint, 'makesnapshot {} {}'.format(tid_or_tname, pid), role)
        time.sleep(wait)
        return rs

    def pausesnapshot(self, endpoint, tid, pid):
        rs = self.run_client(endpoint, 'pausesnapshot {} {}'.format(tid, pid))
        time.sleep(1)
        return rs

    def recoversnapshot(self, endpoint, tid, pid):
        rs = self.run_client(endpoint, 'recoversnapshot {} {}'.format(tid, pid))
        time.sleep(1)
        return rs

    def addreplica(self, endpoint, tid, pid, role='client', *slave_endpoints):
        rs = self.run_client(endpoint, 'addreplica {} {} {}'.format(tid, pid, ' '.join(slave_endpoints)), role)
        time.sleep(1)
        return rs

    def delreplica(self, endpoint, tid, pid, role='client', *slave_endpoints):
        rs = self.run_client(endpoint, 'delreplica {} {} {}'.format(tid, pid, ' '.join(slave_endpoints)), role)
        time.sleep(1)
        return rs

    def loadtable(self, endpoint, tname, tid, pid, ttl=144000, segment=8, isleader='false', *slave_endpoints):
        rs = self.run_client(endpoint, 'loadtable {} {} {} {} {} {} {}'.format(
            tname, tid, pid, ttl, segment, isleader, ' '.join(slave_endpoints)))
        time.sleep(2)
        return rs

    def changerole(self, endpoint, tid, pid, role):
        return self.run_client(endpoint, 'changerole {} {} {}'.format(tid, pid, role))

    def sendsnapshot(self, endpoint, tid, pid, slave_endpoint):
        return self.run_client(endpoint, 'sendsnapshot {} {} {}'.format(tid, pid, slave_endpoint))

    def setexpire(self, endpoint, tid, pid, ttl):
        return self.run_client(endpoint, 'setexpire {} {} {}'.format(tid, pid, ttl))

    def confset(self, endpoint, conf, value):
        return self.run_client(endpoint, 'confset {} {}'.format(conf, value), 'ns_client')

    def confget(self, endpoint, conf):
        return self.run_client(endpoint, 'confget {}'.format(conf), 'ns_client')

    def offlineendpoint(self, endpoint, offline_endpoint):
        return self.run_client(endpoint, 'offlineendpoint {}'.format(offline_endpoint), 'ns_client')

    def recoverendpoint(self, endpoint, offline_endpoint):
        return self.run_client(endpoint, 'recoverendpoint {}'.format(offline_endpoint), 'ns_client')

    def changeleader(self, endpoint, tname, pid):
        return self.run_client(endpoint, 'changeleader {} {}'.format(tname, pid), 'ns_client')

    def connectzk(self, endpoint, role='client'):
        return self.run_client(endpoint, 'connectzk', role)

    def disconnectzk(self, endpoint, role='client'):
        return self.run_client(endpoint, 'disconnectzk', role)

    def migrate(self, endpoint, src, tname, pid_group, des):
        return self.run_client(endpoint, 'migrate {} {} {} {}'.format(src, tname, pid_group, des), 'ns_client')

    @staticmethod
    def parse_tb(rs, splitor, key_cols_index, value_cols_index):
        """
        parse table format response msg
        :param rs:
        :param splitor:
        :param key_cols_index:
        :param value_cols_index:
        :return:
        """
        table_dict = {}
        rs_tb = rs.split('\n')
        real_conent_flag = False
        for line in rs_tb:
            if '------' in line:
                real_conent_flag = True
                continue
            if real_conent_flag:
                elements = line.split(splitor)
                elements = [x for x in elements if x != '']
                try:
                    k = [elements[x] for x in key_cols_index]
                    v = [elements[x] for x in value_cols_index]
                    if len(key_cols_index) <= 1:
                        key = k[0]
                    else:
                        key = tuple(k)
                    table_dict[key] = v
                except Exception as e:
                    traceback.print_exc(file=sys.stdout)
                    infoLogger.error(e)
        if real_conent_flag is False:
            return rs
        return table_dict

    def get_table_status(self, endpoint, tid='', pid=''):
        try:
            rs = self.run_client(endpoint, 'gettablestatus {} {}'.format(tid, pid))
            tablestatus = self.parse_tb(rs, ' ', [0, 1], [2, 3, 4, 5, 6, 7, 8])
            tableststus_d = {(int(k[0]), int(k[1])): v for k, v in tablestatus.items()}
            if tid != '':
                return tableststus_d[(int(tid), int(pid))]
            else:
                return tableststus_d
        except KeyError, e:
            traceback.print_exc(file=sys.stdout)
            infoLogger.error('table {} is not exist!'.format(e))

    def showschema(self, endpoint, tid='', pid=''):
        try:
            rs = self.run_client(endpoint, 'showschema {} {}'.format(tid, pid))
            return self.parse_tb(rs, ' ', [1], [2, 3])
        except KeyError, e:
            traceback.print_exc(file=sys.stdout)
            infoLogger.error('table {} is not exist!'.format(e))

    def showtablet(self, endpoint):
        rs = self.run_client(endpoint, 'showtablet', 'ns_client')
        return self.parse_tb(rs, ' ', [0], [1, 2])

    def showopstatus(self, endpoint):
        rs = self.run_client(endpoint, 'showopstatus', 'ns_client')
        tablestatus = self.parse_tb(rs, ' ', [0], [1, 4, 8])
        tablestatus_d = {(int(k)): v for k, v in tablestatus.items()}
        return tablestatus_d

    def showtable(self, endpoint):
        rs = self.run_client(endpoint, 'showtable', 'ns_client')
        return self.parse_tb(rs, ' ', [0, 1, 2, 3], [4, 5, 6, 7])

    @staticmethod
    def get_table_meta(nodepath, tid, pid):
        table_meta = {}
        with open('{}/db/{}_{}/table_meta.txt'.format(nodepath, tid, pid)) as f:
            for l in f:
                k = l.split(":")[0]
                v = l[:-1].split(":")[1].strip()
                if k in table_meta:
                    v += '|' + table_meta[k]
                table_meta[k] = v
        return table_meta

    def clear_tb_table(self, endpoint):
        table_dict = self.get_table_status(endpoint)
        if isinstance(table_dict, dict):
            for tid_pid in table_dict:
                self.drop(endpoint, tid_pid[0], tid_pid[1])
        else:
            infoLogger.info('gettablestatus empty.')

    def clear_ns_table(self, endpoint):
        table_dict = self.showtable(endpoint)
        if isinstance(table_dict, dict):
            tname_tids = table_dict.keys()
            tnames = set([i[0] for i in tname_tids])
            for tname in tnames:
                self.ns_drop(endpoint, tname)
        else:
            infoLogger.info('showtable empty.')

    def cp_db(self, from_node, to_node, tid, pid):
        utils.exe_shell('cp -r {from_node}/db/{tid}_{pid} {to_node}/db/'.format(
            from_node=from_node, tid=tid, pid=pid, to_node=to_node))

    def put_large_datas(self, data_count, thread_count, data='testvalue' * 200):
        count = data_count

        def put():
            for i in range(0, count):
                self.put(self.leader, self.tid, self.pid, 'testkey', self.now() - i, data)

        threads = []
        for _ in range(0, thread_count):
            threads.append(threading.Thread(
                target=put, args=()))
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    def get_ns_leader(self):
        return utils.exe_shell('cat {}/ns_leader'.format(self.testpath))

    def find_new_tb_leader(self, tname, tid, pid):
        line_count = utils.exe_shell('cat {}/info.log|wc -l'.format(self.ns_leader_path))
        cmd = "grep -a -n {} {}/info.log -m 1".format(tname, self.ns_leader_path)
        op_start_line = utils.exe_shell(cmd + "|awk -F ':' '{print $1}'")
        cmd1 = "tail -n {} {}/info.log|grep -a \"name\[{}\] tid\[{}\] pid\[{}\] offset\[\"".format(
            int(line_count) - int(op_start_line),
            self.ns_leader_path, tname, tid, pid) + \
               "|awk -F 'new leader is\\\\[' '{print $2}'" \
               "|awk -F '\\\\]. name\\\\[tname' '{print $1}'" \
               "|tail -n 1"
        new_leader = utils.exe_shell(cmd1)
        self.new_tb_leader = new_leader
        return new_leader

    @staticmethod
    def update_conf(nodepath, conf_item, conf_value, role='client'):
        conf_file = ''
        if role == 'client':
            conf_file = 'rtidb.flags'
        elif role == 'ns_client':
            conf_file = 'nameserver.flags'
        utils.exe_shell("sed -i '/{}/d' {}/conf/{}".format(conf_item, nodepath, conf_file))
        if conf_value is not None:
            utils.exe_shell("sed -i '1i--{}={}' {}/conf/{}".format(conf_item, conf_value, nodepath, conf_file))

    def get_opid_by_tname_pid(self, tname, pid):
        opid_rs = utils.exe_shell("grep -a {} {}/info.log|grep op_id|grep \"name\[\"|grep \"pid\[{}\]\""
                                  "|sed 's/\(.*\)op_id\[\(.*\)\] name\(.*\)/\\2/g'".format(
            tname, self.ns_leader_path, pid))
        opid_x = opid_rs.split('\n')
        return opid_x

    def get_latest_opid_by_tname_pid(self, tname, pid):
        latest_opid = self.get_opid_by_tname_pid(tname, pid)[-1]
        self.latest_opid = int(latest_opid)
        return self.latest_opid

    def get_op_by_opid(self, op_id):
        rs = self.showopstatus(self.ns_leader)
        return rs[op_id][0]

    def get_task_dict_by_opid(self, tname, opid):
        time.sleep(1)
        task_dict = collections.OrderedDict()
        cmd = "cat {}/info.log |grep -a -A 10000 '{}'|grep -a \"op_id\[{}\]\"|grep task_type".format(
            self.ns_leader_path, tname, opid) \
              + "|awk -F '\\\\[' '{print $4\"]\"$5\"]\"$6}'" \
                "|awk -F '\\\\]' '{print $1\",\"$3\",\"$5}'"
        infoLogger.info(cmd)
        rs = utils.exe_shell(cmd).split('\n')
        infoLogger.info(rs)
        for x in rs:
            x = x.split(',')
            task_dict[(int(x[1]), x[2])] = x[0]
        self.task_dict = task_dict

    def check_tasks(self, op_id, exp_task_list):
        self.get_task_dict_by_opid(self.tname, op_id)
        tasks = [k[1] for k, v in self.task_dict.items() if k[0] == int(op_id) and v == 'kDone']
        infoLogger.info(self.task_dict)
        infoLogger.info(op_id)
        infoLogger.info([k[1] for k, v in self.task_dict.items()])
        infoLogger.info(tasks)
        infoLogger.info(exp_task_list)
        self.assertEqual(exp_task_list, tasks)

    def check_re_add_replica_op(self, op_id):
        self.check_tasks(op_id,
                         ['kPauseSnapshot', 'kSendSnapshot', 'kLoadTable', 'kAddReplica',
                          'kRecoverSnapshot', 'kUpdatePartitionStatus'])

    def check_re_add_replica_no_send_op(self, op_id):
        self.check_tasks(op_id,
                         ['kPauseSnapshot', 'kLoadTable', 'kAddReplica',
                          'kRecoverSnapshot', 'kUpdatePartitionStatus'])

    def check_re_add_replica_with_drop_op(self, op_id):
        self.check_tasks(op_id,
                         ['kPauseSnapshot', 'kDropTable', 'kSendSnapshot', 'kLoadTable', 'kAddReplica',
                          'kRecoverSnapshot', 'kUpdatePartitionStatus'])

    def check_re_add_replica_simplify_op(self, op_id):
        self.check_tasks(op_id,
                         ['kAddReplica', 'kUpdatePartitionStatus'])
