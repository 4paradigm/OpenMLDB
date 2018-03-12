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
sys.path.append(os.getenv('testpath'))
from libs.logger import infoLogger
import libs.conf as conf
import libs.utils as utils


class TestCaseBase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.welcome = 'Welcome to rtidb with version {}\n'.format(os.getenv('rtidbver'))
        cls.testpath = os.getenv('testpath')
        cls.rtidb_path = os.getenv('rtidbpath')
        cls.conf_path = os.getenv('confpath')
        cls.ns_leader = utils.exe_shell('head -n 1 {}/ns_leader'.format(cls.testpath))
        cls.leader, cls.slave1, cls.slave2 = (i[1] for i in conf.tb_endpoints)
        cls.multidimension = conf.multidimension
        cls.multidimension_vk = conf.multidimension_vk
        cls.multidimension_scan_vk = conf.multidimension_scan_vk
        cls.failfast = conf.failfast
        cls.node_path_dict = {cls.leader:cls.testpath + '/tablet1',
                              cls.slave1:cls.testpath + '/tablet2',
                              cls.slave2:cls.testpath + '/tablet3',
                              cls.ns_leader:utils.exe_shell('tail -n 1 {}/ns_leader'.format(cls.testpath))}
        cls.leaderpath = cls.node_path_dict[cls.leader]
        cls.slave1path = cls.node_path_dict[cls.slave1]
        cls.slave2path = cls.node_path_dict[cls.slave2]
        cls.ns_leader_path = cls.node_path_dict[cls.ns_leader]

    def setUp(self):
        infoLogger.info('*** TEST CASE NAME: ' + self._testMethodName)
        try:
            self.tid = int(utils.exe_shell("ls " + self.leaderpath + "/db/|awk -F '_' '{print $1}'|sort -n|tail -1")) + 1
        except Exception, e:
            self.tid = 1
        self.pid = random.randint(10, 100)
        self.clear_ns_table(self.ns_leader)

    def tearDown(self):
        for edp_tuple in conf.tb_endpoints:
            self.start_client(edp_tuple[1])
            self.connectzk(edp_tuple[1])
            self.drop(edp_tuple[1], self.tid, self.pid)
        self.clear_ns_table(self.ns_leader)

    def now(self):
        return int(time.time() * 1000000 / 1000)

    def start_client(self, client, role='tablet'):
        client_path = self.node_path_dict[client]
        if role == 'tablet':
            conf = 'rtidb'
        elif role == 'nameserver':
            conf = 'nameserver'
        else:
            pass
        cmd = '{}/rtidb --flagfile={}/conf/{}.flags'.format(self.testpath, client_path, conf)
        infoLogger.info(cmd)
        args = shlex.split(cmd)
        for _ in range(5):
            rs = utils.exe_shell('lsof -i:{}|grep -v "PID"'.format(client.split(':')[1]))
            if 'rtidb' not in rs:
                time.sleep(2)
                subprocess.Popen(args,stdout=open('{}/info.log'.format(client_path), 'w'),
                                 stderr=open('{}/warning.log'.format(client_path), 'w'))
            else:
                return True
        return False

    def stop_client(self, endpoint):
        cmd = "lsof -i:{}".format(endpoint.split(':')[1]) + "|grep '(LISTEN)'|awk '{print $2}'|xargs kill"
        utils.exe_shell(cmd)

    def run_client(self, endpoint, cmd, role='client'):
        cmd = cmd.strip()
        rs = utils.exe_shell('{} --endpoint={} --role={} --interactive=false --cmd="{}"'.format(
            self.rtidb_path, endpoint, role, cmd))
        return rs.replace(self.welcome, '').replace('>', '')

    def get_table_status(self, endpoint, tid='', pid=''):
        rs = self.run_client(endpoint, 'gettablestatus {} {}'.format(tid, pid))
        infoLogger.info(rs)
        if tid == '' or pid == '':
            return self.parse_table_status(rs)
        else:
            try:
                return self.parse_table_status(rs)[(tid, pid)]
            except KeyError, e:
                infoLogger.error('table {} is not exist!'.format(e))

    @staticmethod
    def parse_table_status(rs):
        table_status_dict = {}
        rs = rs.split('\n')
        for line in rs:
            if not "Welcome" in line and not '------' in line and not '>' in line:
                line_list = line.split(' ')
                status_line_list = [i for i in line_list if i is not '']
                try:
                    if 'tid' not in status_line_list:
                        table_status_dict[int(status_line_list[0]), int(status_line_list[1])] = status_line_list[2:]
                except Exception, e:
                    infoLogger.error(table_status_dict)
        return table_status_dict

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
        return self.run_client(endpoint, 'migrate {} {} {} {}', src, tname, pid_group, des, 'ns_client')

    @staticmethod
    def parse_schema(rs):
        schema_dict = {}
        rs = rs.split('\n')
        for line in rs:
            if not "Welcome" in line and not '------' in line and not '>' in line:
                line_list = line.split(' ')
                schema_line_list = [i for i in line_list if i is not '']
                try:
                    if '#' not in schema_line_list:
                        schema_dict[schema_line_list[1]] = schema_line_list[2:]
                except Exception, e:
                    infoLogger.error(e)
        return schema_dict

    def showschema(self, endpoint, tid='', pid=''):
        try:
            rs = self.run_client(endpoint, 'showschema {} {}'.format(tid, pid))
            return self.parse_schema(rs)
        except KeyError, e:
            infoLogger.error('table {} is not exist!'.format(e))

    @staticmethod
    def parse_tablet(rs):
        tablet_dict = {}
        rs = rs.split('\n')
        for line in rs:
            if not "endpoint" in line and not '------' in line and not '>' in line:
                line_list = line.split(' ')
                schema_line_list = [i for i in line_list if i is not '']
                try:
                    tablet_dict[schema_line_list[0]] = schema_line_list[1:]
                except Exception, e:
                    infoLogger.error(e)
        return tablet_dict

    def showtablet(self, endpoint):
        rs = self.run_client(endpoint, 'showtablet', 'ns_client')
        return self.parse_tablet(rs)

    @staticmethod
    def parse_opstatus(rs):
        tablet_dict = {}
        rs = rs.split('\n')
        for line in rs:
            if not "op_id" in line and not '------' in line and not '>' in line:
                line_list = line.split(' ')
                schema_line_list = [i for i in line_list if i is not '']
                try:
                    tablet_dict[int(schema_line_list[0])] = schema_line_list[1:]
                except Exception, e:
                    infoLogger.error(e)
        return tablet_dict

    def showopstatus(self, endpoint):
        rs = self.run_client(endpoint, 'showopstatus', 'ns_client')
        return self.parse_opstatus(rs)

    @staticmethod
    def parse_table(rs):
        tablet_dict = {}
        rs = rs.split('\n')
        for line in rs:
            if not "endpoint" in line and not '------' in line and not '>' in line:
                line_list = line.split(' ')
                schema_line_list = [i for i in line_list if i is not '']
                try:
                    tablet_dict[tuple(schema_line_list[0:4])] = schema_line_list[4:]
                except Exception, e:
                    infoLogger.error(e)
        return tablet_dict

    def showtable(self, endpoint):
        rs = self.run_client(endpoint, 'showtable', 'ns_client')
        return self.parse_table(rs)


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

    def clear_ns_table(self, endpoint):
        table_dict = self.showtable(endpoint)
        tname_tids = table_dict.keys()
        tnames = set([i[0] for i in tname_tids])
        for tname in tnames:
            self.ns_drop(endpoint, tname)

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
