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
        cls.ns_leader = utils.exe_shell('cat {}/ns_leader'.format(cls.testpath))
        cls.leader, cls.slave1, cls.slave2 = (i[1] for i in conf.tb_endpoints)
        cls.leaderpath = cls.testpath + '/tablet1'
        cls.slave1path = cls.testpath + '/tablet2'
        cls.slave2path = cls.testpath + '/tablet3'
        cls.multidimension = conf.multidimension
        cls.multidimension_vk = conf.multidimension_vk
        cls.multidimension_scan_vk = conf.multidimension_scan_vk
        cls.failfast = conf.failfast

    def setUp(self):
        infoLogger.info('*** TEST CASE NAME: ' + self._testMethodName)
        try:
            self.tid = int(utils.exe_shell("ls " + self.leaderpath + "/db/|awk -F '_' '{print $1}'|sort -n|tail -1")) + 1
        except Exception, e:
            self.tid = 1
        self.pid = random.randint(10, 100)

    def tearDown(self):
        self.drop(self.leader, self.tid, self.pid)
        self.drop(self.slave1, self.tid, self.pid)
        self.drop(self.slave2, self.tid, self.pid)

    def now(self):
        return int(1000 * time.time())

    def start_client(self, client_path):
        cmd = '{}/rtidb --flagfile={}/conf/rtidb.flags'.format(self.testpath, client_path)
        infoLogger.info(cmd)
        args = shlex.split(cmd)
        p = subprocess.Popen(args, stdout=open('{}/log.log'.format(client_path), 'w'))
        infoLogger.info(p)
        time.sleep(3)

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
        if tid == '':
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
                    infoLogger.error(cmd)
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

    def drop(self, endpoint, tid, pid):
        return self.run_client(endpoint, 'drop {} {}'.format(tid, pid))

    def makesnapshot(self, endpoint, tid, pid, role='client'):
        rs = self.run_client(endpoint, 'makesnapshot {} {}'.format(tid, pid), role)
        time.sleep(2)
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

    @staticmethod
    def parse_sechema(rs):
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
        rs = self.run_client(endpoint, 'showschema {} {}'.format(tid, pid))
        if tid == '':
            return self.parse_schema(rs)
        else:
            try:
                return self.parse_sechma(rs)[(tid, pid)]
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
                    tablet_dict[schema_line_list[0]] = schema_line_list[1:]
                except Exception, e:
                    infoLogger.error(e)
        return tablet_dict

    def showtable(self, endpoint):
        rs = self.run_client(endpoint, 'showtable', 'ns_client')
        return self.parse_table(rs)

    def cp_db(self, from_node, to_node, tid, pid):
        utils.exe_shell('cp -r {from_node}/db/{tid}_{pid} {to_node}/db/'.format(
            from_node=from_node, tid=tid, pid=pid, to_node=to_node))

    def put_large_datas(self, data_count, thread_count):
        count = data_count

        def put():
            for i in range(0, count):
                self.put(self.leader, self.tid, self.pid, 'testkey', self.now() - i, 'testvalue'*100)
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
