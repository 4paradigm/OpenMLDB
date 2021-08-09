#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright 2021 4Paradigm
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
import copy
import re


class TestCaseBase(unittest.TestCase):
    @staticmethod
    def skip(msg):
        return unittest.skip(msg)


    @classmethod
    def setUpClass(cls):
        infoLogger.info('\n' + '|' * 50 + ' TEST {} STARTED '.format(cls) + '|' * 50 + '\n')
        cls.welcome = 'Welcome to fedb with version {}\n'.format(os.getenv('rtidbver'))
        cls.testpath = os.getenv('testpath')
        cls.rtidb_path = os.getenv('rtidbpath')
        cls.conf_path = os.getenv('confpath')
        cls.data_path = os.getenv('datapath')
        cls.ns_leader = utils.exe_shell('head -n 1 {}/ns_leader'.format(cls.testpath))
        cls.ns_leader_path = utils.exe_shell('tail -n 1 {}/ns_leader'.format(cls.testpath))
        cls.ns_slaver = [i for i in conf.ns_endpoints if i != cls.ns_leader][0]
        cls.leader, cls.slave1, cls.slave2 = (i for i in conf.tb_endpoints)
        cls.multidimension = conf.multidimension
        cls.multidimension_vk = conf.multidimension_vk
        cls.multidimension_scan_vk = conf.multidimension_scan_vk
        cls.failfast = conf.failfast
        cls.ns_path_dict = {conf.ns_endpoints[0]: cls.testpath + '/ns1',
                            conf.ns_endpoints[1]: cls.testpath + '/ns2'}
        cls.node_path_dict = {cls.leader: cls.testpath + '/tablet1',
                              cls.slave1: cls.testpath + '/tablet2',
                              cls.slave2: cls.testpath + '/tablet3',
                              cls.ns_leader: cls.ns_path_dict[cls.ns_leader],
                              cls.ns_slaver: cls.ns_path_dict[cls.ns_slaver]}
        cls.leaderpath = cls.node_path_dict[cls.leader]
        cls.slave1path = cls.node_path_dict[cls.slave1]
        cls.slave2path = cls.node_path_dict[cls.slave2]
        infoLogger.info('*'*88)
        infoLogger.info([i for i in conf.ns_endpoints]) 
        infoLogger.info(cls.ns_slaver)
        infoLogger.info(conf.cluster_mode)

        # remote env 
        cls.ns_leader_r = utils.exe_shell('head -n 1 {}/ns_leader_remote'.format(cls.testpath))
        cls.ns_leader_path_r = utils.exe_shell('tail -n 1 {}/ns_leader_remote'.format(cls.testpath))
        cls.ns_slaver_r = [i for i in conf.ns_endpoints_r if i != cls.ns_leader_r][0]
        cls.leader_r, cls.slave1_r, cls.slave2_r = (i for i in conf.tb_endpoints_r)
        cls.ns_path_dict_r = {conf.ns_endpoints_r[0]: cls.testpath + '/ns1remote',
                            conf.ns_endpoints_r[1]: cls.testpath + '/ns2remote'}
        cls.node_path_dict_r = {cls.leader_r: cls.testpath + '/tablet1remote',
                              cls.slave1_r: cls.testpath + '/tablet2remote',
                              cls.slave2_r: cls.testpath + '/tablet3remote',
                              cls.ns_leader_r: cls.ns_path_dict_r[cls.ns_leader_r],
                              cls.ns_slaver_r: cls.ns_path_dict_r[cls.ns_slaver_r]}
        infoLogger.info('*'*88)
        infoLogger.info([i for i in conf.ns_endpoints_r]) 
        infoLogger.info(cls.ns_slaver_r)

    @classmethod
    def tearDownClass(cls):
        for edp in conf.tb_endpoints:
            utils.exe_shell('rm -rf {}/recycle/*'.format(cls.node_path_dict[edp]))
            utils.exe_shell('rm -rf {}/db/*'.format(cls.node_path_dict[edp]))
        for edp in conf.tb_endpoints_r:
            utils.exe_shell('rm -rf {}/recycle/*'.format(cls.node_path_dict_r[edp]))
            utils.exe_shell('rm -rf {}/db/*'.format(cls.node_path_dict_r[edp]))
        infoLogger.info('\n' + '=' * 50 + ' TEST {} FINISHED '.format(cls) + '=' * 50 + '\n' * 5)
    def setUp(self):
        infoLogger.info('\nTEST CASE NAME: {} {} {}'.format(
            self, self._testMethodDoc, '\n' + '|' * 50 + ' SETUP STARTED ' + '|' * 50 + '\n'))
        try:
            self.ns_leader = utils.exe_shell('head -n 1 {}/ns_leader'.format(self.testpath))
            nss = copy.deepcopy(conf.ns_endpoints)
            nss.remove(self.ns_leader)
            self.ns_slaver = nss[0]
            self.ns_leader_path = utils.exe_shell('tail -n 1 {}/ns_leader'.format(self.testpath))
            self.tid = random.randint(1, 1000)
            self.pid = random.randint(1, 1000)
            if conf.cluster_mode == "cluster":
                self.clear_ns_table(self.ns_leader)
            else:
                for edp in conf.tb_endpoints:
                    self.clear_tb_table(edp)
            #remote env
            self.alias = 'rem'
            self.ns_leader_r = utils.exe_shell('head -n 1 {}/ns_leader_remote'.format(self.testpath))
            nss_r = copy.deepcopy(conf.ns_endpoints_r)
            nss_r.remove(self.ns_leader_r)
            self.ns_slaver_r = nss_r[0]
            self.ns_leader_path_r = utils.exe_shell('tail -n 1 {}/ns_leader_remote'.format(self.testpath))
            self.clear_ns_table(self.ns_leader_r)
            for edp in conf.tb_endpoints_r:
                self.clear_tb_table(edp)
        except Exception as e:
            traceback.print_exc(file=sys.stdout)
        infoLogger.info('\n\n' + '=' * 50 + ' SETUP FINISHED ' + '=' * 50 + '\n')

    def tearDown(self):
        infoLogger.info('\n\n' + '|' * 50 + ' TEARDOWN STARTED ' + '|' * 50 + '\n')
        try:
            rs = self.showtablet(self.ns_leader)
            for edp in conf.tb_endpoints:
                if rs[edp][0] != 'kTabletHealthy':
                    infoLogger.info("Endpoint offline !!!! " * 10 + edp)
                    self.stop_client(edp)
                    time.sleep(1)
                    self.start_client(edp)
                    time.sleep(10)
            if conf.cluster_mode == "cluster":
                self.clear_ns_table(self.ns_leader)

            #remote env
            rs_r = self.showtablet(self.ns_leader_r)
            for edp in conf.tb_endpoints_r:
                if rs_r[edp][0] != 'kTabletHealthy':
                    infoLogger.info("Endpoint offline !!!! " * 10 + edp)
                    self.stop_client(edp)
                    time.sleep(1)
                    self.start_client(edp)
                    time.sleep(10)
            if conf.cluster_mode == "cluster":
                self.clear_ns_table(self.ns_leader_r)
        except Exception as e:
            traceback.print_exc(file=sys.stdout)
        infoLogger.info('\n\n' + '=' * 50 + ' TEARDOWN FINISHED ' + '=' * 50 + '\n' * 5)

    def now(self):
        return int(time.time() * 1000000 / 1000)

    def start_client(self, endpoint, role='tablet'):
        client_path = self.node_path_dict[endpoint]
        if role == 'tablet' or role == 'nameserver':
            conf = role
        else:
            pass
        cmd = '{}/fedb --flagfile={}/conf/{}.flags'.format(self.testpath, client_path, conf)
        infoLogger.info(cmd)
        args = shlex.split(cmd)
        need_start = False
        for _ in range(20):
            rs = utils.exe_shell('lsof -i:{}|grep -v "PID"'.format(endpoint.split(':')[1]))
            if 'fedb' not in rs:
                need_start = True
                time.sleep(1)
                subprocess.Popen(args, stdout=open('{}/info.log'.format(client_path), 'a'),
                                 stderr=open('{}/warning.log'.format(client_path), 'a'))
            else:
                time.sleep(1)
                rs = utils.exe_shell('lsof -i:{}|grep -v "PID"'.format(endpoint.split(':')[1]))
                if 'fedb' in rs:
                    return True, need_start
        return False, need_start

    def stop_client(self, endpoint):
        port = endpoint.split(':')[1]
        cmd = "lsof -i:{}".format(port) + "|grep '(LISTEN)'|awk '{print $2}'|xargs kill -9"
        utils.exe_shell(cmd)
        rs = utils.exe_shell('lsof -i:{}|grep -v "PID"'.format(port))
        if 'CLOSE_WAIT' in rs:
            time.sleep(2)
            #infoLogger.error('Kill failed because of CLOSE_WAIT !!!!!!!!!!!!!!!!')
            #cmd = "lsof -i:{}".format(port) + "|grep '(CLOSE_WAIT)'|awk '{print $2}'|xargs kill -9"
            #utils.exe_shell(cmd)

    def get_new_ns_leader(self):
        nsc = NsCluster(conf.zk_endpoint, *(i for i in conf.ns_endpoints))
        nsc.get_ns_leader()
        infoLogger.info(conf.ns_endpoints)
        nss = copy.deepcopy(conf.ns_endpoints)
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
        rs = utils.exe_shell('{} --endpoint={} --role={} --interactive=false --request_timeout_ms=200000 --cmd="{}"'.format(
            self.rtidb_path, endpoint, role, cmd))
        return rs.replace(self.welcome, '').replace('>', '')

    @staticmethod
    def get_manifest_by_realpath(realpath, tid, pid):
        manifest_dict = {}
        with open('{}/{}_{}/snapshot/MANIFEST'.format(realpath, tid, pid)) as f:
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

    @staticmethod
    def get_manifest(nodepath, tid, pid):
        realpath = nodepath + "/db"
        return TestCaseBase.get_manifest_by_realpath(realpath, tid, pid)

    @staticmethod
    def get_table_meta_no_db(nodepath, tid, pid):
        table_meta_dict = {}
        with open('{}/{}_{}/table_meta.txt'.format(nodepath, tid, pid)) as f:
            for l in f:
                if 'tid: ' in l:
                    table_meta_dict['tid'] = l.split(':')[1].strip()
                elif 'name: ' in l:
                    table_meta_dict['name'] = l.split(':')[1][2:-2].strip()
                elif 'compress_type: ' in l:
                    table_meta_dict['compress_type'] = l.split(':')[1].strip()
                elif 'key_entry_max_height: ' in l:
                    table_meta_dict['key_entry_max_height'] = l.split(':')[1].strip()
        return table_meta_dict

    @staticmethod
    def get_table_meta(nodepath, tid, pid):
        table_meta_dict = {}
        with open('{}/db/{}_{}/table_meta.txt'.format(nodepath, tid, pid)) as f:
            for l in f:
                if 'tid: ' in l:
                    table_meta_dict['tid'] = l.split(':')[1].strip()
                elif 'name: ' in l:
                    table_meta_dict['name'] = l.split(':')[1][2:-2].strip()
                elif 'compress_type: ' in l:
                    table_meta_dict['compress_type'] = l.split(':')[1].strip()
                elif 'key_entry_max_height: ' in l:
                    table_meta_dict['key_entry_max_height'] = l.split(':')[1].strip()
        return table_meta_dict

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

    def execute_gc(self, endpoint, tid, pid):
        cmd = "curl -d \'{\"tid\":%s, \"pid\":%s}\'  http://%s/TabletServer/ExecuteGc" % (tid, pid, endpoint)
        utils.exe_shell(cmd)
        time.sleep(2)

    def ns_switch_mode(self, endpoint, mode):
        cmd = 'switchmode ' + mode
        return self.run_client(endpoint, cmd, 'ns_client')

    def add_replica_cluster(self, endpoint, zk_endpoints, zk_root_path, alias):
        cmd = 'addrepcluster {} {} {}'.format(zk_endpoints, zk_root_path, alias)
        return self.run_client(endpoint, cmd, 'ns_client')
    
    def remove_replica_cluster(self, endpoint, alias):
        cmd = 'removerepcluster ' + alias
        return self.run_client(endpoint, cmd, 'ns_client')

    def ns_create(self, endpoint, metadata_path):
        return self.run_client(endpoint, 'create ' + metadata_path, 'ns_client')

    def ns_create_cmd(self, endpoint, name, ttl, partition_num, replica_num, schema = ''):
        cmd = 'create {} {} {} {} {}'.format(name, ttl, partition_num, replica_num, schema)
        return self.run_client(endpoint, cmd, 'ns_client')

    def ns_add_table_field(self, endpoint, name ,col_name, col_type):
        cmd = 'addtablefield {} {} {}'.format(name, col_name, col_type);
        return self.run_client(endpoint, cmd, 'ns_client')

    def ns_addindex(self, endpoint, name, index_name, col_name='', ts_name=''):
        cmd = 'addindex {} {} {} {}'.format(name, index_name, col_name, ts_name)
        return self.run_client(endpoint, cmd, 'ns_client')

    def parse_scan_result(self, result):    
        arr = result.split("\n")
        key_arr = re.sub(' +', ' ', arr[0]).strip().split(" ")
        value = []
        for i in range(2, len(arr)):
            record = re.sub(' +', ' ', arr[i]).strip().split(" ")
            cur_map = {}
            for idx in range(len(key_arr)):
                cur_map[key_arr[idx]] = record[idx]
            value.append(cur_map)    
        return value

    def ns_preview(self, endpoint, name, limit = ''):
        cmd = 'preview {} {}'.format(name, limit)
        result = self.run_client(endpoint, cmd, 'ns_client')
        return self.parse_scan_result(result)

    def ns_count(self, endpoint, name, key, idx_name, filter_expired_data = 'false'):
        cmd = 'count {} {} {} {}'.format(name, key, idx_name, filter_expired_data)
        result = self.run_client(endpoint, cmd, 'ns_client')
        infoLogger.debug(result)
        return result

    def ns_count_with_pair(self, endpoint, name, key, idx_name, ts_name, filter_expired_data = 'false'):
        cmd = 'count {} {} {} {} {}'.format('table_name='+name, 'key='+key, 'index_name='+idx_name, 'ts_name='+ts_name, 'filter_expired_data='+filter_expired_data)
        result = self.run_client(endpoint, cmd, 'ns_client')
        infoLogger.debug(result)
        return result

    def ns_scan_kv(self, endpoint, name, pk, start_time, end_time, limit = ''):
        cmd = 'scan {} {} {} {} {}'.format(name, pk, start_time, end_time, limit)
        result = self.run_client(endpoint, cmd, 'ns_client')
        return self.parse_scan_result(result)

    def ns_scan_multi(self, endpoint, name, pk, idx_name, start_time, end_time, limit = ''):
        cmd = 'scan {} {} {} {} {} {}'.format(name, pk, idx_name, start_time, end_time, limit)
        result = self.run_client(endpoint, cmd, 'ns_client')
        return self.parse_scan_result(result)

    def ns_scan_multi_with_pair(self, endpoint, name, pk, idx_name, start_time, end_time, ts_name, limit = '0', atleast = '0'):
        cmd = 'scan {} {} {} {} {} {} {} {}'.format('table_name='+name, 'key='+pk, 'index_name='+idx_name, 'st='+start_time, 'et='+end_time, 'ts_name='+ts_name, 'limit='+limit, 'atleast='+atleast)
        result = self.run_client(endpoint, cmd, 'ns_client')
        return self.parse_scan_result(result)

    def ns_delete(self, endpoint, name, key, idx_name = ''):
        cmd = 'delete {} {} {}'.format(name, key, idx_name);
        return self.run_client(endpoint, cmd, 'ns_client')

    def ns_info(self, endpoint, name):
        cmd = 'info {}'.format(name);
        result = self.run_client(endpoint, cmd, 'ns_client')
        lines = result.split("\n")
        kv = {}
        for line_num in xrange(2, len(lines)):
            arr = lines[line_num].strip().split(" ")
            key = arr[0]
            value = lines[line_num].strip().lstrip(key).strip()
            kv[key] = value
        return kv

    def ns_get_kv(self, endpoint, name, key, ts):
        cmd = 'get ' + name + ' ' + key+ ' ' + ts
        return self.run_client(endpoint, cmd, 'ns_client')

    def ns_get_multi(self, endpoint, name, key, idx_name, ts):
        cmd = 'get {} {} {} {}'.format(name, key, idx_name, ts)
        result = self.run_client(endpoint, cmd, 'ns_client')
        arr = result.split("\n")
        key_arr = re.sub(' +', ' ', arr[0]).replace("# ts", "").strip().split(" ")
        value = {}
        record = re.sub(' +', ' ', arr[2]).strip().split(" ")
        for idx in range(len(key_arr)):
            value[key_arr[idx]] = record[idx+2]
        return value

    def ns_get_multi_with_pair(self, endpoint, name, key, idx_name, ts, ts_name):
        cmd = 'get {} {} {} {} {}'.format('table_name='+name,'key='+ key, 'index_name='+idx_name,'ts='+ts,'ts_name='+ts_name)
        result = self.run_client(endpoint, cmd, 'ns_client')
        value = {}
        if result.find("Fail to get value") != -1:
            return value
        arr = result.split("\n")
        key_arr = re.sub(' +', ' ', arr[0]).replace("# ts", "").strip().split(" ")
        record = re.sub(' +', ' ', arr[2]).strip().split(" ")
        for idx in range(len(key_arr)):
            value[key_arr[idx]] = record[idx+2]
        return value

    def ns_put_kv(self, endpoint, name, pk, ts, value):
        cmd = 'put {} {} {} {}'.format(name, pk, ts, value)
        return self.run_client(endpoint, cmd, 'ns_client')

    def ns_put_multi(self, endpoint, name, ts, row):
        cmd = 'put {} {} {}'.format(name, ts, ' '.join(row))
        return self.run_client(endpoint, cmd, 'ns_client')
    
    def ns_put_multi_with_pair(self, endpoint, name, row):
        cmd = 'put {} {}'.format(name, ' '.join(row))
        return self.run_client(endpoint, cmd, 'ns_client')

    def ns_query(self, endpoint, name, row):
        cmd = 'query {} {}'.format('table_name=' + name, row)
        result = self.run_client(endpoint, cmd, 'ns_client')
        return self.parse_scan_result(result)

    def ns_update(self, endpoint, name, row):
        cmd = 'update {} {}'.format('table_name=' + name, row)
        return self.run_client(endpoint, cmd, 'ns_client')

    def ns_drop(self, endpoint, tname):
        infoLogger.debug(tname)
        return self.run_client(endpoint, 'drop {}'.format(tname), 'ns_client')

    def ns_update_table_alive_cmd(self, ns_endpoint, updatetablealive, table_name, pid, endpoint, is_alive):
        cmd = '{} {} {} {} {}'.format(updatetablealive, table_name, pid, endpoint, is_alive)
        return self.run_client(ns_endpoint, cmd, 'ns_client')

    def ns_synctable(self, ns_endpoint, table_name, alias, pid = ''):
        cmd = '{} {} {} {} '.format('synctable', table_name, alias, pid)
        return self.run_client(ns_endpoint, cmd, 'ns_client')

    def ns_recover_table_cmd(self, ns_endpoint, recovertable, table_name, pid, endpoint):
        cmd = '{} {} {} {}'.format(recovertable, table_name, pid, endpoint)
        return self.run_client(ns_endpoint, cmd, 'ns_client')

    def ns_addreplica(self, ns_endpoint, name, pid, replica_endpoint):
        cmd = 'addreplica {} {} {}'.format(name, pid, replica_endpoint)
        return self.run_client(ns_endpoint, cmd, 'ns_client')

    def ns_gettablepartition(self, ns_endpoint, gettablepartition, name, pid):
        cmd = '{} {} {}'.format(gettablepartition, name, pid)
        return self.run_client(ns_endpoint, cmd, 'ns_client')

    def ns_showns(self, ns_endpoint, showns):
        cmd = '{}'.format(showns)
        return self.run_client(ns_endpoint, cmd, 'ns_client')

    def ns_showopstatus(self, endpoint):
        rs = self.run_client(endpoint, 'showopstatus', 'ns_client')
        return rs

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

    def sput(self, endpoint, tid, pid, ts, *values):
        return self.run_client(endpoint, 'sput {} {} {} {}'.format(
            tid, pid, ts, ' '.join(values[0])))

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

    def preview(self, endpoint, tid, pid, limit = ''):
        cmd = 'preview {} {} {}'.format(tid, pid, limit)
        result = self.run_client(endpoint, cmd, 'client')
        return self.parse_scan_result(result)

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

    def offlineendpoint(self, endpoint, offline_endpoint, concurrency=''):
        return self.run_client(endpoint, 'offlineendpoint {} {}'.format(offline_endpoint, concurrency), 'ns_client')

    def recoverendpoint(self, endpoint, offline_endpoint, need_restore='', concurrency=''):
        return self.run_client(endpoint, 'recoverendpoint {} {} {}'.format(offline_endpoint, need_restore, concurrency), 'ns_client')

    def recovertable(self, endpoint, name, pid, offline_endpoint):
        return self.run_client(endpoint, 'recovertable {} {} {}'.format(name, pid, offline_endpoint), 'ns_client')

    def changeleader(self, endpoint, tname, pid, candidate_leader=''):
        if candidate_leader != '':
            return self.run_client(endpoint, 'changeleader {} {} {}'.format(tname, pid, candidate_leader), 'ns_client')
        else:
            return self.run_client(endpoint, 'changeleader {} {}'.format(tname, pid), 'ns_client')

    def settablepartition(self, endpoint, name, partition_file):
        return self.run_client(endpoint, 'settablepartition {} {}'.format(name, partition_file), 'ns_client')

    def updatetablealive(self, endpoint, name, pid, des_endpint, is_alive):
        return self.run_client(endpoint, 'updatetablealive {} {} {} {}'.format(name, pid, des_endpint, is_alive), 'ns_client')

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

    def gettablestatus(self, endpoint, tid='', pid=''):
        rs = self.run_client(endpoint, 'gettablestatus {} {}'.format(tid, pid))
        return rs

    @staticmethod
    def parse_schema(rs):
        arr = rs.strip().split("\n")
        schema = []
        column_key = []
        parts = 0;
        for line in arr:
            line = line.strip()
            if line.find("--------------") != -1:
                parts += 1
                continue
            if parts == 0:
                continue
            item = line.split(" ")
            elements = [x for x in item if x != '']
            if parts == 1 and (len(elements) == 3 or len(elements) == 4):
                schema.append(elements)
            elif parts == 2 and len(elements) == 5:
                column_key.append(elements)
        return (schema, column_key)

    @staticmethod
    def parse_db(rs):
        arr = rs.strip().split("\n")
        dbs = []
        start = False
        for line in arr:
            if line.find("----") != -1:
                start = True
                continue
            if not start:
                continue
            items = line.split(" ")
            for db in items:
                if db != '':
                    dbs.append(db)
                    break
        return dbs

    def showschema(self, endpoint, tid='', pid=''):
        rs = self.run_client(endpoint, 'showschema {} {}'.format(tid, pid))
        return self.parse_schema(rs)

    def ns_showschema(self, endpoint, name):
        rs = self.run_client(endpoint, 'showschema {}'.format(name), 'ns_client')
        return self.parse_schema(rs)

    def ns_usedb(self, endpoint, name=''):
        rs = self.run_client(endpoint, 'use {}'.format(name), 'ns_client')
        return rs

    def ns_showdb(self, endpoint):
        rs = self.run_client(endpoint, 'showdb', 'ns_client')
        return self.parse_db(rs)

    def ns_createdb(self, endpoint, name=''):
        rs = self.run_client(endpoint, 'createdb {}'.format(name), 'ns_client')
        return rs

    def ns_dropdb(self, endpoint, name=''):
        rs = self.run_client(endpoint, 'dropdb {}'.format(name), 'ns_client')
        return rs

    def showtablet(self, endpoint):
        rs = self.run_client(endpoint, 'showtablet', 'ns_client')
        return self.parse_tb(rs, ' ', [0], [2, 3])

    def showopstatus(self, endpoint, name='', pid=''):
        rs = self.run_client(endpoint, 'showopstatus {} {}'.format(name, pid), 'ns_client')
        tablestatus = self.parse_tb(rs, ' ', [0], [1, 4, 8])
        tablestatus_d = {(int(k)): v for k, v in tablestatus.items()}
        return tablestatus_d

    def showtable(self, endpoint, table_name = ''):
        cmd = 'showtable {}'.format(table_name)
        rs = self.run_client(endpoint, cmd, 'ns_client')
        return self.parse_tb(rs, ' ', [0, 1, 2, 3], [4, 5, 6, 7])

    def showtable_with_tablename(self, endpoint, table = ''):
        cmd = 'showtable {}'.format(table)
        return self.run_client(endpoint, cmd, 'ns_client')

    def ns_deleteindex(self, endpoint, table_name, index_name):
        cmd = 'deleteindex {} {}'.format(table_name, index_name)
        return self.run_client(endpoint, cmd, 'ns_client')

    @staticmethod
    def get_table_meta(nodepath, tid, pid):
        table_meta = {}
        with open('{}/db/{}_{}/table_meta.txt'.format(nodepath, tid, pid)) as f:
            for l in f:
                arr = l.split(":")
                if (len(arr) < 2):
                    continue
                k = arr[0]
                v = arr[1].strip()
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
        rs = self.showtable(self.ns_leader, tname)
        infoLogger.info(rs)
        for (key, value) in rs.items():
            if key[1] == str(tid) and key[2] == str(pid):
                infoLogger.info(value)
                if value[0] == "leader" and value[2] == "yes":
                    new_leader = key[3]
                    infoLogger.debug('------new leader:' + new_leader + '-----------')
        self.new_tb_leader = new_leader
        return new_leader

    @staticmethod
    def update_conf(nodepath, conf_item, conf_value, role='client'):
        conf_file = ''
        if role == 'client':
            conf_file = 'tablet.flags'
        elif role == 'ns_client':
            conf_file = 'nameserver.flags'
        utils.exe_shell("sed -i '/{}/d' {}/conf/{}".format(conf_item, nodepath, conf_file))
        if conf_value is not None:
            utils.exe_shell("sed -i '1i--{}={}' {}/conf/{}".format(conf_item, conf_value, nodepath, conf_file))

    def get_latest_opid_by_tname_pid(self, tname, pid):
        rs = self.run_client(self.ns_leader, 'showopstatus {} {}'.format(tname, pid), 'ns_client')
        opstatus = self.parse_tb(rs, ' ', [0], [1, 4, 8])
        op_id_arr = []
        for op_id in opstatus.keys():
            op_id_arr.append(int(op_id))
        self.latest_opid = sorted(op_id_arr)[-1]
        infoLogger.debug('------latest_opid:' + str(self.latest_opid) + '---------------')
        return self.latest_opid

    def check_op_done(self, tname):
        rs = self.run_client(self.ns_leader, 'showopstatus {} '.format(tname), 'ns_client')
        infoLogger.info(rs)
        opstatus = self.parse_tb(rs, ' ', [0], [1, 4, 8])
        infoLogger.info(opstatus)
        for op_id in opstatus.keys():
            if opstatus[op_id][1] == "kDoing" or opstatus[op_id][1] == "kInited":
                return False
        return True    

    def wait_op_done(self, tname):
        for cnt in xrange(10):
            if self.check_op_done(tname):
                return
            time.sleep(2)

    def get_op_by_opid(self, op_id):
        rs = self.showopstatus(self.ns_leader)
        return rs[op_id][0]

    def get_task_dict_by_opid(self, tname, opid):
        time.sleep(1)
        task_dict = collections.OrderedDict()
        cmd = "cat {}/warning.log |grep -a -A 10000 '{}'|grep -a \"op_id\[{}\]\"|grep task_type".format(
            self.ns_leader_path, tname, opid) \
              + "|awk -F '\\\\[' '{print $3\"]\"$4\"]\"$5}'" \
                "|awk -F '\\\\]' '{print $1\",\"$3\",\"$5}'"
        infoLogger.info(cmd)
        rs = utils.exe_shell(cmd).split('\n')
        infoLogger.info(rs)
        for x in rs:
            x = x.split(',')
            task_dict[(int(x[1]), x[2])] = x[0]
        self.task_dict = task_dict

    def get_tablet_endpoints(self):
        return set(conf.tb_endpoints)

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
                          'kRecoverSnapshot', 'kCheckBinlogSyncProgress', 'kUpdatePartitionStatus'])

    def check_re_add_replica_no_send_op(self, op_id):
        self.check_tasks(op_id,
                         ['kPauseSnapshot', 'kLoadTable', 'kAddReplica',
                          'kRecoverSnapshot', 'kCheckBinlogSyncProgress', 'kUpdatePartitionStatus'])

    def check_re_add_replica_with_drop_op(self, op_id):
        self.check_tasks(op_id,
                         ['kPauseSnapshot', 'kDropTable', 'kSendSnapshot', 'kLoadTable', 'kAddReplica',
                          'kRecoverSnapshot', 'kCheckBinlogSyncProgress', 'kUpdatePartitionStatus'])

    def check_re_add_replica_simplify_op(self, op_id):
        self.check_tasks(op_id, ['kAddReplica', 'kCheckBinlogSyncProgress', 'kUpdatePartitionStatus'])

    def check_migrate_op(self, op_id):
        self.check_tasks(op_id, ['kPauseSnapshot', 'kSendSnapshot', 'kRecoverSnapshot', 'kLoadTable', 
                                 'kAddReplica', 'kAddTableInfo', 'kCheckBinlogSyncProgress', 'kDelReplica',
                                 'kUpdateTableInfo', 'kDropTable'])

    def ns_setttl(self, endpoint, setttl, table_name, ttl_type, ttl, ts_name = ''):
        cmd = '{} {} {} {} {}'.format(setttl, table_name, ttl_type, ttl, ts_name)
        return self.run_client(endpoint, cmd, 'ns_client')

    def setttl(self, endpoint, setttl, table_name, ttl_type, ttl):
        cmd = '{} {} {} {}'.format(setttl, table_name, ttl_type, ttl)
        return self.run_client(endpoint, cmd)

    def print_table(self, endpoint = '',  name = ''):
        infoLogger.info('*' * 50)
        rs_show = ''
        if endpoint != '':
            rs_show = self.showtable_with_tablename(endpoint, name)
        else:
            rs_show = self.showtable_with_tablename(self.ns_leader, name)
        infoLogger.info(rs_show)
        rs_show = self.parse_tb(rs_show, ' ', [0, 1, 2, 3], [4, 5, 6, 7, 8, 9, 10])
        for table_info in rs_show:
            infoLogger.info('{} =  {}'.format(table_info, rs_show[table_info]))
        infoLogger.info('*' * 50)
        
