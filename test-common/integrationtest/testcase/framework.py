# -*- coding: utf-8 -*-
import unittest
import commands
import random
import os
import time
import sys
sys.path.append("test-common/integrationtest")

class TestCaseBase(unittest.TestCase):

    def exe_shell(self, cmd):
        # print cmd
        retcode, output = commands.getstatusoutput(cmd)
        return output

    @classmethod
    def setUpClass(cls):
        cls.welcome = 'Welcome to rtidb with version {}\n'.format(os.getenv('rtidbver'))
        cls.rtidb_path = os.getenv('rtidbpath')
        cls.conf_path = os.getenv('confpath')
        cls.leaderpath = os.getenv('leaderpath')
        cls.slave1path = os.getenv('slave1path')
        cls.slave2path = os.getenv('slave2path')
        cls.leader = os.getenv('leader')
        cls.slave1 = os.getenv('slave1')
        cls.slave2 = os.getenv('slave2')

    def setUp(self):
        try:
            self.tid = int(self.exe_shell("ls " + self.leaderpath + "/db/|awk -F '_' '{print $1}'|sort -n|tail -1")) + 1
        except Exception, e:
            self.tid = 1
        self.pid = random.randint(10, 100)
        # print self.tid

    def tearDown(self):
        self.drop(self.leader, self.tid, self.pid)
        self.drop(self.slave1, self.tid, self.pid)
        self.drop(self.slave2, self.tid, self.pid)

    def now(self):
        return int(1000 * time.time())

    def start_client(self, client_path):
        self.exe_shell('cd {} && ../mon ./conf/boot.sh -d -l ./rtidb_mon.log'.format(client_path))
        time.sleep(2)

    def stop_client(self, endpoint):
        pid = self.exe_shell("lsof -i:"+ endpoint.split(':')[1] +" |grep LISTEN|awk '{print $2}'")
        self.exe_shell("ps xf|grep -B 2 " + str(pid) + "|grep rtidb_mon.log|awk '{print $1}'|xargs kill -9")
        self.exe_shell("kill -9 {}".format(pid))

    def run_client(self, endpoint, cmd):
        # print cmd
        rs = self.exe_shell('{} --endpoint={} --role=client --interactive=false --cmd="{}"'.format(
            self.rtidb_path, endpoint, cmd))
        return rs.replace(self.welcome, '').replace('>', '')

    def get_table_status(self, endpoint, tid='', pid=''):
        rs = self.run_client(endpoint, 'gettablestatus {} {}'.format(tid, pid))
        if tid == '':
            return self.parse_table_status(rs)
        else:
            try:
                return self.parse_table_status(rs)[(tid, pid)]
            except KeyError, e:
                print 'table {} is not exist!'.format(e)

    def parse_table_status(self, rs):
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
                    print e
        return table_status_dict

    def get_manifest(self, nodepath, tid, pid):
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

    def put(self, endpoint, tid, pid, key, ts, value):
        return self.run_client(endpoint, 'put {} {} {} {} {}'.format(
            tid, pid, key, ts, value))

    def makesnapshot(self, endpoint, tid, pid):
        rs = self.run_client(endpoint, 'makesnapshot {} {}'.format(tid, pid))
        time.sleep(1)
        return rs

    def pausesnapshot(self, endpoint, tid, pid):
        rs = self.run_client(endpoint, 'pausesnapshot {} {}'.format(tid, pid))
        time.sleep(1)
        return rs

    def recoversnapshot(self, endpoint, tid, pid):
        rs = self.run_client(endpoint, 'recoversnapshot {} {}'.format(tid, pid))
        time.sleep(1)
        return rs

    def addreplica(self, endpoint, tid, pid, *slave_endpoints):
        rs = self.run_client(endpoint, 'addreplica {} {} {}'.format(tid, pid, ' '.join(slave_endpoints)))
        time.sleep(1)
        return rs

    def scan(self, endpoint, tid, pid, key, ts_from, ts_end):
        return self.run_client(endpoint, 'scan {} {} {} {} {}'.format(
            tid, pid, key, ts_from, ts_end))

    def create(self, endpoint, tname, tid, pid, ttl=144000, segment=8, isleader='true', *slave_endpoints):
        return self.run_client(endpoint, 'create {} {} {} {} {} {} {}'.format(
            tname, tid, pid, ttl, segment, isleader, ' '.join(slave_endpoints)))

    def loadtable(self, endpoint, tname, tid, pid, ttl=144000, segment=8, isleader='false', *slave_endpoints):
        return self.run_client(endpoint, 'loadtable {} {} {} {} {} {} {}'.format(
            tname, tid, pid, ttl, segment, isleader, ' '.join(slave_endpoints)))

    def drop(self, endpoint, tid, pid):
        return self.run_client(endpoint, 'drop {} {}'.format(tid, pid))

    def changerole(self, endpoint, tid, pid, role):
        return self.run_client(endpoint, 'changerole {} {} {}'.format(tid, pid, role))

    def cp_db(self, from_node, to_node, tid, pid):
        self.exe_shell('cp -r {from_node}/db/{tid}_{pid} {to_node}/db/'.format(
            from_node=from_node, tid=tid, pid=pid, to_node=to_node))