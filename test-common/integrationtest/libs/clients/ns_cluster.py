# -*- coding: utf-8 -*-
import sys
import os
import shlex
import subprocess
import time
sys.path.append(os.getenv('testpath'))
from libs.utils import exe_shell
from libs.logger import infoLogger


class NsCluster(object):
    def __init__(self, zk_endpoint, *endpoints):
        self.endpoints = endpoints
        self.zk_endpoint = zk_endpoint
        self.leader = ''


    def start_zk(self):
        zk_path = os.getenv('zkpath')
        port = self.zk_endpoint.split(':')[1]
        exe_shell("echo tickTime=2000 > {}/conf/zoo.cfg".format(zk_path))
        exe_shell("echo initLimit=10 >> {}/conf/zoo.cfg".format(zk_path))
        exe_shell("echo syncLimit=5 >> {}/conf/zoo.cfg".format(zk_path))
        exe_shell("echo dataDir={}/data >> {}/conf/zoo.cfg".format(zk_path, zk_path))
        exe_shell("echo clientPort={} >> {}/conf/zoo.cfg".format(port, zk_path))
        exe_shell("sh {}/bin/zkServer.sh start".format(zk_path))
        time.sleep(2)


    def stop_zk(self):
        exe_shell("sh {}/bin/zkServer.sh stop".format(os.getenv('zkpath')))
        time.sleep(2)


    def clear_zk(self):
        exe_shell('rm -rf {}/data'.format(os.getenv('zkpath')))


    def start(self, *endpoints):
        i = 0
        for ep in endpoints:
            i += 1
            test_path = os.getenv('testpath')
            ns_path = test_path + '/ns{}'.format(i)
            exe_shell('mkdir -p {}/conf'.format(ns_path))
            exe_shell("echo '--endpoint='{} > {}/conf/rtidb.flags".format(ep, ns_path))
            exe_shell("echo '--role=nameserver' >> {}/conf/rtidb.flags".format(ns_path))
            exe_shell("echo '--zk_cluster='{} >> {}/conf/rtidb.flags".format(self.zk_endpoint, ns_path))
            exe_shell("echo '--zk_root_path=/onebox' >> {}/conf/rtidb.flags".format(ns_path))
            cmd = '{}/rtidb --flagfile={}/conf/rtidb.flags'.format(test_path, ns_path)
            args = shlex.split(cmd)
            print cmd
            subprocess.Popen(args, stdout=open('{}/log.log'.format(ns_path), 'w'))
            time.sleep(5)
            if_lock = exe_shell('grep "get lock with assigned_path" {}/log.log'.format(ns_path))
            if 'get lock with assigned_path' in if_lock:
                self.leader = ep
                exe_shell('echo "{}" > {}/ns_leader'.format(ep, test_path))


    def kill(self, *endpoints):
        infoLogger.info(endpoints)
        port = ''
        for ep in endpoints:
            infoLogger.info(ep)
            port += ep.split(':')[1] + ' '
        infoLogger.info(port)
        cmd = "for i in {};".format(port) + " do lsof -i:${i}|grep -v 'PID'|awk '{print $2}'|xargs kill;done"
        infoLogger.info(cmd)
        exe_shell(cmd)
        time.sleep(2)


#
# n = NsCluster('127.0.0.1:22181', '127.0.0.1:37777', '127.0.0.1:37778')
# print n.leader
