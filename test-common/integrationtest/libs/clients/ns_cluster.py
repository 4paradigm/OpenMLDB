# -*- coding: utf-8 -*-
import sys
import os
import shlex
import subprocess
import time
sys.path.append(os.getenv('testpath'))
import libs.conf as conf
from libs.utils import exe_shell
from libs.logger import infoLogger


class NsCluster(object):
    def __init__(self, zk_endpoint, *endpoints):
        self.endpoints = endpoints
        self.zk_endpoint = zk_endpoint
        self.zk_path = os.getenv('zkpath')
        self.test_path = os.getenv('testpath')
        self.ns_edp_path = {endpoints[i]: self.test_path + '/ns{}'.format(i + 1) for i in range(len(endpoints))}
        self.leader = ''
        self.leaderpath = ''


    def start_zk(self):
        exe_shell("echo 1 >> {}/data/myid".format(self.zk_path))
        port = self.zk_endpoint.split(':')[-1]
        zoo_cfg = '{}/conf/zoo.cfg'.format(self.zk_path)
        exe_shell("echo tickTime=2000 > {}".format(zoo_cfg))
        exe_shell("echo initLimit=10 >> {}".format(zoo_cfg))
        exe_shell("echo syncLimit=5 >> {}".format(zoo_cfg))
        exe_shell("echo dataDir={}/data >> {}".format(self.zk_path, zoo_cfg))
        exe_shell("echo clientPort={} >> {}".format(port, zoo_cfg))
        exe_shell("echo server.1=127.0.0.1:2888:2890 >> {}".format(zoo_cfg))
        # exe_shell("echo server.2=172.27.2.252:2888:2890 >> {}".format(zoo_cfg))
        # exe_shell("echo server.3=172.27.128.37:2888:2890 >> {}".format(zoo_cfg))
        exe_shell("sh {}/bin/zkServer.sh start".format(self.zk_path))
        time.sleep(3)


    def stop_zk(self):
        port = conf.zk_endpoint.split(':')[-1]
        exe_shell("lsof -i:" + port + "|awk '{print $2}'|xargs kill")
        exe_shell("sh {}/bin/zkServer.sh stop".format(os.getenv('zkpath')))
        time.sleep(2)


    def clear_zk(self):
        exe_shell('rm -rf {}/data'.format(os.getenv('zkpath')))


    def start(self, *endpoints):
        nsconfpath = os.getenv('nsconfpath')
        i = 0
        for ep in endpoints:
            i += 1
            ns_path = self.ns_edp_path[ep]
            nameserver_flags = '{}/conf/nameserver.flags'.format(ns_path)
            exe_shell('mkdir -p {}/conf'.format(ns_path))
            exe_shell('touch {}'.format(nameserver_flags))
            exe_shell("echo '--log_level={}' >> {}".format(conf.rtidb_log_info, nameserver_flags))
            exe_shell("echo '--endpoint='{} >> {}".format(ep, nameserver_flags))
            exe_shell("echo '--role=nameserver' >> {}".format(nameserver_flags))
            exe_shell("echo '--zk_cluster='{} >> {}".format(self.zk_endpoint, nameserver_flags))
            exe_shell("echo '--zk_root_path=/onebox' >> {}".format(nameserver_flags))
            exe_shell("echo '--auto_failover=true' >> {}".format(nameserver_flags))
            exe_shell("echo '--auto_recover_table=true' >> {}".format(nameserver_flags))
            exe_shell("echo '--get_task_status_interval=1' >> {}".format(nameserver_flags))
            exe_shell("echo '--name_server_task_pool_size=10' >> {}".format(nameserver_flags))
            exe_shell("echo '--tablet_startup_wait_time=3000' >> {}".format(nameserver_flags))
            exe_shell("echo '--zk_keep_alive_check_interval=500000' >> {}".format(nameserver_flags))
            cmd = '{}/rtidb --flagfile={}'.format(self.test_path, nameserver_flags)
            infoLogger.info('start rtidb: {}'.format(cmd))
            args = shlex.split(cmd)
            started = []
            for _ in range(5):
                rs = exe_shell('lsof -i:{}|grep -v "PID"'.format(ep.split(':')[1]))
                if 'rtidb' not in rs:
                    time.sleep(2)
                    subprocess.Popen(args,stdout=open('{}/info{}.log'.format(ns_path, time.time()), 'w'),
                                     stderr=open('{}/warning.log'.format(ns_path), 'w'))
                else:
                    started.append(True)
                    break
        return started


    def get_ns_leader(self):
        locks = exe_shell("echo \"ls /onebox/leader\"|sh {}/bin/zkCli.sh -server {}"
                          "|tail -n 2".format(self.zk_path, self.zk_endpoint))
        if locks:
            nodes = locks.split('\n')[0][1:-1].split(',')
            nodex = [int(node.strip()[-10:]) for node in nodes]
        nodex.sort()
        node = str(nodex[0])
        node_leader = 'lock_request0000000000'[:-len(node)] + node
        output = exe_shell("echo \"get /onebox/leader/{}\""
                              "|sh {}/bin/zkCli.sh -server {}"
                              "|tail -n 2".format(node_leader, self.zk_path, self.zk_endpoint))
        ns_leader = output.split('\n')[-2]
        self.ns_leader = ns_leader
        exe_shell('echo "{}" > {}/ns_leader'.format(ns_leader, self.test_path))
        exe_shell('echo "{}" >> {}/ns_leader'.format(self.ns_edp_path[ns_leader], self.test_path))


    def kill(self, *endpoints):
        infoLogger.info(endpoints)
        port = ''
        for ep in endpoints:
            infoLogger.info(ep)
            port += ep.split(':')[1] + ' '
        infoLogger.info(port)
        cmd = "for i in {};".format(port) + " do lsof -i:${i}|grep -v 'PID'|awk '{print $2}'|xargs kill;done"
        exe_shell(cmd)
        time.sleep(1)


    def clear_ns(self):
        exe_shell('rm -rf {}/ns*'.format(os.getenv('testpath')))
