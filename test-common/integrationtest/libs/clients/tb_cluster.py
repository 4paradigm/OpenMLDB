# -*- coding: utf-8 -*-
import sys
import os
import shlex
import subprocess
import time
sys.path.append(os.getenv('testpath'))
from libs.utils import exe_shell
from libs.logger import infoLogger


class TbCluster(object):
    def __init__(self, zk_endpoint, endpoints, scan_endpoints):
        self.endpoints = endpoints
        self.scan_endpoints = scan_endpoints
        self.zk_endpoint = zk_endpoint
        self.leader = ''


    def start(self, endpoints, scan_endpoints):
        confpath = os.getenv('confpath')
        i = 0
        test_path = os.getenv('testpath')
        for ep in endpoints:
            ep_scan = '{}:{}'.format(ep.split(':')[0], scan_endpoints[i].split(':')[1])
            i += 1
            tb_path = test_path + '/tablet{}'.format(i)
            exe_shell('mkdir -p {}/conf'.format(tb_path))
            exe_shell('cat {} | egrep -v "endpoint=|--gc_interval=|--db_root_path=|--log_dir=" > '
                      '{}/conf/rtidb.flags'.format(confpath, tb_path))
            exe_shell("sed -i '1a --endpoint='{} {}/conf/rtidb.flags".format(ep, tb_path))
            exe_shell("sed -i '1a --scan_endpoint='{} {}/conf/rtidb.flags".format(ep_scan, tb_path))
            exe_shell("sed -i '1a --gc_interval=1' {}/conf/rtidb.flags".format(tb_path))
            exe_shell("sed -i '1a --db_root_path={}/db' {}/conf/rtidb.flags".format(tb_path, tb_path))
            exe_shell("sed -i '1a --zk_cluster='{} {}/conf/rtidb.flags".format(self.zk_endpoint, tb_path))
            exe_shell("echo '--zk_root_path=/onebox' >> {}/conf/rtidb.flags".format(tb_path))
            cmd = '{}/rtidb --flagfile={}/conf/rtidb.flags'.format(test_path, tb_path)
            args = shlex.split(cmd)
            print cmd
            subprocess.Popen(args, stdout=open('{}/log.log'.format(tb_path), 'w'))
            time.sleep(3)


    def kill(self, *endpoints):
        infoLogger.info(endpoints)
        port = ''
        for ep in endpoints:
            infoLogger.info(ep)
            port += ep.split(':')[1] + ' '
        infoLogger.info(port)
        cmd = "for i in {};".format(port) + " do lsof -i:${i}|grep (LISTEN)|awk '{print $2}'|xargs kill;done"
        infoLogger.info(cmd)
        exe_shell(cmd)

