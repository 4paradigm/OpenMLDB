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
    def __init__(self, zk_endpoint, endpoints):
        self.endpoints = endpoints
        self.zk_endpoint = zk_endpoint
        self.leader = ''


    def start(self, endpoints):
        tbconfpath = os.getenv('tbconfpath')
        i = 0
        test_path = os.getenv('testpath')
        for ep in endpoints:
            i += 1
            tb_path = test_path + '/tablet{}'.format(i)
            rtidb_flags = '{}/conf/rtidb.flags'.format(tb_path)
            exe_shell('mkdir -p {}/conf'.format(tb_path))
            exe_shell('cat {} | egrep -v "endpoint|gc_interval|db_root_path|log_dir|recycle_bin_root_path" > '
                      '{}'.format(tbconfpath, rtidb_flags))
            exe_shell("sed -i '1a --endpoint='{} {}".format(ep, rtidb_flags))
            exe_shell("sed -i '1a --gc_interval=1' {}".format(rtidb_flags))
            exe_shell("sed -i '1a --db_root_path={}/db' {}".format(tb_path, rtidb_flags))
            exe_shell("sed -i '1a --zk_cluster='{} {}".format(self.zk_endpoint, rtidb_flags))
            exe_shell("sed -i '1a --recycle_bin_root_path={}/recycle' {}".format(tb_path, rtidb_flags))
            exe_shell("echo '--stream_close_wait_time_ms=10' >> {}".format(rtidb_flags))
            exe_shell("echo '--stream_bandwidth_limit=0' >> {}".format(rtidb_flags))
            exe_shell("echo '--zk_root_path=/onebox' >> {}".format(rtidb_flags))
            exe_shell("echo '--zk_keep_alive_check_interval=500000' >> {}".format(rtidb_flags))
            exe_shell("ulimit -c unlimited")
            cmd = '{}/rtidb --flagfile={}/conf/rtidb.flags'.format(test_path, tb_path)
            infoLogger.info('start rtidb: {}'.format(cmd))
            args = shlex.split(cmd)
            subprocess.Popen(args,stdout=open('{}/info.log'.format(tb_path), 'w'),
                             stderr=open('{}/warning.log'.format(tb_path), 'w'))
            time.sleep(3)


    def kill(self, *endpoints):
        infoLogger.info(endpoints)
        port = ''
        for ep in endpoints:
            infoLogger.info(ep)
            port += ep.split(':')[1] + ' '
        infoLogger.info(port)
        cmd = "for i in {};".format(port) + " do lsof -i:${i}|grep \"(LISTEN)\"|awk '{print $2}'|xargs kill;done"
        exe_shell(cmd)


    def clear_db(self):
        exe_shell('rm -rf {}/tablet*'.format(os.getenv('testpath')))