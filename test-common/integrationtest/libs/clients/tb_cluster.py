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
            exe_shell('export ')
            rtidb_flags = '{}/conf/rtidb.flags'.format(tb_path)
            exe_shell('mkdir -p {}/conf'.format(tb_path))
            exe_shell('cat {} | egrep -v "endpoint|log_level|gc_interval|db_root_path|log_dir|recycle_bin_root_path" > '
                      '{}'.format(tbconfpath, rtidb_flags))
            exe_shell("sed -i '1a --endpoint='{} {}".format(ep, rtidb_flags))
            exe_shell("sed -i '1a --gc_interval=1' {}".format(rtidb_flags))
            exe_shell("sed -i '1a --db_root_path={}/db' {}".format(tb_path, rtidb_flags))
            exe_shell("sed -i '1a --ssd_root_path={}/ssd_db' {}".format(tb_path, rtidb_flags))
            exe_shell("sed -i '1a --hdd_root_path={}/hdd_db' {}".format(tb_path, rtidb_flags))
            exe_shell("sed -i '1a --zk_cluster='{} {}".format(self.zk_endpoint, rtidb_flags))
            exe_shell("sed -i '1a --recycle_bin_root_path={}/recycle' {}".format(tb_path, rtidb_flags))
            exe_shell("sed -i '1a --recycle_ssd_bin_root_path={}/ssd_recycle' {}".format(tb_path, rtidb_flags))
            exe_shell("sed -i '1a --recycle_hdd_bin_root_path={}/hdd_recycle' {}".format(tb_path, rtidb_flags))
            exe_shell("echo '--log_level={}' >> {}".format(conf.rtidb_log_info, rtidb_flags))
            exe_shell("echo '--stream_close_wait_time_ms=10' >> {}".format(rtidb_flags))
            exe_shell("echo '--stream_bandwidth_limit=0' >> {}".format(rtidb_flags))
            exe_shell("echo '--zk_root_path=/onebox' >> {}".format(rtidb_flags))
            exe_shell("echo '--zk_keep_alive_check_interval=500000' >> {}".format(rtidb_flags))
            exe_shell("echo '--gc_safe_offset=0' >> {}".format(rtidb_flags))
            exe_shell("echo '--binlog_sync_to_disk_interval=10' >> {}".format(rtidb_flags))
            exe_shell("echo '--binlog_sync_wait_time=10' >> {}".format(rtidb_flags))
            exe_shell("echo '--zk_session_timeout=2000' >> {}".format(rtidb_flags))
            exe_shell("echo '--make_snapshot_threshold_offset=0' >> {}".format(rtidb_flags))
            exe_shell("ulimit -c unlimited")
            cmd = '{}/rtidb --flagfile={}/conf/rtidb.flags'.format(test_path, tb_path)
            infoLogger.info('start rtidb: {}'.format(cmd))
            args = shlex.split(cmd)
            started = []
            for _ in range(5):
                rs = exe_shell('lsof -i:{}|grep -v "PID"'.format(ep.split(':')[1]))
                if 'rtidb' not in rs:
                    time.sleep(2)
                    subprocess.Popen(args,stdout=open('{}/info.log'.format(tb_path), 'a'),
                                     stderr=open('{}/warning.log'.format(tb_path), 'a'))
                else:
                    started.append(True)
                    break
        return started

    def kill(self, *endpoints):
        infoLogger.info(endpoints)
        port = ''
        for ep in endpoints:
            infoLogger.info(ep)
            port += ep.split(':')[1] + ' '
        infoLogger.info(port)
        cmd = "for i in {};".format(port) + " do lsof -i:${i}|grep \"(LISTEN)\"|awk '{print $2}'|xargs kill -9;done"
        exe_shell(cmd)


    def clear_db(self):
        exe_shell('rm -rf {}/tablet*'.format(os.getenv('testpath')))
