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
import logging
import os
import re

import paramiko
from paramiko.file import BufferedFile

from diagnostic_tool.dist_conf import DistConf, CXX_SERVER_ROLES, ServerInfo, JAVA_SERVER_ROLES

log = logging.getLogger(__name__)


def parse_config_from_properties(props_str, config_name) -> str:
    f"""
    the config line must start from {config_name}, no comment
    :param props_str: 
    :param config_name: 
    :return: 
    """
    config_name = re.escape(config_name)
    m = re.search(rf'^{config_name}.*', props_str, )
    if not m:
        return ''
    conf_line = m.group(0)  # the whole line
    # TODO(hw): what if relative path
    return conf_line.split('=')[1]


def buf2str(buf: BufferedFile) -> str:
    return buf.read().decode("utf-8")


class Collector:
    def __init__(self, dist_conf: DistConf):
        self.dist_conf = dist_conf
        # use one ssh client to connect all servers, ssh connections won't keep alive
        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.load_system_host_keys()

    def ping_all(self) -> bool:
        """
        test ssh
        return False if command got some errors
        throw SSHException if the server fails to execute the command
        :return: bool
        """

        def ping(server_info: ServerInfo) -> bool:
            self.ssh_client.connect(hostname=server_info.host)
            _, stdout, stderr = self.ssh_client.exec_command('whoami && pwd')
            print(buf2str(stdout))
            err = buf2str(stderr)
            if len(err) != 0:
                log.warning(f"failed to ping {server_info}, err: {err}")
                return False
            return True

        return self.dist_conf.server_info_map.for_each(ping)

    def pull_config_files(self, dest) -> bool:
        def pull_one(server_info: ServerInfo) -> bool:
            # if taskmanager, pull taskmanager.properties, no log4j
            config_paths = server_info.conf_path_pair(dest)
            return self.pull_file(server_info.host, config_paths)

        return self.dist_conf.server_info_map.for_each(pull_one)

    def pull_log_files(self, dest) -> bool:
        def pull_cxx(server_info: ServerInfo) -> bool:
            return self.pull_cxx_server_logs(server_info, dest, 2)

        def pull_taskmanager(server_info: ServerInfo) -> bool:
            res = self.pull_job_logs(server_info, dest, 2)
            return self.pull_tm_server_logs(server_info, dest, 2) and res

        ok = self.dist_conf.server_info_map.for_each(pull_cxx, CXX_SERVER_ROLES)
        return self.dist_conf.server_info_map.for_each(pull_taskmanager, JAVA_SERVER_ROLES) and ok


    def collect_version(self):
        """
        get the version of components before starts
        :return:
        """
        version_map = {}
        def extract_version(raw_version):
            return raw_version.split(' ')[2].split('-')[0]

        def run_version(server_info: ServerInfo) -> bool:
            version_map.setdefault(server_info.role, [])
            self.ssh_client.connect(hostname=server_info.host)
            _, stdout, _ = self.ssh_client.exec_command(f'{server_info.path}/bin/openmldb --version')
            version = buf2str(stdout)
            if not version:
                log.warning('failed at get version from %s', server_info)
                return False
            version_map[server_info.role].append((server_info.host, extract_version(version)))
            return True

        self.dist_conf.server_info_map.for_each(run_version, CXX_SERVER_ROLES)

        def jar_version(server_info: ServerInfo) -> bool:
            remote_config_file = server_info.conf_path_pair('')[0]
            bv = self.get_batch_version(self.get_spark_home(remote_config_file))
            print(bv) if bv else log.warning('failed at get batch version from %s', server_info)
            tv = self.get_taskmanager_version(server_info.taskmanager_path())
            print(tv) if tv else log.warning('failed at get taskmanager version from %s', server_info)
            return len(bv) != 0 and len(tv) != 0

        self.dist_conf.server_info_map.for_each(jar_version, JAVA_SERVER_ROLES)
        return version_map

    def get_spark_home(self, remote_config_file):
        """

        :param remote_config_file:
        :return: abs path
        """
        config_name = 'spark.home='
        log.info("get %s from %s", config_name, remote_config_file)
        # avoid comments
        _, stdout, _ = self.ssh_client.exec_command(f"grep {config_name} {remote_config_file}")
        grep_str = buf2str(stdout)

        value = ''
        if not grep_str:
            # TODO(hw):no config in file, get env SPARK_HOME?
            #  what if ssh user is different with server user or it's a temp env?
            # or force to set spark home in config?
            # _, stdout, _ = self.ssh_client.exec_command(f'env | grep SPARK_HOME')
            # env = stdout.read()
            # if not env:
            #     raise RuntimeError('no env SPARK_HOME')
            return value

        # may have spark home in config(discard if it's in comment)
        return parse_config_from_properties(grep_str, config_name)

    def get_batch_version(self, spark_home):
        # TODO(hw): check if multi batch jars
        log.info("spark_home %s", spark_home)
        batch_jar_path = f'{spark_home}/jars/openmldb-batch-*'
        _, stdout, err = self.ssh_client.exec_command(
            f'java -cp {batch_jar_path} com._4paradigm.openmldb.batch.utils.VersionCli')
        return buf2str(stdout)

    def get_taskmanager_version(self, root_path):
        # TODO(hw): check if multi taskmanager jars
        _, stdout, _ = self.ssh_client.exec_command(
            f'java -cp {root_path}/lib/openmldb-taskmanager-* '
            f'com._4paradigm.openmldb.taskmanager.utils.VersionCli')
        return buf2str(stdout)

    def pull_job_logs(self, server_info, dest, last_n) -> bool:
        # job log path is in config
        remote_conf_path = server_info.conf_path_pair('')[0]
        job_log_dir = self.remote_config_value(server_info.host, remote_conf_path, 'job.log.path=', '../log')
        # job_log_dir is start from taskmanager/bin
        # TODO(hw): what if abs path?
        job_log_dir = f'{server_info.taskmanager_path()}/bin/{job_log_dir}'

        # only log names job_x_error.log
        log_list = self.remote_log_file_list(server_info.host, job_log_dir,
                                             lambda di: 'error' in di['filename'], last_n)
        return self.pull_files(server_info, job_log_dir, log_list, dest)

    def pull_cxx_server_logs(self, server_info, dest, last_n) -> bool:
        """
        nameserver, tablet: config name openmldb_log_dir
        :param server_info:
        :param dest:
        :param last_n:
        :return:
        """
        remote_conf_path = server_info.conf_path_pair('')[0]
        server_log_dir = self.remote_config_value(server_info.host, remote_conf_path,
                                                  'openmldb_log_dir=', './logs')
        # TODO(hw): what if `openmldb_log_dir` is abs path
        server_log_dir = f'{server_info.path}/{server_log_dir}'
        # only get info log, no soft link file
        log_list = self.remote_log_file_list(server_info.host, server_log_dir,
                                             lambda di: f'{server_info.role}.info.log' in di['filename'], last_n)
        return self.pull_files(server_info, server_log_dir, log_list, dest)

    def pull_tm_server_logs(self, server_info, dest, last_n) -> bool:
        """
        taskmanager: config name log4j.appender.file.file= in log4j, start from taskmanager/bin/
        :param server_info:
        :param dest:
        :param last_n:
        :return:
        """
        # job log path is in config
        if not server_info.is_taskmanager():
            return False
        remote_conf_path = server_info.remote_log4j_path()
        server_log_file_pattern = self.remote_config_value(server_info.host, remote_conf_path, 'log4j.appender.file'
                                                                                               '.file=', '')
        # file.file is a file name, not a dir
        server_log_dir = os.path.split(server_log_file_pattern)[0]

        # TODO(hw): what if abs path?
        # dir is start from taskmanager/bin
        server_log_dir = f'{server_info.taskmanager_path()}/bin/{server_log_dir}'

        log_list = self.remote_log_file_list(server_info.host, server_log_dir,
                                             lambda di: 'taskmanager.log' in di['filename'], last_n)
        return self.pull_files(server_info, server_log_dir, log_list, dest)

    def remote_config_value(self, host, conf_path, config_name, default_v):
        v = default_v
        log.info('get %s from %s', config_name, conf_path)
        self.ssh_client.connect(hostname=host)
        _, stdout, _ = self.ssh_client.exec_command(f'grep {config_name} {conf_path}')
        grep_str = buf2str(stdout)
        if grep_str:
            # may set config in config file
            tmp = parse_config_from_properties(grep_str, config_name)
            if tmp:
                v = tmp
        return v

    def pull_file(self, remote_host, paths) -> bool:
        remote_path, local_path = paths[0], paths[1]
        print(f"remote {remote_path}, local: {local_path}")
        self.ssh_client.connect(hostname=remote_host)
        sftp = self.ssh_client.open_sftp()
        try:
            # ensure local path is exists
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            # local path must be a file, not a dir
            sftp.get(remote_path, local_path)
        except Exception as e:
            log.warning(f"pull from remote {remote_host}:{remote_path} error on , err: {e}")
            return False
        return True

    def pull_files(self, server_info, remote_path, file_list, dest) -> bool:
        if not file_list:
            log.warning('no file in %s on %s', remote_path, server_info)
            return False
        return all([self.pull_file(server_info.host,
                                   server_info.remote_local_pairs(remote_path, file, dest))
                    for file in file_list])

    def get_log_dir_from_conf(self, remote_config_file, server_info):
        """
        nameserver, tablet: server logs
        taskmanager: config file only has job log path, get taskmanager log by log4j

        :param remote_config_file:
        :param server_info:
        :return:
        """
        config_name = "openmldb_log_dir"
        default_dir = "/logs"
        if server_info.role == "taskmanager":
            # log4j logs, not the job logs
            config_name = "job.log.path"
            # taskmanager '../log' is from 'bin/', so it's '/log'.
            # TODO(hw): fix taskmanager start dir
            default_dir = "/log"

        log.info("get %s from %s", config_name, remote_config_file)
        _, stdout, _ = self.ssh_client.exec_command(f"grep {config_name} {remote_config_file}")
        grep_str = buf2str(stdout)

        if not grep_str:
            return server_info.path + default_dir
        # may set log dir path in config
        return parse_config_from_properties(grep_str, config_name)

    def remote_log_file_list(self, host, log_dir, filter_func, last_n):
        """
        if role is taskmanager, log path setting is in log4j.properties
        if ns or tablet, log path setting is in *.flags

        :param host:
        :param log_dir:
        :param filter_func:
        :param last_n:
        :return:
        """
        self.ssh_client.connect(hostname=host)
        sftp = self.ssh_client.open_sftp()

        log_dir = os.path.normpath(log_dir)
        log.info('get logs name from %s, %s', log_dir, host)
        # if no the log dir, let it crash
        logs = [attr.__dict__ for attr in sftp.listdir_attr(log_dir)]

        logs = list(filter(filter_func, logs))

        # avoid soft link file?
        # sort by modify time
        logs.sort(key=lambda x: x["st_mtime"], reverse=True)
        log.info("all_logs(sorted): %s", logs)
        # get last n
        logs = [log_attr['filename'] for log_attr in logs[:last_n]]
        log.info("get last %d: %s", last_n, logs)
        return logs
