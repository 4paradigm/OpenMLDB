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

import paramiko

from diagnostic_tool.dist_conf import DistConf, CXX_SERVER_ROLES, ServerInfo

log = logging.getLogger(__name__)


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
            print(stdout.read())
            stderr_byte = stderr.read()
            if len(stderr_byte) != 0:
                log.warning(f"failed to ping {server_info}, err: {stderr_byte}")
                return False
            return True

        return self.dist_conf.server_info_map.for_each(ping)

    def pull_config_files(self, dest) -> bool:
        def pull_one(server_info: ServerInfo) -> bool:
            config_paths = server_info.make_paths_of_conf(dest)
            return self.pull_file(server_info.host, config_paths)

        return self.dist_conf.server_info_map.for_each(pull_one)

    def pull_log_files(self, dest) -> bool:
        def pull_one(server_info: ServerInfo) -> bool:
            # get log dir
            remote_log_dir, log_names = self.remote_log_file_list(server_info)
            if len(log_names) == 0:
                log.warning('no logs in %s', remote_log_dir)
                return False
            return self.pull_files(server_info.host,
                                   server_info.make_paths_of_logs(remote_log_dir, log_names, dest))

        return self.dist_conf.server_info_map.for_each(pull_one)

    def collect_version(self):
        # TODO(hw): just print?
        def run_version(server_info: ServerInfo) -> bool:
            self.ssh_client.connect(hostname=server_info.host)
            _, stdout, _ = self.ssh_client.exec_command(f'{server_info.path}/bin/openmldb --version')
            version = stdout.read()
            if not version:
                log.warning('failed at get version from %s:%s', server_info.endpoint, server_info.path)
                return False
            print(server_info, 'version: ', version)
            return True

        self.dist_conf.server_info_map.for_each(run_version, CXX_SERVER_ROLES)
        # TODO(hw): taskmanager and batch version
        # if no other jars with the same prefix, it's ok
        # java -cp jars/openmldb-batch-*.jar com._4paradigm.openmldb.batch.utils.VersionCli
        # taskmanager?
        # get from zk, only get the master taskmanager
        return True

    def pull_file(self, remote_host, paths):
        remote_path, local_path = paths[0], paths[1]
        log.info(f"remote {remote_path}, local: {local_path}")
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

    def pull_files(self, remote_host, paths_list) -> bool:
        return all([self.pull_file(remote_host, path_pair) for path_pair in paths_list])

    def get_log_dir_from_conf(self, remote_config_file, server_info):
        config_name = "openmldb_log_dir"
        remote_log_dir_suffix = "/logs"
        if server_info.role == "taskmanager":
            config_name = "job.log.path"
            # taskmanager '../log' is from 'bin/', so it's '/log'.
            # TODO(hw): fix taskmanager start dir
            remote_log_dir_suffix = "/log"

        log.info("get %s from %s", config_name, remote_config_file)
        _, stdout, _ = self.ssh_client.exec_command(f"grep {config_name} {remote_config_file}")
        grep_str = stdout.read()
        # if set openmldb_log_dir in config
        if grep_str:
            # TODO(hw):
            log.warning('unfinished, if taskmanager, from bin/. notice that #<config>=<> comment')
            # remote_log_dir_suffix = "/foo"
        return server_info.path + remote_log_dir_suffix

    def remote_log_file_list(self, server_info, last_n=2):
        """
        if role is taskmanager, ...
        if ns or tablet, ...

        :param server_info:
        :param last_n:
        :return:
        """
        self.ssh_client.connect(hostname=server_info.host)
        remote_config_file = server_info.make_paths_of_conf('')[0]
        remote_log_dir = self.get_log_dir_from_conf(remote_config_file, server_info)
        log.info("remote log dir is " + remote_log_dir)
        sftp = self.ssh_client.open_sftp()
        # if no the log dir, let it crash
        logs = [attr.__dict__ for attr in sftp.listdir_attr(remote_log_dir)]

        # taskmanager.log
        # TODO(hw): grep job log exception
        if server_info.role == "taskmanager":
            logs = list(filter(lambda di: 'taskmanager' in di['filename'], logs))
        else:
            # info log
            logs = list(filter(lambda di: 'info' in di['filename'], logs))
            # TODO(hw):  warn log?

        # sort by modify time
        logs.sort(key=lambda x: x["st_mtime"], reverse=True)
        log.info("all_logs(sorted): %s", logs)
        # get last n
        logs = [log_attr['filename'] for log_attr in logs[:last_n]]
        log.info("get last %d: %s", last_n, logs)
        return remote_log_dir, logs
