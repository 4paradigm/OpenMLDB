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

from diagnostic_tool.dist_conf import DistConf, ALL_SERVER_ROLES


def get_config_paths(endpoint, role, remote_root, local_root):
    config_name = f'{role}.flags' if role != 'taskmanager' else f'{role}.properties'
    local_prefix = f"{endpoint}-{role}"
    return f"{remote_root}/conf/{config_name}", f"{local_root}/{local_prefix}/{config_name}"


def get_log_paths(endpoint, role, remote_log_dir, log_names, dest):
    # TODO(hw): openmldb glog config? will it get a too large log file? fix the settings
    return [(f'{remote_log_dir}/{log_name}', f'{dest}/{endpoint}-{role}/{log_name}')
            for log_name in log_names]


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
        endpoints = self.dist_conf.get_from_all_hosts(lambda server: server['endpoint'].split(':')[0])
        for host in endpoints:
            self.ssh_client.connect(hostname=host)
            stdin, stdout, stderr = self.ssh_client.exec_command('whoami && pwd')
            logging.info(stdout.read())
            stderr_byte = stderr.read()
            if len(stderr_byte) != 0:
                logging.warning("got stderr when ping, " + str(stderr_byte))
                return False
        return True

    def pull_file(self, remote_host, paths):
        remote_path, local_path = paths[0], paths[1]
        logging.info(f"remote {remote_path}, local: {local_path}")
        self.ssh_client.connect(hostname=remote_host)
        sftp = self.ssh_client.open_sftp()
        try:
            # ensure local path is exists
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            # local path must be a file, not a dir
            sftp.get(remote_path, local_path)
        except Exception as e:
            logging.warning(f"pull from remote {remote_host}:{remote_path} error on , err: {e}")
            return False
        return True

    def pull_config_files(self, dest) -> bool:
        host_path_dict = {}
        self.dist_conf.map(ALL_SERVER_ROLES, None, host_path_dict)
        for role, servers_conf in host_path_dict.items():
            for server_conf in servers_conf:
                endpoint = server_conf['endpoint']
                host = endpoint.split(':')[0]
                path = server_conf['path']
                ok = self.pull_file(host, get_config_paths(endpoint, role, path, dest))
                if not ok:
                    return False
        return True

    def pull_log_files(self, dest) -> bool:
        host_path_dict = {}
        # TODO(hw): taskmanager
        self.dist_conf.map(['nameserver', 'tablet'], None, host_path_dict)
        for role, servers_conf in host_path_dict.items():
            for server_conf in servers_conf:
                endpoint = server_conf['endpoint']
                host = endpoint.split(':')[0]
                path = server_conf['path']
                # get log dir
                remote_log_dir, log_names = self.remote_log_file_list(host, role, path)
                ok = self.pull_files(host, get_log_paths(endpoint, role, remote_log_dir, log_names, dest))
                if not ok:
                    return False
        return True

    def pull_files(self, remote_host, paths_list) -> bool:
        for paths in paths_list:
            if not self.pull_file(remote_host, paths):
                return False
        return True

    def remote_log_file_list(self, remote_host, role, path, last_n=2):
        self.ssh_client.connect(hostname=remote_host)
        remote_config_file = get_config_paths(remote_host, role, path, '')[0]
        logging.info("get openmldb_log_dir from %s", remote_config_file)
        _, stdout, _ = self.ssh_client.exec_command(f"grep openmldb_log_dir {remote_config_file}")
        grep_str = stdout.read()
        # default log dir
        remote_log_dir = path + '/logs'
        # if set openmldb_log_dir in config
        if grep_str:
            logging.warning('unfinished')
            return '', []

        logging.info("remote log dir is " + remote_log_dir)
        sftp = self.ssh_client.open_sftp()
        # if no the log dir, let it crash
        logs = [attr.__dict__ for attr in sftp.listdir_attr(remote_log_dir)]
        # sort by modify time
        logs.sort(key=lambda x: x["st_mtime"], reverse=True)
        logging.info("all_logs: %s", logs)
        # get last n
        logs = [log_attr['filename'] for log_attr in logs[:last_n]]
        logging.info("get last %d: %s", last_n, logs)
        return remote_log_dir, logs

    # TODO(hw): exec -version
    def collect_version(self):
        pass
