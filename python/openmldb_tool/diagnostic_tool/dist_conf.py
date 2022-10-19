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

import yaml

log = logging.getLogger(__name__)

ALL_SERVER_ROLES = ['nameserver', 'tablet', 'taskmanager']

CXX_SERVER_ROLES = ALL_SERVER_ROLES[:2]

JAVA_SERVER_ROLES = [ALL_SERVER_ROLES[2]]


class ServerInfo:
    def __init__(self, role, endpoint, path, is_local):
        self.role = role
        self.endpoint = endpoint
        self.path = path
        self.host = endpoint.split(':')[0]
        self.is_local = is_local

    def __str__(self):
        return f'Server[{self.role}, {self.endpoint}, {self.path}]'

    def is_taskmanager(self):
        return self.role == 'taskmanager'

    def conf_path(self):
        return f'{self.path}/conf'

    def bin_path(self):
        return f'{self.path}/bin'

    def taskmanager_path(self):
        return f'{self.path}/taskmanager'

    def conf_path_pair(self, local_root):
        config_name = f'{self.role}.flags' if self.role != 'taskmanager' \
            else f'{self.role}.properties'
        local_prefix = f'{self.endpoint}-{self.role}'
        return f'{self.path}/conf/{config_name}', f'{local_root}/{local_prefix}/{config_name}'

    def remote_log4j_path(self):
        return f'{self.path}/taskmanager/conf/log4j.properties'

    # TODO(hw): openmldb glog config? will it get a too large log file? fix the settings
    def remote_local_pairs(self, remote_dir, file, dest):
        return f'{remote_dir}/{file}', f'{dest}/{self.endpoint}-{self.role}/{file}'


class ServerInfoMap:
    def __init__(self, server_info_map):
        self.map = server_info_map

    def for_each(self, func, roles=None, check_result=True):
        """
        even some failed, call func for all
        :param roles:
        :param func:
        :param check_result:
        :return:
        """
        if roles is None:
            roles = ALL_SERVER_ROLES
        ok = True
        for role in roles:
            if role not in self.map:
                log.warning("role %s is not in map", role)
                ok = False
                continue
            for server_info in self.map[role]:
                res = func(server_info)
                if check_result and not res:
                    ok = False
        return ok


class DistConf:
    def __init__(self, conf_dict):
        self.full_conf = conf_dict
        self.mode = self.full_conf['mode']
        self.server_info_map = ServerInfoMap(
            self.map(ALL_SERVER_ROLES, lambda role, s: ServerInfo(role, s['endpoint'], s['path'],
                s['is_local'] if 'is_local' in s else False)))

    def __str__(self):
        return str(self.full_conf)

    def map(self, role_list, trans):
        result = {}
        for role in role_list:
            if role not in self.full_conf:
                continue
            ss = self.full_conf[role]
            if ss:
                result[role] = []
                for s in ss:
                    result[role].append(trans(role, s) if trans is not None else s)
        return result


class DistConfReader:
    def __init__(self, config_path):
        with open(config_path, "r") as stream:
            self.dist_conf = DistConf(yaml.safe_load(stream))

    def conf(self):
        return self.dist_conf

class ConfParser:
    def __init__(self, config_path):
        self.conf_map = {}
        with open(config_path, "r") as stream:
            for line in stream:
                item = line.strip()
                if item == '' or item.startswith('#'):
                    continue
                arr = item.split("=")
                if len(arr) != 2:
                    continue
                if arr[0].startswith('--'):
                    # for gflag
                    self.conf_map[arr[0][2:]] = arr[1]
                else:
                    self.conf_map[arr[0]] = arr[1]

    def conf(self):
        return self.conf_map
