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
import yaml

ALL_SERVER_ROLES = ['nameserver', 'tablet', 'taskmanager']


class DistConf:
    def __init__(self, conf_dict):
        self.full_conf = conf_dict
        self.mode = self.full_conf['mode']
        self.nameservers = self.full_conf['nameserver']
        self.tabletservers = self.full_conf['tablet']
        if self.mode == 'cluster':
            self.zk = self.full_conf['zookeeper']
            self.taskmanagers = self.full_conf['taskmanager']

    def __str__(self):
        return str(self.full_conf)

    def get_from_all_hosts(self, pred):
        return [pred(server) for server in self.nameservers] + \
               [pred(server) for server in self.tabletservers] + \
               [pred(server) for server in self.taskmanagers]

    def map(self, role_list, trans, result):
        for role in role_list:
            ss = self.full_conf[role]
            if ss:
                result[role] = []
                for s in ss:
                    result[role].append(trans(s) if trans is not None else s)


class DistConfReader:
    def __init__(self, config_path):
        with open(config_path, "r") as stream:
            self.dist_conf = DistConf(yaml.safe_load(stream))

    def conf(self):
        return self.dist_conf
