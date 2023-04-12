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

import configparser
from tool import SystemUtil
from tool import Status

class ClusterManager:
    def __init__(self, hosts_file) -> None:
        self.conf = {}
        self.conf["zk_root_path"] = "/openmldb"
        cf = configparser.ConfigParser(strict=False, delimiters=" ", allow_no_value=True)
        cf.read(hosts_file)
        for sec in cf.sections():
            self.conf.setdefault(sec, {})
            if sec != "zookeeper":
                for key, value in cf[sec].items():
                    self.conf[sec][key] = value
            else:
                for key, value in cf[sec].items():
                    endpoint = ":".join(key.split(":")[:2])
                    self.conf[sec][endpoint] = value
                    if "zk_cluster" not in self.conf:
                        self.conf["zk_cluster"] = endpoint
                    else:
                        self.conf["zk_cluster"] = conf["zk_cluster"] + "," + endpoint

    @staticmethod
    def GenHosts(base_path, ns_num, tablet_num, apiserver_num, taskmanager_num, hosts_file):
        tablet_start_port = 10921
        ns_start_port = 7527
        apiserver_start_port = 9080
        zk_port = 2181
        cf = configparser.ConfigParser(strict=False, delimiters=" ", allow_no_value=True)
        cf.add_section("tablet")
        for i in range(tablet_num):
            endpoint = f"localhost:{tablet_start_port + i}"
            path = f"{base_path}/openmldb/tablet-{i}"
            cf.set("tablet", endpoint, path)
        cf.add_section("nameserver")
        for i in range(ns_num):
            endpoint = f"localhost:{ns_start_port + i}"
            path = f"{base_path}/openmldb/ns-{i}"
            cf.set("nameserver", endpoint, path)
        if apiserver_num > 0:
            cf.add_section("apiserver")
            for i in range(apiserver_num):
                endpoint = f"localhost:{apiserver_start_port + i}"
                path = f"{base_path}/openmldb/apiserver-{i}"
                cf.set("apiserver", endpoint, path)
        cf.add_section("zookeeper")
        cf.set("zookeeper", f"localhost:{zk_port}:2888:3888", f"{base_path}/openmldb/zookeeper")
        with open(hosts_file, "w") as f:
           cf.write(f)

    def GetConf(self) -> dict:
        return self.conf

    def GetBinPath(self) -> str:
        for k, v in self.conf["tablet"].items():
            return self.conf["tablet"][k] + "/bin/openmldb"

    def GetComponent(self, role: str, pos: int = 0) -> str:
        if role not in self.conf or pos >= len(self.conf[role]):
            return None
        i = 0
        for endpoint, _ in self.conf[role].items():
            if pos == i:
                return endpoint
            i += 1

    def GetComponents(self, role: str) -> list:
        if role not in self.conf:
            return None
        return list(self.conf[role].keys())

    def OperateComponent(self, role: str, endpoint: str, op: str) -> Status:
        if role not in self.conf:
            return Status(-1, f"{role} is not exist in conf")
        if endpoint not in self.conf[role]:
            return Status(-1, f"{endpoint} is not exist in conf")
        if op not in ["start", "stop", "restart"]:
            return Status(-1, f"invalid operation {op}")
        path = self.conf[role][endpoint]
        cmd = ["bash", f"{path}/bin/start.sh", op, role]
        status, output = SystemUtil.ExecuteCmd(cmd)
        return status

    def StartComponent(self, role: str, endpoint: str) -> Status:
        return self.OperateComponent(role, endpoint, "start")
    
    def RestartComponent(self, role: str, endpoint: str) -> Status:
        return self.OperateComponent(role, endpoint, "restart")
    
    def StopComponent(self, role: str, endpoint: str) -> Status:
        return self.OperateComponent(role, endpoint, "stop")
