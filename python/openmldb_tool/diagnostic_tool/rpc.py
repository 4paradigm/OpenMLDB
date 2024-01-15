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

import json
import requests

from .server_checker import StatusChecker
from .connector import Connector

from absl import flags

# used by pb.py but set here for simplicity, we will check pbdir before call hint(import pb)
flags.DEFINE_string(
    "pbdir",
    "/tmp/diag_cache",
    "pb2 root dir, if not set, will use the <path>/pb2 directory in the same directory as this script",
)

def validate_ip_address(ip_string):
    # localhost:xxxx is valid ip too, ip must have at least one ":"
    return ip_string.find(":") != -1


host2service = {
    "nameserver": "NameServer",
    "taskmanager": "openmldb.taskmanager.TaskManagerServer",
    "tablet": "TabletServer",
}


class RPC:
    """rpc service"""

    def __init__(self, host) -> None:
        if validate_ip_address(host):
            self.endpoint = host
            self.host = "tablet"  # TODO: you can get ns/tm by name, it's not necessary to input ip
            self.service = host2service[self.host]
        else:
            self.host, self.endpoint, self.service = RPC.get_endpoint_service(
                host.lower()
            )

    def rpc_help(self):
        if self.host == "taskmanager":
            r = requests.post(f"http://{self.endpoint}")
        else:
            r = requests.post(f"http://{self.endpoint}/{self.service}")
        return RPC.parse_html(r.text)

    def rpc_exec(self, operation, field):
        r = requests.post(
            f"http://{self.endpoint}/{self.service}/{operation}", json=field
        )
        return r.text

    def hint(self, method):
        if not method:
            # show service name and all rpc methods
            print(self.rpc_help())
            return

        # if taskmanager, service in pb2 is TaskManagerServer
        service = (
            self.service
            if not self.service.endswith("TaskManagerServer")
            else "TaskManagerServer"
        )
        from .pb import DescriptorHelper

        ok, input_json = DescriptorHelper(service).get_input_json(method)
        if not ok:
            print(input_json) # if not ok, it's message
            return
        # input message to json style
        json_str = json.dumps(input_json, indent=4)
        print(
            f"You should input json like this:\n --field '{json_str}'"
        )
        print("ignore round brackets in the key, e.g. (required)")
        print('"<>" shows the data type, e.g. "<string>" means you should set string')

    def search_in(self, typ, info):
        for item in typ:
            if info in item.keys():
                return item[info]

    def __call__(self, operation, field):
        if not operation:
            text = self.rpc_help()
        else:
            text = self.rpc_exec(operation, field)
        print(text)

    def get_endpoint_service(host):
        conn = Connector()
        components_map = StatusChecker(conn)._get_components()
        if host.startswith("tablet"):
            num = int(host[6:]) - 1
            host = "tablet"
        else:
            assert host in ["ns", "tm"]
            num = 0
            host = "nameserver" if host == "ns" else "taskmanager"
        assert host in components_map, f"{host} not found in cluster"
        endpoint = components_map[host][num][0]

        service = host2service[host]
        return host, endpoint, service

    def parse_html(html):
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "html.parser")
        return soup.get_text("\n")
