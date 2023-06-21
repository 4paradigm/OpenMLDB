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
import os
import requests
from bs4 import BeautifulSoup

from .server_checker import StatusChecker
from .connector import Connector


class RPC:
    """rpc service"""
    def __init__(self, host, operation, field) -> None:
        self.host = host
        self.host, self.endpoint, self.service = self._get_endpoint_service(self.host)
        self.operation = operation
        self.field = field

    def rpc_help(self):
        if self.host == "taskmanager":
            r = requests.post(f"http://{self.endpoint}")
        else:
            r = requests.post(f"http://{self.endpoint}/{self.service}")
        return self.parse_html(r.text)

    def rpc_exec(self):
        r = requests.post(f"http://{self.endpoint}/{self.service}/{self.operation}", json=self.field)
        return r.text

    def hint(self, info):
        dir_path = os.path.dirname(os.path.abspath(__file__))
        json_path = os.path.join(dir_path, "proto.json")
        with open(json_path, 'r') as file:
            proto = json.load(file)
        result = self.search_in(proto["enum"], info)
        if result is None:
            result = self.search_in(proto["message"], info)
            for sublist in result.values():
                for dictionary in sublist:
                    for key, value in dictionary.items():
                        if value and value[0].isupper():
                            self.hint(value)
        print(info, result)

    def search_in(self, typ, info):
        for item in typ:
            if info in item.keys():
                return item[info]

    def __call__(self):
        if not self.operation:
            text = self.rpc_help()
        else:
            text = self.rpc_exec()
        print(text)

    def _get_endpoint_service(self, host):
        conn = Connector()
        components_map = StatusChecker(conn)._get_components()
        if host[-1].isdigit():
            num = int(host[-1]) - 1
            host = host[:-1]
        else:
            num = 0
        endpoint = components_map[host][num][0]
        host2service = {
            "nameserver": "NameServer",
            "taskmanager": "openmldb.taskmanager.TaskManagerServer",
            "tablet": "TabletServer",
        }
        service = host2service[host]
        return host, endpoint, service

    def parse_html(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        return soup.get_text("\n")
