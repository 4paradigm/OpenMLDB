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

from absl import flags
import json
import requests
from bs4 import BeautifulSoup
from google.protobuf.descriptor import FieldDescriptor

from .server_checker import StatusChecker
from .connector import Connector

flags.DEFINE_string(
    "pbdir",
    "/tmp/diag_cache",
    "pb2 root dir, if not set, will use the <path>/pb2 directory in the same directory as this script",
)


class DescriptorHelper:
    def __init__(self, service):
        # TODO(hw): symbol_database is useful?
        # lazy import
        assert flags.FLAGS.pbdir, "pbdir not set"
        import sys
        from pathlib import Path
        sys.path.append(Path(flags.FLAGS.pbdir).as_posix())
        import tablet_pb2
        import name_server_pb2
        import taskmanager_pb2
        
        pb_map = {
            "TabletServer": tablet_pb2,
            "NameServer": name_server_pb2,
            "TaskManagerServer": taskmanager_pb2,
            # "ApiServer": api_server_pb2,
            # "DataSync": data_sync_pb2,
        }
        self.descriptor = pb_map[service].DESCRIPTOR.services_by_name[service]

    def get_input_json(self, method):
        inp = self.descriptor.FindMethodByName(method).input_type
        return Field.to_json(inp)


class Field:
    def to_str(typ):
        typ2str = {
            FieldDescriptor.TYPE_DOUBLE: "double",
            FieldDescriptor.TYPE_FLOAT: "float",
            FieldDescriptor.TYPE_INT64: "int64",
            FieldDescriptor.TYPE_UINT64: "uint64",
            FieldDescriptor.TYPE_INT32: "int32",
            FieldDescriptor.TYPE_FIXED64: "fixed64",
            FieldDescriptor.TYPE_FIXED32: "fixed32",
            FieldDescriptor.TYPE_BOOL: "bool",
            FieldDescriptor.TYPE_STRING: "string",
            FieldDescriptor.TYPE_GROUP: "group",
            FieldDescriptor.TYPE_MESSAGE: "message",
            FieldDescriptor.TYPE_BYTES: "bytes",
            FieldDescriptor.TYPE_UINT32: "uint32",
        }
        return typ2str[typ]

    def to_json(field):
        # label optional, required, or repeated.
        label = {1: "optional", 2: "required", 3: "repeated"}
        if isinstance(field, FieldDescriptor):
            key = f"({label[field.label]})" + field.name
            if field.type == FieldDescriptor.TYPE_MESSAGE:
                value = Field.to_json(field.message_type)
            elif field.type == FieldDescriptor.TYPE_ENUM:
                value = "/".join([n.name for n in field.enum_type.values])
            else:
                value = Field.to_str(field.type)
            if field.label == 3:
                # json list style
                return {key: [value, "..."]}
            else:
                return {key: value}
        else:
            # field is a message
            if field.containing_type and [f.name for f in field.fields] == [
                "key",
                "value",
            ]:
                # treat key-value as map type, can't figure out custom type
                # TODO(hw): it's ok to pass a json list to proto map?
                return {"k": "v", "...": "..."}
            d = {}
            for f in field.fields:
                d.update(Field.to_json(f))
            return d


class RPC:
    """rpc service"""

    def __init__(self, host) -> None:
        self.host, self.endpoint, self.service = RPC.get_endpoint_service(host.lower())

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

    def hint(self, info):
        if not info:
            # show service name and all rpc methods
            print(self.rpc_help())
            return

        # input message to json style

        # if taskmanager, service in pb2 is TaskManagerServer
        service = (
            self.service
            if not self.service.endswith("TaskManagerServer")
            else "TaskManagerServer"
        )

        helper = DescriptorHelper(service)
        json_str = json.dumps(helper.get_input_json(info), indent=4)
        print(
            f"You should input json like this, ignore round brackets in the key and double quotation marks in the value: --field '{json_str}'"
        )

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
        host2service = {
            "nameserver": "NameServer",
            "taskmanager": "openmldb.taskmanager.TaskManagerServer",
            "tablet": "TabletServer",
        }
        service = host2service[host]
        return host, endpoint, service

    def parse_html(html):
        soup = BeautifulSoup(html, "html.parser")
        return soup.get_text("\n")
