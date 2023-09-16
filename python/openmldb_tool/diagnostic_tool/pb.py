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

from google.protobuf.descriptor import FieldDescriptor
from absl import flags

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
