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

from google.protobuf.descriptor import Descriptor, FieldDescriptor
from absl import flags


class DescriptorHelper:
    def __init__(self, service):
        # lazy import
        assert flags.FLAGS.pbdir, "pbdir not set"
        import sys
        from pathlib import Path

        sys.path.append(Path(flags.FLAGS.pbdir).as_posix())
        import tablet_pb2
        import name_server_pb2
        import taskmanager_pb2

        # google.protobuf.symbol_database can get service desc by name, but we have already included all pb2 files we need
        # just use one file
        pb_map = {
            "TabletServer": tablet_pb2,
            "NameServer": name_server_pb2,
            "TaskManagerServer": taskmanager_pb2,
            # "ApiServer": api_server_pb2,
            # "DataSync": data_sync_pb2,
        }
        self.descriptor = pb_map[service].DESCRIPTOR.services_by_name[service]
        # from google.protobuf import symbol_database
        # self.sym_db = symbol_database.Default()

    def get_input_json(self, method):
        m = self.descriptor.FindMethodByName(method)
        if not m:
            return False, f"method {method} not found"
        if not m.input_type.fields:  # e.g. ShowTabletRequest is emtpy
            return False, f"method {method} has no input"
        # GeneratedProtocolMessageType __dict__ is complex, can't use it directly
        # cl = self.sym_db.GetSymbol(m.input_type.full_name)

        # fields build a map, message is Descriptor, fields in msg is FieldDescriptor
        return True, Field.to_json(m.input_type)


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

        def is_map(f):
            # I'm a map(containing_type = who includes me and my fields name are key-value)
            # e.g. tm RunBatchSql --hint the conf field is map
            return f.containing_type and [ff.name for ff in f.fields] == [
                "key",
                "value",
            ]

        if isinstance(field, FieldDescriptor):
            if field.message_type:
                # message_type is a Descriptor, check if it's a map
                if is_map(field.message_type):
                    m = field.message_type
                    # treat key-value as map type, can't figure out custom type, no nested, so just generate here
                    return {
                        f"<{m.fields[0].name}>": f"<{m.fields[1].name}>",
                        "...": "...",
                    }
                else:
                    # normal nested message
                    return Field.to_json(field.message_type)
            elif field.type == FieldDescriptor.TYPE_ENUM:
                return "/".join([n.name for n in field.enum_type.values])
            else:
                return f"<{Field.to_str(field.type)}>"

        elif isinstance(field, Descriptor):
            d = {}
            for f in field.fields:
                # each one is FieldDescriptor
                # map is repeated too, but it's not a list
                if f.label == 3 and not is_map(f.message_type):
                    # json list style
                    d[f"({label[f.label]})" + f.name] = [Field.to_json(f), "..."]
                else:
                    d[f"({label[f.label]})" + f.name] = Field.to_json(f)
            return d
        else:
            raise ValueError(f"unknown type {type(field)}")
