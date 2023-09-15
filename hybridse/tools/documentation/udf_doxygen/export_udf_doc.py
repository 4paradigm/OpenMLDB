#!/usr/bin/env python3
"""
generate udf document from native source
"""

# -*- coding: utf-8 -*-
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

import os
import sys
import subprocess
import yaml

DOXYGEN_DIR = os.path.abspath(os.path.dirname(__file__))
HOME_DIR = os.path.join(DOXYGEN_DIR, "../../../..")
BUILD_DIR = os.path.abspath(os.path.join(HOME_DIR, "build"))
TMP_DIR = DOXYGEN_DIR


def export_yaml():
    if not os.path.exists(TMP_DIR):
        os.makedirs(TMP_DIR)
    ret = subprocess.call([
        os.path.join(
            BUILD_DIR, "hybridse/src/export_udf_info"), "--output_dir", TMP_DIR, "--output_file",
        "udf_defs.yaml"
    ])
    if ret != 0:
        print("Invoke native export udf binary failed")
        sys.exit(ret)


def process_doc(doc):
    lines = doc.split("\n")
    first_line_indent = -1
    for i in range(0, len(lines)):
        line = lines[i]
        if line.strip() == "":
            continue
        if first_line_indent < 0:
            first_line_indent = len(line) - len(line.lstrip())
        indent = min(len(line) - len(line.lstrip()), first_line_indent)
        lines[i] = lines[i][indent:]
    return "\n".join(lines)

def surrond_backquote(t):
    return f"`{t}`"

def to_list_type(t):
    return f"`list<{t}>`"


numeric_types = frozenset(["int16", "int32", "int64", "float", "double"])
all_basic_types = frozenset(["bool", "int16", "int32", "int64", "float", "double", "string", "timestamp", "date"])

numeric_types_code = list(map(surrond_backquote, numeric_types))
list_numeric_types_code = list(map(to_list_type, numeric_types))
all_types_code = list(map(surrond_backquote, all_basic_types))
list_all_types_code = list(map(to_list_type, all_basic_types))

def merge_arith_types(signature_set):
    found = True

    def _find_and_merge(arg_types, idx, list_ty, merge_ty):
        merge_keys = []
        if arg_types[idx] in list_ty:
            for dtype in list_ty:
                origin = arg_types[idx]
                arg_types[idx] = dtype
                cur_key = ", ".join(arg_types)
                arg_types[idx] = origin
                merge_keys.append(cur_key)
                if not cur_key in signature_set:
                    break
            else:
                for key in merge_keys:
                    signature_set.pop(key)
                arg_types[idx] = merge_ty
                new_key = ", ".join(arg_types)
                signature_set[new_key] = arg_types
                return True
        return False

    while found:
        found = False
        for key in signature_set:
            arg_types = [_ for _ in signature_set[key]]
            for i in range(len(arg_types)):
                if _find_and_merge(arg_types, i, all_types_code, "`any`"):
                    # NOTE: must merge any before number
                    found = True
                    break
                elif _find_and_merge(arg_types, i, list_all_types_code, "`list<any>`"):
                    found = True
                    break
                elif _find_and_merge(arg_types, i, numeric_types_code, "`number`"):
                    found = True
                    break
                elif _find_and_merge(arg_types, i, list_numeric_types_code, "`list<number>`"):
                    found = True
                    break
            if found:
                break

    return signature_set


types_map = {
    "bool": "`bool`",
    "int16": "`int16`",
    "int32": "`int32`",
    "int64": "`int64`",
    "float": "`float`",
    "double": "`double`",
    "timestamp": "`timestamp`",
    "date": "`date`",
    "row": "`row`",
    "string": "`string`",
    "list_bool": "`list<bool>`",
    "list_number": "`list<number>`",
    "list_timestamp": "`list<timestamp>`",
    "list_date": "`list<date>`",
    "list_row": "`list<row>`",
    "list_int16": "`list<int16>`",
    "list_int32": "`list<int32>`",
    "list_int64": "`list<int64>`",
    "list_float": "`list<float>`",
    "list_double": "`list<double>`",
    "list_string": "`list<string>`",
}


def make_header():
    with open(os.path.join(TMP_DIR, "udf_defs.yaml"), mode="r", encoding="utf-8") as yaml_file:
        udf_defs = yaml.safe_load(yaml_file.read())

    if not os.path.exists(DOXYGEN_DIR + "/udfs"):
        os.makedirs(DOXYGEN_DIR + "/udfs")

    fake_header = os.path.join(DOXYGEN_DIR + "/udfs/udfs.h")
    with open(fake_header, "w", encoding="utf-8") as header_file:
        # generate the helper function entry first

        for name in sorted(udf_defs.keys()):
            content = "/**\n"
            items = udf_defs[name]

            if isinstance(items, str):
                # alilas function
                content += items
                content += "\n*/\n"
                content += name + "();\n"
                header_file.write(content)
                continue

            for item in items:
                doc = item["doc"]
                if doc.strip() != "":
                    content += process_doc(doc)
                    break
            # \*\* is required to generate bold style line in markdown
            # pylint: disable=anomalous-backslash-in-string
            content += "\n\n\*\*Supported Types**:\n"
            sig_set = {}
            sig_list = []
            for item in items:
                is_variadic = item["is_variadic"]
                for sig in item["signatures"]:
                    arg_types = sig["arg_types"]
                    if is_variadic:
                        arg_types.append("...")
                    return_type = sig["return_type"]
                    for i in range(len(arg_types)):
                        if arg_types[i] in types_map:
                            arg_types[i] = types_map[arg_types[i]]
                    key = ", ".join(arg_types)
                    if key not in sig_set:
                        sig_set[key] = arg_types

            # merge for number type
            sig_set = merge_arith_types(sig_set)

            sig_list = sorted([_ for _ in sig_set])
            for sig in sig_list:
                content += "- [" + sig + "]\n"

            content += "\n*/\n"
            content += name + "();\n"
            header_file.write(content)


def doxygen():
    ret = subprocess.call(["doxygen"], cwd=DOXYGEN_DIR)
    if ret != 0:
        print("Invoke doxygen failed")
        sys.exit(ret)


if __name__ == "__main__":
    export_yaml()
    make_header()
    doxygen()
