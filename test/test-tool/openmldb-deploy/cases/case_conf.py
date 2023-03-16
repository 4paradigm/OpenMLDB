#!/usr/bin/env python3
# -*- coding: utf-8 -*-
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
"""
OpenMLDB Cluster config
"""
import configparser
import os
import sys
from os import path

abs_path=path.abspath(__file__)
dirname,filename=path.split(abs_path)

conf = {}
conf["zk_cluster"] = ""
conf["zk_root_path"] = "/openmldb"
conf["base_dir"] = path.dirname(path.dirname(path.dirname(path.dirname(dirname))))
conf["components"] = {}
cf = configparser.ConfigParser(strict=False, delimiters=" ", allow_no_value=True)
host_file = conf["base_dir"] + "/openmldb/conf/hosts"
cf.read(host_file)
for sec in cf.sections():
    if sec != "zookeeper":
        conf["components"].setdefault(sec, [])
        conf["components"][sec] = [k for k, v in cf[sec].items()]
    else:
        for k, v in cf[sec].items():
            endpoint = ":".join(k.split(":")[:2])
            if conf["zk_cluster"] == "":
               conf["zk_cluster"] = endpoint
            else:
               conf["zk_cluster"] = conf["zk_cluster"] + "," + endpoint
#print(conf)
