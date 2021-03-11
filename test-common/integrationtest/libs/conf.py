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

import ConfigParser
import string
import os
import sys
import getpass

cur_user = getpass.getuser()
cf = ConfigParser.ConfigParser()
cf.read(os.getenv("testconfpath"))

failfast = cf.getboolean("test_opt", "failfast")

multidimension = cf.getboolean("dimension", "multidimension")
multidimension_vk = eval(cf.get("dimension", "multidimension_vk"))
multidimension_scan_vk = eval(cf.get("dimension", "multidimension_scan_vk"))

log_level = cf.get("log", "log_level")

#local env
if cf.has_option("tb_endpoints", cur_user):
    tb_endpoints = cf.get("tb_endpoints", cur_user).split(',')
else:
    tb_endpoints = cf.get("tb_endpoints", "others").split(',')

if cf.has_option("ns_endpoints", cur_user):
    ns_endpoints = cf.get("ns_endpoints", cur_user).split(',')
else:
    ns_endpoints = cf.get("ns_endpoints", "others").split(',')

if cf.has_option("zookeeper", cur_user):
    zk_endpoint = cf.get("zookeeper", cur_user)
else:
    zk_endpoint = cf.get("zookeeper", "others")

# remote env
if cf.has_option("tb_endpoints_r", cur_user):
    tb_endpoints_r = cf.get("tb_endpoints_r", cur_user).split(',')
else:
    tb_endpoints_r = cf.get("tb_endpoints_r", "others").split(',')

if cf.has_option("ns_endpoints_r", cur_user):
    ns_endpoints_r = cf.get("ns_endpoints_r", cur_user).split(',')
else:
    ns_endpoints_r = cf.get("ns_endpoints_r", "others").split(',')

table_meta_ele = {
    'table_partition': ['endpoint', 'pid_group', 'is_leader'],
    'column_desc': ['name', 'type', 'add_ts_idx']
}

rtidb_log_info = cf.get("rtidb", "log_level")

cluster_mode = cf.get("mode", "cluster_mode")
