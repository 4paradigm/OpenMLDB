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

import argparse
import os
import time
import sys

from prometheus_client import start_http_server
from prometheus_client import Gauge

dir_name = os.path.dirname(os.path.realpath(sys.argv[0]))

import urllib.request as request

if sys.version_info.major > 2:
    do_open = lambda filename, mode: open(filename, mode, encoding="utf-8")
else:
    do_open = lambda filename, mode: open(filename, mode)

method = ["put", "get", "scan", "query", "subquery", "batchquery", "sqlbatchrequestquery", "subbatchrequestquery"]
method_set = set(method)
monitor_key = [
    "count", "error", "qps", "latency", "latency_50", "latency_90", "latency_99", "latency_999", "latency_9999",
    "max_latency"
]
openmldb_logs = {
    "tablet": [{
        "file_name": "/tablet_mon.log",
        "offset": 0,
        "item": {
            "name": "restart_num",
            "key": "./bin/boot.sh",
            "num": 0
        }
    }, {
        "file_name": "/tablet.WARNING",
        "offset": 0,
        "item": {
            "name": "reconnect_num",
            "key": "reconnect zk",
            "num": 0
        }
    }],
    "ns": [{
        "file_name": "/nameserver_mon.log",
        "offset": 0,
        "item": {
            "name": "restart_num",
            "key": "./bin/boot.sh",
            "num": 0
        }
    }, {
        "file_name": "/nameserver.WARNING",
        "offset": 0,
        "item": {
            "name": "reconnect_num",
            "key": "reconnect zk",
            "num": 0
        }
    }]
}


def parse_data(resp):
    beginParse = False
    stageBegin = False
    server = ""
    servers = {}
    for i in resp:
        if len(i) < 0:
            continue
        l = i.decode()
        if beginParse and l[0] == '\n':
            stageBegin = False
            resp.readline()
            continue
        elif l[0] == '[':
            beginParse = True
            resp.readline()
            continue
        if not beginParse:
            continue
        if stageBegin:
            cols = l.strip().split()
            indicator = cols[0].strip(":")
            value = int(cols[1])
            servers[server][indicator] = value
        else:
            stageBegin = True
            server = l.split()[0]
            servers[server] = {}
    return servers


def get_data(url):
    with request.urlopen(url) as resp:
        return parse_data(resp)


def get_mem(url):
    memory_by_application = 0
    memory_central_cache_freelist = 0
    memory_acutal_used = 0
    resp = request.urlopen(url)
    for i in resp:
        line = i.decode().strip()
        if line.rfind("use by application") > 0:
            try:
                memory_by_application = int(line.split()[1])
            except Exception as e:
                memory_by_application = 0
        elif line.rfind("central cache freelist") > 0:
            try:
                memory_central_cache_freelist = line.split()[2]
            except Exception as e:
                memory_central_cache_freelist = 0
        elif line.rfind("Actual memory used") > 0:
            try:
                memory_acutal_used = line.split()[2]
            except Exception as e:
                memory_acutal_used = 0
        else:
            continue
    return memory_by_application, memory_central_cache_freelist, memory_acutal_used


def get_conf(conf_file):
    conf_map = {}
    if conf_file.startswith('/'):
        conf_path = conf_file
    else:
        conf_path = dir_name + '/' + conf_file
    print("reading config file: ", conf_path)
    with do_open(conf_path, "r") as conf_file:
        for line in conf_file:
            if line.startswith("#"):
                continue
            arr = line.split("=")
            if arr[0] == "tablet_endpoint":
                conf_map["endpoint"] = arr[1].strip()
            elif arr[0] == "tablet_log_dir":
                conf_map["tablet"] = arr[1].strip()
            elif arr[0] == "ns_log_dir":
                conf_map["ns"] = arr[1].strip()
            elif arr[0] == "port":
                conf_map["port"] = int(arr[1].strip())
            elif arr[0] == "interval":
                conf_map["interval"] = int(arr[1].strip())
        return conf_map


def get_timestamp():

    def big_int(ct):
        try:
            return long(ct)
        except:
            return int(ct)

    ct = time.time()
    local_time = time.localtime(ct)
    data_head = time.strftime("%Y-%m-%d %H:%M:%S", local_time)
    data_secs = (ct - big_int(ct)) * 1000
    today = time.strftime("%Y%m%d", local_time)
    return ["%s.%03d" % (data_head, data_secs) + " +0800", today]


def search_key(file_name, offset, keyword):
    if not os.path.exists(file_name):
        return (0, 0)
    count = 0
    with do_open(file_name, 'r') as f:
        f.seek(offset)
        for line in f:
            if line.find(keyword) != -1:
                count += 1
        return (count, f.tell())

def parse_args():
    parser = argparse.ArgumentParser(description='OpenMLDB exporter')
    parser.add_argument("--config", type=str, help="path to config file")
    return parser.parse_args()


def main():
    args = parse_args()

    conf_map = get_conf(args.config or 'openmldb_exporter.conf')
    env_dist = os.environ
    port = conf_map["port"]
    if "metric_port" in env_dist:
        port = int(env_dist["metric_port"])
    start_http_server(port)

    # three gauge metric built in, from service status(named api), log, application memory respectively
    # NOTE: those gauges are not finalized, any change will happen without warning
    gauge = {}

    # gauge for method provided by OpenMLDB, has two labels
    # 1. method: method name, e.g put, get
    # 2. category: currently two categories:
    #    1. latency related metric
    #    2. couter related: qps, error count
    gauge["api"] = Gauge("openmldb_api", "metric for OpenMLDB server api status", ["method", "category"])

    gauge["log"] = Gauge("openmldb_log", "metric for OpenMLDB log", ["role", "type"])
    gauge["memory"] = Gauge("openmldb_memory", "metric for OpenMLDB memory", ["type"])

    endpoint = ""
    if "endpoint" in conf_map:
        endpoint = conf_map["endpoint"]
    if "endpoint" in env_dist:
        endpoint = env_dist["endpoint"]
    url = ""
    mem_url = ""
    if endpoint != "":
        url = "http://" + endpoint + "/status"
        mem_url = "http://{}/TabletServer/ShowMemPool".format(endpoint)

    last_date = get_timestamp()[1]
    sleep_sec = conf_map["interval"]
    while True:
        try:
            time_stamp = get_timestamp()[0]
            new_date = get_timestamp()[1]

            # analysis openmldb logs
            for module in openmldb_logs.keys():
                if module not in conf_map:
                    continue
                for var in openmldb_logs[module]:
                    (count, offset) = search_key(conf_map[module] + var["file_name"], var["offset"], var["item"]["key"])
                    if var["item"]["name"] == "restart_num":
                        var["offset"] = offset
                        if count > 0:
                            var["item"]["num"] += count
                            openmldb_logs[module][1]["item"]["offset"] = 0
                            openmldb_logs[module][1]["item"]["num"] = 0
                        value = count - 1 if count > 0 else count
                        gauge["log"].labels(role=module, type=var["item"]["name"]).set(var["item"]["num"])
                    else:
                        var["item"]["num"] += count
                        var["offset"] = offset
                        gauge["log"].labels(role=module, type=var["item"]["name"]).set(str(var["item"]["num"]))

            # pull OpenMLDB metric status
            if url == "":
                continue
            result = get_data(url)
            for method_data in result:
                lower_method = method_data.lower()
                if lower_method not in method_set:
                    continue
                data = "{}\t{}\t".format(time_stamp, lower_method)
                for key in monitor_key:
                    data = "{}\t{}:{}".format(data, key, result[method_data][key])
                    gauge["api"].labels(method=lower_method, category=key).set(result[method_data][key])

            # pull OpenMLDB memory usage
            if len(mem_url) < 1:
                continue
            mema, memb, memc = get_mem(mem_url)
            gauge["memory"].labels(type="use_by_application").set(mema)
            gauge["memory"].labels(type="central_cache_freelist").set(memb)
            gauge["memory"].labels(type="actual_memory_used").set(memc)
        except Exception as e:
            print(time.ctime(), ", exception happened during pull metrics: ", e)
        finally:
            print(time.ctime(), ", pulled, sleep for ", sleep_sec, " seconds")
            time.sleep(sleep_sec)

if __name__ == "__main__":
    main()
