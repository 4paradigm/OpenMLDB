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
main entry of openmldb prometheus exporter
"""

from urllib import request
import argparse
import os
import sys

from prometheus_client import start_http_server
from prometheus_client import Gauge

dir_name = os.path.dirname(os.path.realpath(sys.argv[0]))

def get_mem(url):
    memory_by_application = 0
    memory_central_cache_freelist = 0
    memory_acutal_used = 0
    with request.urlopen(url) as resp:
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


def parse_args():
    parser = argparse.ArgumentParser(description="OpenMLDB exporter")
    parser.add_argument("--config", type=str, help="path to config file")
    return parser.parse_args()


def main():
    args = parse_args()

    env_dist = os.environ
    port = 8000
    if "metric_port" in env_dist:
        port = int(env_dist["metric_port"])
    start_http_server(port)

    gauge = {}

    gauge["memory"] = Gauge(
        "openmldb_memory", "metric for OpenMLDB memory", ["type"])

if __name__ == "__main__":
    main()
