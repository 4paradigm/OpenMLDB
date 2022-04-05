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

import argparse
import os
import sys
from urllib import request

from openmldb_collector import connected_seconds, component_status, table_rows, table_partitions, table_partitions_unalive, table_memory, table_disk, table_replica, deploy_response_time
from prometheus_client import Gauge
from prometheus_client.twisted import MetricsResource
from sqlalchemy import engine, select
from twisted.internet import reactor
from twisted.web.resource import Resource
from twisted.web.server import Site

dir_name = os.path.dirname(os.path.realpath(sys.argv[0]))


class OpenMLDBScraper(object):
    '''
    basic class to pull OpenMLDB metrics through sdk
    '''
    conn: engine.Connection

    def __init__(self, conn):
        self.conn = conn

    def scrape_components(self):
        rs = self.conn.execute("SHOW COMPONENTS")
        components = rs.fetchall()
        for row in components:
            endpoint = row[0]
            # connect time in millisecond
            connect_time = int(row[2])
            status = row[3]
            # set protected member for Counter is dangerous, though it seems the only way
            connected_seconds.labels(endpoint)._value.set(connect_time / 1000)
            component_status.labels(endpoint).state(status)

    def scrape_deploy_response_time(self):
        rs = self.conn.execute("SELECT * FROM INFORMATION_SCHEMA.DEPLOY_RESPONSE_TIME")
        row = rs.fetchone()
        sum = 0
        while row != None:
            dp_name, time, count, total = row
            time = float(time) / 1000000
            sum += float(total) / 1000000
            for i, bound in enumerate(deploy_response_time._upper_bounds):
                if time <= bound:
                    # FIXME: handle Histogram reset correctly
                    deploy_response_time.labels(dp_name)._buckets[i].set(int(count))
                    break
            deploy_response_time.labels(dp_name)._sum.set(sum)
            row = rs.fetchone()

    def scrape_table_status(self):
        rs = self.conn.execute("SHOW TABLE STATUS")
        rows = rs.fetchall()
        for row in rows:
            # TODO: use storage_type
            tid, tb_name, db_name, storage_type, rows, mem, disk, partition, partition_unalive, replica, *_ = row
            tb_path = f"{db_name}_{tb_name}"
            tid = int(tid)
            table_rows.labels(tb_path, tid).set(int(rows))
            table_partitions.labels(tb_path, tid).set(int(partition))
            table_partitions_unalive.labels(tb_path, tid).set(int(partition_unalive))
            table_replica.labels(tb_path, tid).set(int(replica))
            table_memory.labels(tb_path, tid).set(int(mem))
            table_disk.labels(tb_path, tid).set(int(disk))


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

    gauge = {}

    gauge["memory"] = Gauge("openmldb_memory", "metric for OpenMLDB memory", ["type"])

    zk_root = "127.0.0.1:6181"
    zk_path = "/onebox"
    eng = engine.create_engine(f"openmldb:///?zk={zk_root}&zkPath={zk_path}")
    conn = eng.connect()

    scraper = OpenMLDBScraper(conn)
    scraper.scrape_components()
    scraper.scrape_table_status()
    scraper.scrape_deploy_response_time()

    root = Resource()
    root.putChild(b"metrics", MetricsResource())
    factory = Site(root)
    reactor.listenTCP(port, factory)
    reactor.run()


if __name__ == "__main__":
    main()
