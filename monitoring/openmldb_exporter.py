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
import logging
from urllib import request
from typing import Iterable

from openmldb_collector import (connected_seconds, component_status, table_rows, table_partitions,
                                table_partitions_unalive, table_memory, table_disk, table_replica, deploy_response_time,
                                tablet_memory_application, tablet_memory_actual)
from prometheus_client.twisted import MetricsResource
from sqlalchemy import engine, select
from twisted.internet import reactor, task
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
            time = float(time)
            sum += float(total)
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


def pull_db_stats(inst: OpenMLDBScraper):
    logging.info("running pull_db_stats")
    inst.scrape_components()
    inst.scrape_table_status()
    inst.scrape_deploy_response_time()


def pull_mem_stats(endpoint_list: Iterable[str]):
    logging.info("running pull_mem_stats")
    for endpoint in endpoint_list:
        app, actual = get_mem(f"{endpoint}/TabletServer/ShowMemPool")
        tablet_memory_application.labels(endpoint).set(app)
        tablet_memory_actual.labels(endpoint).set(actual)


def get_mem(url):
    memory_by_application = 0
    memory_acutal_used = 0
    with request.urlopen(url) as resp:
        for i in resp:
            line = i.decode().strip()
            if line.rfind("use by application") > 0:
                try:
                    memory_by_application = int(line.split()[1])
                except Exception as e:
                    memory_by_application = 0
            elif line.rfind("Actual memory used") > 0:
                try:
                    memory_acutal_used = int(line.split()[2])
                except Exception as e:
                    memory_acutal_used = 0
            else:
                continue
    return memory_by_application, memory_acutal_used


def parse_args():
    parser = argparse.ArgumentParser(description="OpenMLDB exporter")
    parser.add_argument("--config", type=str, help="path to config file")
    parser.add_argument("--log", type=str, default="INFO", help="config log level")
    parser.add_argument("--port", type=int, default=8000, help="process listen port")
    parser.add_argument("--zk_root", type=str, default="127.0.0.1:6181", help="endpoint to zookeeper")
    parser.add_argument("--zk_path", type=str, default="/", help="root path in zookeeper for OpenMLDB")
    return parser.parse_args()


def main():
    args = parse_args()

    # assuming loglevel is bound to the string value obtained from the
    # command line argument. Convert to upper case to allow the user to
    # specify --log=DEBUG or --log=debug
    numeric_level = getattr(logging, args.log.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {args.log}")
    logging.basicConfig(level=numeric_level)

    eng = engine.create_engine(f"openmldb:///?zk={args.zk_root}&zkPath={args.zk_path}")
    conn = eng.connect()

    scraper = OpenMLDBScraper(conn)

    repeated_task = task.LoopingCall(pull_db_stats, scraper)
    repeated_task.start(10.0)

    memory_task = task.LoopingCall(pull_mem_stats, [])
    memory_task.start(10.0)

    root = Resource()
    root.putChild(b"metrics", MetricsResource())
    factory = Site(root)
    reactor.listenTCP(args.port, factory)
    reactor.run()


if __name__ == "__main__":
    main()
