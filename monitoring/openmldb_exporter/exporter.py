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

import os
import sys
import logging
from typing import (Iterable)

from openmldb_exporter.collector import (ConfigStore, Collector, TableStatusCollector, DeployQueryStatCollector,
                                         ComponentStatusCollector)
from prometheus_client.twisted import MetricsResource
from sqlalchemy import engine
from twisted.internet import reactor, task
from twisted.web.resource import Resource
from twisted.web.server import Site

dir_name = os.path.dirname(os.path.realpath(sys.argv[0]))


def collect_task(collectors: Iterable[Collector]):
    for collector in collectors:
        try:
            logging.info("%s collecting", type(collector).__qualname__)
            collector.collect()
        except:
            logging.exception("error in %s", type(collector).__qualname__)


def main():
    cfg_store = ConfigStore()

    # assuming loglevel is bound to the string value obtained from the command line argument.
    log_level = cfg_store.get_log_level()
    logging.basicConfig(level=log_level)

    eng = engine.create_engine(f"openmldb:///?zk={cfg_store.zk_root}&zkPath={cfg_store.zk_path}")
    conn = eng.connect()

    collectors = (
        TableStatusCollector(conn),
        DeployQueryStatCollector(conn),
        ComponentStatusCollector(conn),
    )

    task.LoopingCall(collect_task, collectors).start(cfg_store.pull_interval)

    root = Resource()
    # child path must be bytes
    root.putChild(cfg_store.telemetry_path.encode(), MetricsResource())
    factory = Site(root)
    reactor.listenTCP(cfg_store.listen_port, factory)
    reactor.run()


if __name__ == "__main__":
    main()
