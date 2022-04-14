# Copyright 2022 4Paradigm
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
Collector definations
"""

from abc import ABC, abstractmethod
from sqlalchemy import (engine, Table, Column, MetaData, String, Integer)
from openmldb_collector import (connected_seconds, component_status, table_rows, table_partitions,
                                table_partitions_unalive, table_memory, table_disk, table_replica, deploy_response_time,
                                tablet_memory_application, tablet_memory_actual)
from urllib import request

from typing import (Iterable)
import logging

class Collector(ABC):
    '''
    ABC for OpenMLDB prometheus collectors
    '''
    @abstractmethod
    def collect(self):
        '''
        define how to collect and save metric values
        '''
        pass


class SDKConnectable(object):
    '''
    base class that hold a OpenMLDB connection through python sdk
    '''
    _conn: engine.Connection

    def __init__(self, conn: engine.Connection):
        self._conn = conn



class TableStatusCollector(Collector, SDKConnectable):
    '''
    table statistics metric collector
    '''

    def collect(self):
        rs = self._conn.execute("SHOW TABLE STATUS")
        rows = rs.fetchall()
        for row in rows:
            logging.debug(row)

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

class DeployQueryStatCollector(Collector, SDKConnectable):
    '''
    deploy query statistics collector
    '''
    _metadata: MetaData
    _deploy_response_time: Table

    def __init__(self, conn: engine.Connection):
        super().__init__(conn)
        self._init_table_info()

    def collect(self):
        rs = self._conn.execute(self._deploy_response_time.select())
        row = rs.fetchone()
        acc = 0
        while row is not None:
            logging.debug(row)

            dp_name, time, count, total = row
            time = float(time)
            acc += float(total)
            for i, bound in enumerate(deploy_response_time._upper_bounds):
                if time <= bound:
                    # FIXME: handle Histogram reset correctly
                    deploy_response_time.labels(dp_name)._buckets[i].set(int(count))
                    break
            deploy_response_time.labels(dp_name)._sum.set(acc)
            row = rs.fetchone()

    def _init_table_info(self):
        self._metadata = MetaData(schema="INFORMATION_SCHEMA", bind=self._conn, quote_schema=False)
        self._deploy_response_time = Table(
            "DEPLOY_RESPONSE_TIME",
            self._metadata,
            Column("DEPLOY_NAME", String, quote=False),
            Column("TIME", String, quote=False),
            Column("COUNT", Integer, quote=False),
            Column("TOTAL", String, quote=False),
            quote=False,
        )


class ComponentStatusCollector(Collector, SDKConnectable):
    '''
    component statistics collector
    '''

    def collect(self):
        rs = self._conn.execute("SHOW COMPONENTS")
        components = rs.fetchall()
        for row in components:
            logging.debug(row)

            endpoint = row[0]
            # connect time in millisecond
            connect_time = int(row[2])
            status = row[3]
            # set protected member for Counter is dangerous, though it seems the only way
            connected_seconds.labels(endpoint)._value.set(connect_time / 1000)
            component_status.labels(endpoint).state(status)

class AppMemCollector(Collector):
    '''
    collector for OpenMLDB instance memory statistics
    '''

    _endpoints: Iterable[str]

    def __init__(self, endpoints: Iterable[str]):
        self._endpoints = endpoints

    def collect(self):
        for endpoint in self._endpoints:
            app, actual = self._get_mem(f"{endpoint}/TabletServer/ShowMemPool")
            tablet_memory_application.labels(endpoint).set(app)
            tablet_memory_actual.labels(endpoint).set(actual)

    def _get_mem(self, url: str):
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
