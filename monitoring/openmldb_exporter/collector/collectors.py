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
from openmldb_exporter.collector import (connected_seconds, component_status, table_rows, table_partitions,
                                table_partitions_unalive, table_memory, table_disk, table_replica, deploy_response_time,
                                tablet_memory_application, tablet_memory_actual)
from urllib import request
import time

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
            table_rows.labels(tb_path, tid, storage_type).set(int(rows))
            table_partitions.labels(tb_path, tid, storage_type).set(int(partition))
            table_partitions_unalive.labels(tb_path, tid, storage_type).set(int(partition_unalive))
            table_replica.labels(tb_path, tid, storage_type).set(int(replica))
            table_memory.labels(tb_path, tid, storage_type).set(int(mem))
            table_disk.labels(tb_path, tid, storage_type).set(int(disk))


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
        acc_map = {}
        while row is not None:
            logging.debug(row)

            dp_name, time_second, count, total = row
            time_second = float(time_second)

            # update bucket count
            for i, bound in enumerate(deploy_response_time._upper_bounds):
                if time_second <= bound:
                    # FIXME: handle Histogram reset correctly
                    deploy_response_time.labels(dp_name)._buckets[i].set(int(count))
                    break
            # update sum for each deploy
            if dp_name in acc_map:
                acc_map[dp_name] += float(total)
            else:
                acc_map[dp_name] = float(total)
            row = rs.fetchone()

        # write sums
        for key,value in acc_map.items():
            deploy_response_time.labels(key)._sum.set(value)

    def _init_table_info(self):
        # sql parser do not recognize quoted string
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
    component statistics and tablet memory collector
    '''

    def collect(self):
        rs = self._conn.execute("SHOW COMPONENTS")
        components = rs.fetchall()
        for row in components:
            logging.debug(row)

            endpoint = row[0]
            role = row[1]
            # connected time in millisecond
            connected_time = int(row[2])
            connect_duration = int(time.time()) - connected_time / 1000
            status = row[3]
            # set protected member for Counter is dangerous, though it seems the only way
            connected_seconds.labels(endpoint, role)._value.set(connect_duration)
            component_status.labels(endpoint, role).state(status)

            # collect tablet application memory
            if role == "tablet":
                app, actual = self._get_mem(f"http://{endpoint}/TabletServer/ShowMemPool")
                tablet_memory_application.labels(endpoint).set(app)
                tablet_memory_actual.labels(endpoint).set(actual)


    def _get_mem(self, url: str):
        memory_by_application = 0
        memory_acutal_used = 0
        # http request with 1s timeout
        with request.urlopen(url, timeout=1) as resp:
            for i in resp:
                line = i.decode().strip()
                if line.rfind("use by application") > 0:
                    try:
                        memory_by_application = int(line.split()[1])
                    except ValueError as e:
                        memory_by_application = 0
                        logging.error(e)
                elif line.rfind("Actual memory used") > 0:
                    try:
                        memory_acutal_used = int(line.split()[2])
                    except ValueError as e:
                        memory_acutal_used = 0
                        logging.error(e)
                else:
                    continue
        return memory_by_application, memory_acutal_used
