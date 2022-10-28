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
import sys
import logging
log = logging.getLogger(__name__)
from tool import *
import time
from optparse import OptionParser
parser = OptionParser()

parser.add_option("--openmldb_bin_path",
                  dest="openmldb_bin_path",
                  help="the openmldb bin path")

parser.add_option("--zk_cluster", 
                  dest="zk_cluster",
                  help="the zookeeper cluster")

parser.add_option("--zk_root_path",
                  dest="zk_root_path",
                  help="the zookeeper root path")

parser.add_option("--cmd",
                  dest="cmd",
                  help="cmd")


INTERNAL_DB = ["__INTERNAL_DB", "__PRE_AGG_DB", "INFORMATION_SCHEMA"]

def CheckTable(executor, db, table_name):
    status, table_partitions = executor.GetTablePartition(db, table_name)
    if not status.OK():
        return status
    endpoints = set()
    for pid, partitions in table_partitions.items():
        for partition in partitions:
            endpoints.add(partition.GetEndpoint())
            if not partition.IsAlive():
                return Status(-1, f"partition is not alive. {db} {table_name} {pid} {partition.GetEndpoint()}")
    endpoint_status = {}
    for endpoint in endpoints:
        status, result = executor.GetTableStatus(endpoint)
        if not status.OK():
            return status
        endpoint_status[endpoint] = result
    for pid, partitions in table_partitions.items():
        for partition in partitions:
            endpoint = partition.GetEndpoint()
            key = "{}_{}".format(partition.GetTid(), pid)
            if endpoint not in endpoint_status or key not in endpoint_status[endpoint]:
                return Status(-1, f"not table partition in {endpoint}")
            if partition.IsLeader() and endpoint_status[endpoint][key][3] == "kTableLeader":
                continue
            elif not partition.IsLeader() and endpoint_status[endpoint][key][3] != "kTableLeader":
                continue
            return Status(-1, f"role is not match")
    return Status()

def RecoverPartition(executor, db, partitions, endpoint_status):
    leader_pos = -1
    max_offset = 0
    table_name = partitions[0].GetName()
    pid = partitions[0].GetPid()
    for pos in range(len(partitions)):
        partition = partitions[pos]
        if partition.IsLeader() and partition.GetOffset() >= max_offset:
            leader_pos = pos
    if leader_pos < 0:
        log.error(f"cannot find leader partition. db {db} name {table_name} partition {pid}")
        return Status(-1, "recover partition failed")
    tid = partitions[0].GetTid()
    leader_endpoint = partitions[leader_pos].GetEndpoint()
    # recover leader
    if f"{tid}_{pid}" not in endpoint_status[leader_endpoint]:
        log.info(f"leader partition is not in tablet, db {db} name {table_name} pid {pid} endpoint {leader_endpoint}. start loading data...")
        if not executor.LoadTable(leader_endpoint, table_name, tid, pid).OK():
            log.error(f"load table failed. db {db} name {table_name} pid {pid} endpoint {leader_endpoint}")
            return Status(-1, "recover partition failed")
    if not partitions[leader_pos].IsAlive():
        status =  executor.UpdateTableAlive(db, table_name, pid, leader_endpoint, "yes")
        if not status.OK():
            log.error(f"update leader alive failed. db {db} name {table_name} pid {pid} endpoint {leader_endpoint}")
            return Status(-1, "recover partition failed")
    # recover follower
    for pos in range(len(partitions)):
        if pos == leader_pos:
            continue
        partition = partitions[pos]
        endpoint = partition.GetEndpoint()
        if partition.IsAlive():
            status = executor.UpdateTableAlive(db, table_name, pid, endpoint, "no")
            if not status.OK():
                log.error(f"update alive failed. db {db} name {table_name} pid {pid} endpoint {endpoint}")
                return Status(-1, "recover partition failed")
        if not executor.RecoverTablePartition(db, table_name, pid, endpoint).OK():
            log.error(f"recover table partition failed. db {db} name {table_name} pid {pid} endpoint {endpoint}")
            return Status(-1, "recover table partition failed")

def RecoverTable(executor, db, table_name):
    if CheckTable(executor, db, table_name).OK():
        log.info(f"{table_name} in {db} is healthy")
        return
    log.info(f"recover {table_name} in {db}")
    status, table_info = executor.GetTableInfo(db, table_name)
    if not status.OK():
        log.warn(f"get table info failed. msg is {status.GetMsg()}")
        return
    partition_dict = executor.ParseTableInfo(table_info)
    endpoints = set()
    for record in table_info:
        endpoints.add(record[3])
    endpoint_status = {}
    for endpoint in endpoints:
        status, result = executor.GetTableStatus(endpoint)
        if not status.OK():
            log.warn(f"get table status failed. msg is {status.GetMsg()}")
            return
        endpoint_status[endpoint] = result
    # print(endpoint_status)
    tid = int(table_info[-1][1])
    max_pid = int(table_info[-1][2])
    for pid in range(max_pid + 1):
        RecoverPartition(executor, db, partition_dict[str(pid)], endpoint_status)
    # wait op
    while True:
        status, result = executor.ShowOpStatus(db, table_name)
        is_finish = True
        if status.OK():
            for record in result:
                if record[4] == 'kDoing':
                    value = " ".join(record)
                    log.info(value)
                    is_finish = False
                    break
        if not is_finish:
            log.info(f"waiting task")
            time.sleep(2)
        else:
            break
    status = CheckTable(executor, db, table_name)
    if status.OK():
        log.info(f"{table_name} in {db} recover success")
    else:
        log.warn(status.GetMsg())

def RecoverData(executor):
    status, auto_failover = executor.GetAutofailover()
    if not status.OK():
        log.warn("get failover failed")
        return
    if auto_failover:
        if not executor.SetAutofailover("false").OK():
            log.warn("set auto_failover failed")
            return

    status, dbs = executor.GetAllDatabase()
    if not status.OK():
        return
    alldb = list(INTERNAL_DB)
    alldb.extend(dbs)
    for db in alldb:
        status, tables = executor.GetAllTable(db)
        if not status.OK():
            continue
        for name in tables:
            RecoverTable(executor, db, name)

    if auto_failover:
        if not executor.SetAutofailover("true").OK():
            log.warn("set auto_failover failed")

if __name__ == "__main__":
    (options, args) = parser.parse_args()
    executor = Executor(options.openmldb_bin_path, options.zk_cluster, options.zk_root_path)
    RecoverData(executor)
