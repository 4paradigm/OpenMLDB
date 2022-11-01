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
import logging
import os
import subprocess
import sys
import time
log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format = '%(levelname)s: %(message)s')

USE_SHELL = sys.platform.startswith("win")
class Status:
    def __init__(self, code = 0, msg = "ok"):
        self.code = code
        self.msg = msg

    def OK(self):
        return True if self.code == 0 else False

    def GetMsg(self):
        return self.msg

    def GetCode(self):
        return self.code

class Partition:
    def __init__(self, name, tid, pid, endpoint, is_leader, is_alive, offset):
        self.name = name
        self.tid = tid
        self.pid = pid
        self.endpoint = endpoint
        self.is_leader = is_leader
        self.is_alive = is_alive
        self.offset = offset

    def GetTid(self):
        return self.tid
    def GetName(self):
        return self.name
    def GetPid(self):
        return self.pid
    def GetOffset(self):
        return self.offset
    def GetEndpoint(self):
        return self.endpoint
    def IsAlive(self):
        return self.is_alive
    def IsLeader(self):
        return self.is_leader
    def GetKey(self):
        return "{}_{}".format(self.tid, self.pid)

class Executor:
    def __init__(self, openmldb_bin_path, zk_cluster, zk_root_path):
        self.openmldb_bin_path = openmldb_bin_path
        self.zk_cluster = zk_cluster
        self.zk_root_path = zk_root_path
        self.ns_base_cmd = [self.openmldb_bin_path,
                            "--zk_cluster=" + self.zk_cluster,
                            "--zk_root_path=" + self.zk_root_path,
                            "--role=ns_client",
                            "--interactive=false"]
        self.tablet_base_cmd = [self.openmldb_bin_path, "--role=client", "--interactive=false"]
        status, endpoint = self.GetNsLeader()
        if status.OK():
            self.ns_leader = endpoint
            log.info(f"ns leader: {self.ns_leader}")
            self.ns_base_cmd = [self.openmldb_bin_path,
                                "--endpoint=" + self.ns_leader,
                                "--role=ns_client",
                                "--interactive=false"]

    def RunWithRetuncode(self, command,
                         universal_newlines = True,
                         useshell = USE_SHELL,
                         env = os.environ) -> tuple(Status, str):
        try:
            p = subprocess.Popen(command, stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell = useshell, universal_newlines = universal_newlines, env = env)
            output = p.stdout.read()
            p.wait()
            errout = p.stderr.read()
            p.stdout.close()
            p.stderr.close()
            return Status(p.returncode, errout), output
        except Exception as ex:
            return Status(-1, ex), None

    def GetNsLeader(self) -> tuple(Status, str):
        cmd = list(self.ns_base_cmd)
        cmd.append("--cmd=showns")
        status, output = self.RunWithRetuncode(cmd)
        if status.OK():
            result = self.ParseResult(output)
            for record in result:
                if record[2] == "leader":
                    return Status(), record[0]
        return Status(-1, "get ns leader falied"), None


    def ParseResult(self, output) -> list:
        result = []
        lines = output.split("\n")
        content_is_started = False
        for line in lines:
            if line.startswith("---------"):
                content_is_started = True
                continue
            if not content_is_started:
                continue
            record = line.split()
            if len(record) > 0:
                result.append(record)
        return result

    def GetAutofailover(self) -> tuple(Status, bool):
        cmd = list(self.ns_base_cmd)
        cmd.append("--cmd=confget auto_failover")
        status, output = self.RunWithRetuncode(cmd)
        if not status.OK():
            return status, None
        if output.find("true") != -1:
            return Status(), True
        return Status(), False;

    def SetAutofailover(self, value) -> Status:
        cmd = list(self.ns_base_cmd)
        cmd.append("--cmd=confset auto_failover " + value)
        status, output = self.RunWithRetuncode(cmd)
        return status

    def GetAllDatabase(self) -> tuple(Status, list):
        cmd = list(self.ns_base_cmd)
        cmd.append("--cmd=showdb")
        status, output = self.RunWithRetuncode(cmd)
        if not status.OK():
            return status, None
        dbs = []
        for record in self.ParseResult(output):
            if len(record) < 2:
                continue
            dbs.append(record[1])
        return Status(), dbs

    def GetTableInfo(self, database, table_name = '') -> tuple(Status, list):
        cmd = list(self.ns_base_cmd)
        cmd.append("--cmd=showtable " + table_name)
        cmd.append("--database=" + database)
        status, output = self.RunWithRetuncode(cmd)
        if not status.OK():
            return status, None
        result = []
        for record in self.ParseResult(output):
            if len(record) < 4:
                continue
            result.append(record)
        return Status(), result

    def ParseTableInfo(self, table_info) -> dict[str, list[Partition]]:
        result = {}
        for record in table_info:
            is_leader = True if record[4] == "leader" else False
            is_alive = True if record[5] == "yes" else False
            partition = Partition(record[0], record[1], record[2], record[3], is_leader, is_alive, int(record[6]));
            result.setdefault(record[2], [])
            result[record[2]].append(partition)
        return result

    def GetTablePartition(self, database, table_name) -> tuple(Status, dict):
        status, result = self.GetTableInfo(database, table_name)
        if not status.OK:
            return status, None
        partition_dict = self.ParseTableInfo(result)
        return Status(), partition_dict

    def GetAllTable(self, database) -> tuple[Status, list[Partition]]:
        status, result = self.GetTableInfo(database)
        if not status.OK():
            return status, None
        tables = []
        for partition in result:
            if partition[0] not in tables:
                tables.append(partition[0])
        return Status(), tables

    def GetTableStatus(self, endpoint, tid = '', pid = '') -> tuple(Status, dict):
        cmd = list(self.tablet_base_cmd)
        cmd.append("--endpoint=" + endpoint)
        cmd.append("--cmd=gettablestatus " + tid + " " + pid)
        status, output = self.RunWithRetuncode(cmd)
        if not status.OK():
            return status, None
        result = {}
        for record in self.ParseResult(output):
            if len(record) < 4:
                continue
            key = "{}_{}".format(record[0], record[1])
            result[key] = record
        return Status(), result

    def LoadTable(self, endpoint, name, tid, pid, sync = True) -> Status:
        cmd = list(self.tablet_base_cmd)
        cmd.append("--endpoint=" + endpoint)
        cmd.append("--cmd=loadtable {} {} {} 0 8".format(name, tid, pid))
        status, output = self.RunWithRetuncode(cmd)
        if status.OK() and output.find("LoadTable ok") != -1:
            if not sync:
                return Status()
            while True:
                status, result = self.GetTableStatus(endpoint, tid, pid)
                key = "{}_{}".format(tid, pid)
                if status.OK() and key in result:
                    table_stat = result[key][4]
                    if table_stat == "kTableNormal":
                        return Status()
                    elif table_stat == "kLoading" or table_stat == "kTableUndefined":
                        log.info(f"table is loading... tid {tid} pid {pid}")
                        time.sleep(2)
                    else:
                        return Status(-1, "load table failed")

        return Status(-1, "load table failed")

    def GetLeaderFollowerOffset(self, endpoint, tid, pid) -> tuple(Status, list):
        cmd = list(self.tablet_base_cmd)
        cmd.append("--endpoint=" + endpoint)
        cmd.append("--cmd=getfollower {} {}".format(tid, pid))
        status, output = self.RunWithRetuncode(cmd)
        if not status.OK():
            return status
        return Status(), self.ParseResult(output)

    def RecoverTablePartition(self, database, name, pid, endpoint) -> Status:
        cmd = list(self.ns_base_cmd)
        cmd.append("--cmd=recovertable {} {} {}".format(name, pid, endpoint))
        cmd.append("--database=" + database)
        status, output = self.RunWithRetuncode(cmd)
        if status.OK() and output.find("recover table ok") != -1:
            return Status()
        return Status(-1, "recover table failed")

    def UpdateTableAlive(self, database, name, pid, endpoint, is_alive) -> Status:
        if is_alive not in ["yes", "no"]:
            return Status(-1, "invalid argument {is_alive}")
        cmd = list(self.ns_base_cmd)
        cmd.append("--cmd=updatetablealive {} {} {} {}".format(name, pid, endpoint, is_alive))
        cmd.append("--database=" + database)
        status, output = self.RunWithRetuncode(cmd)
        if status.OK() and output.find("update ok") != -1:
            return Status()
        return Status(-1, "update table alive failed")

    def ChangeLeader(self, database, name, pid) -> Status:
        cmd = list(self.ns_base_cmd)
        cmd.append("--cmd=changeleader {} {} auto".format(name, pid))
        cmd.append("--database=" + database)
        status, output = self.RunWithRetuncode(cmd)
        return status

    def ShowOpStatus(self, database, name, pid = '') -> tuple(Status, list):
        cmd = list(self.ns_base_cmd)
        cmd.append("--cmd=showopstatus {} {} ".format(name, pid))
        cmd.append("--database=" + database)
        status, output = self.RunWithRetuncode(cmd)
        if not status.OK():
            return status, None
        return Status(), self.ParseResult(output)

    def CancelOp(self, database, op_id) -> Status:
        cmd = list(self.ns_base_cmd)
        cmd.append("--cmd=cancelop {}".format(op_id))
        cmd.append("--database=" + database)
        status, output = self.RunWithRetuncode(cmd)
        return status

    def Migrate(self, database, name, pid, src_endpoint, desc_endpoint) -> Status:
        cmd = list(self.ns_base_cmd)
        cmd.append("--cmd=migrate {} {} {} {}".format(src_endpoint, name, pid, desc_endpoint))
        cmd.append("--database=" + database)
        status, output = self.RunWithRetuncode(cmd)
        if status.OK() and output.find("migrate ok") != -1:
            return Status()
        return Status(-1, "migrate failed")

    def ShowTablet(self) -> tuple(Status, list):
        cmd = list(self.ns_base_cmd)
        cmd.append("--cmd=showtablet")
        status, output = self.RunWithRetuncode(cmd)
        if not status.OK():
            return status, None
        return Status(), self.ParseResult(output)

    def AddReplica(self, database, name, pid, endpoint) -> Status:
        cmd = list(self.ns_base_cmd)
        cmd.append("--cmd=addreplica {} {} {}".format(name, pid, endpoint))
        cmd.append("--database=" + database)
        status, output = self.RunWithRetuncode(cmd)
        if status.OK() and output.find("ok") != -1:
            return Status()
        return Status(-1, "add replica failed")

    def DelReplica(self, database, name, pid, endpoint) -> Status:
        cmd = list(self.ns_base_cmd)
        cmd.append("--cmd=delreplica {} {} {}".format(name, pid, endpoint))
        cmd.append("--database=" + database)
        status, output = self.RunWithRetuncode(cmd)
        if status.OK() and output.find("ok") != -1:
            return Status()
        return Status(-1, "del replica failed")