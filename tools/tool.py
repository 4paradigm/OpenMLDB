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
# http lib for python2 or 3
import json
try:
    import httplib
    import urllib
except ImportError:
    import http.client as httplib
    import urllib.parse as urllib

# for Python 2, don't use f-string
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

    def __str__(self):
        return "code: {code}, msg: {msg}".format(code = self.code, msg = self.msg)

class Partition:
    def __init__(self, name, tid, pid, endpoint, is_leader, is_alive, offset):
        self.name = name
        self.tid = tid
        self.pid = pid
        self.endpoint = endpoint
        self.is_leader = is_leader
        self.is_alive = is_alive
        self.offset = 0 if offset == "-" else int(offset)

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
        self.sql_base_cmd = [self.openmldb_bin_path,
                            "--zk_cluster=" + self.zk_cluster,
                            "--zk_root_path=" + self.zk_root_path,
                            "--role=sql_client",
                            "--interactive=false"]
        self.endpoint_map = {}

    def Connect(self):
        cmd = list(self.ns_base_cmd)
        cmd.append("--cmd=showns")
        status, output = self.RunWithRetuncode(cmd)
        if not status.OK() or status.GetMsg().find("zk client init failed") != -1:
            return Status(-1, "get ns failed")
        result = self.ParseResult(output)
        for record in result:
            if record[2] == "leader":
                self.ns_leader = record[0]
            if record[1] != '-':
                self.endpoint_map[record[0]] = record[1]
            else:
                self.endpoint_map[record[0]] = record[0]
        cmd = list(self.ns_base_cmd)
        cmd.append("--cmd=showtablet")
        status, output = self.RunWithRetuncode(cmd)
        if not status.OK():
            return Status(-1, "get tablet failed")
        result = self.ParseResult(output)
        for record in result:
            if record[1] != '-':
                self.endpoint_map[record[0]] = record[1]
            else:
                self.endpoint_map[record[0]] = record[0]


        log.info("ns leader: {ns_leader}".format(ns_leader = self.ns_leader))
        self.ns_base_cmd = [self.openmldb_bin_path,
                            "--endpoint=" + self.endpoint_map[self.ns_leader],
                            "--role=ns_client",
                            "--interactive=false"]
        return Status()

    def RunWithRetuncode(self, command,
                         universal_newlines = True,
                         useshell = USE_SHELL,
                         env = os.environ):
        try:
            log.info(" ".join(command))
            p = subprocess.Popen(command, stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell = useshell, universal_newlines = universal_newlines, env = env)
            output, errout = p.communicate()
            # TODO(hw): the print from ns/tablet client are not standard, print it for debug
            if output != "":
                log.info(output)
            # errout has glog output, don't print it
            if "error msg" in output:
                return Status(-1, output), output
            return Status(p.returncode, errout), output
        except Exception as ex:
            return Status(-1, ex), None

    def GetNsLeader(self):
        cmd = list(self.ns_base_cmd)
        cmd.append("--cmd=showns")
        status, output = self.RunWithRetuncode(cmd)
        if status.OK():
            result = self.ParseResult(output)
            for record in result:
                if record[2] == "leader":
                    return Status(), record[0]
        return Status(-1, "get ns leader failed"), None


    def ParseResult(self, output):
        result = []
        lines = output.split("\n")
        content_is_started = False
        for line in lines:
            line = line.lstrip()
            if line.startswith("------"):
                content_is_started = True
                continue
            if not content_is_started:
                continue
            record = line.split()
            if len(record) > 0:
                result.append(record)
        return result

    def GetAutofailover(self):
        cmd = list(self.ns_base_cmd)
        cmd.append("--cmd=confget auto_failover")
        status, output = self.RunWithRetuncode(cmd)
        if not status.OK():
            return status, None
        if output.find("true") != -1:
            return Status(), True
        return Status(), False

    def SetAutofailover(self, value):
        cmd = list(self.ns_base_cmd)
        cmd.append("--cmd=confset auto_failover " + value)
        status, output = self.RunWithRetuncode(cmd)
        return status

    def GetAllDatabase(self):
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

    def GetTableInfo(self, database, table_name = ''):
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
    def GetTableInfoHTTP(self, database, table_name = ''):
        """http post ShowTable to ns leader, return one or all table info"""
        ns = self.endpoint_map[self.ns_leader]
        conn = httplib.HTTPConnection(ns)
        param = {"db": database, "name": table_name}
        headers = {"Content-type": "application/json", "Authorization": "foo"}
        conn.request("POST", "/NameServer/ShowTable", json.dumps(param), headers)
        response = conn.getresponse()
        if response.status != 200:
            return Status(response.status, response.reason), None
        result = json.loads(response.read())
        conn.close()
        # check resp
        if result["code"] != 0:
            return Status(result["code"], "get table info failed: {msg}".format(msg=result["msg"]))
        return Status(), result["table_info"]

    def ParseTableInfo(self, table_info):
        result = {}
        if not table_info:
            return Status(-1, "table info is empty"), None
        for record in table_info:
            is_leader = True if record[4] == "leader" else False
            is_alive = True if record[5] == "yes" else False
            partition = Partition(record[0], record[1], record[2], record[3], is_leader, is_alive, record[6])
            result.setdefault(record[2], [])
            result[record[2]].append(partition)
        return Status(), result

    def ParseTableInfoJson(self, table_info):
        """parse one table's partition info from json"""
        result = {}
        parts = table_info["table_partition"]
        for partition in parts:
            # one partition(one leader and others)
            for replica in partition["partition_meta"]:
                is_leader = replica["is_leader"]
                is_alive = True if "is_alive" not in replica else replica["is_alive"]
                # the classname should be replica, but use partition for compatible
                pinfo = Partition(table_info["name"], table_info["tid"], partition["pid"], replica["endpoint"], is_leader, is_alive, replica["offset"])
                result.setdefault(partition["pid"], [])
                result[partition["pid"]].append(pinfo)
        return result

    def GetTablePartition(self, database, table_name):
        status, result = self.GetTableInfo(database, table_name)
        if not status.OK:
            return status, None
        return self.ParseTableInfo(result)

    def GetAllTable(self, database):
        status, result = self.GetTableInfo(database)
        if not status.OK():
            return status, None
        tables = []
        for partition in result:
            if partition[0] not in tables:
                tables.append(partition[0])
        return Status(), tables

    def GetTableStatus(self, endpoint, tid = '', pid = ''):
        cmd = list(self.tablet_base_cmd)
        cmd.append("--endpoint=" + self.endpoint_map[endpoint])
        cmd.append("--cmd=gettablestatus " + tid + " " + pid)
        status, output = self.RunWithRetuncode(cmd)
        if not status.OK():
            log.error("gettablestatus failed on " + str(cmd))
            return status, None
        if "failed" in output:
            log.error("gettablestatus failed on " + str(cmd))
            return Status(-1, output), None
        result = {}
        for record in self.ParseResult(output):
            if len(record) < 4:
                continue
            key = "{}_{}".format(record[0], record[1])
            result[key] = record
        return Status(), result

    def ShowTableStatus(self, pattern = '%'):
        cmd = list(self.sql_base_cmd)
        cmd.append("--cmd=show table status like '{pattern}';".format(pattern = pattern))
        status, output = self.RunWithRetuncode(cmd)
        if not status.OK():
            log.error("show table status failed")
            return status, None
        if "failed" in output:
            log.error("show table status failed")
            return Status(-1, output), None
        output = self.ParseResult(output)
        output_processed = []
        if len(output) >= 1:
            header = output[0]
            output_processed.append(header)
            col_num = len(header)
            for i in range(1, len(output)):
                # warnings col may be empty
                if len(output[i]) == col_num - 1:
                    output_processed.append(output[i] + [""])
                elif len(output[i]) == col_num:
                    output_processed.append(output[i])

        return Status(), output_processed

    def LoadTableHTTP(self, endpoint, name, tid, pid, storage):
        """http post LoadTable to tablet, support all storage mode"""
        conn = httplib.HTTPConnection(endpoint)
        # ttl won't effect, set to 0, and seg cnt is always 8
        # and no matter if leader
        param = {"table_meta": {"name": name, "tid": tid, "pid": pid, "ttl":0, "seg_cnt":8, "storage_mode": storage}}
        headers = {"Content-type": "application/json", "Authorization": "foo"}
        conn.request("POST", "/TabletServer/LoadTable", json.dumps(param), headers)
        response = conn.getresponse()
        if response.status != 200:
            return Status(response.status, response.reason)
        result = response.read()
        conn.close()
        resp = json.loads(result)
        if resp["code"] != 0:
            return Status(resp["code"], resp["msg"])
        # wait for success TODO(hw): refactor
        while True:
            status, result = self.GetTableStatus(endpoint, str(tid), str(pid))
            key = "{}_{}".format(tid, pid)
            if status.OK() and key in result:
                table_stat = result[key][4]
                if table_stat == "kTableNormal":
                    return Status()
                elif table_stat == "kTableLoading" or table_stat == "kTableUndefined":
                    log.info("table is loading... tid {tid} pid {pid}".format(tid = tid, pid = pid))
                else:
                    return Status(-1, "table stat is {table_stat}".format(table_stat = table_stat))
            time.sleep(2)

    def GetLeaderFollowerOffset(self, endpoint, tid, pid):
        cmd = list(self.tablet_base_cmd)
        cmd.append("--endpoint=" + self.endpoint_map[endpoint])
        cmd.append("--cmd=getfollower {} {}".format(tid, pid))
        status, output = self.RunWithRetuncode(cmd)
        if not status.OK():
            return status
        return Status(), self.ParseResult(output)

    def RecoverTablePartition(self, database, name, pid, endpoint, sync = False):
        cmd = list(self.ns_base_cmd)
        cmd.append("--cmd=recovertable {} {} {}".format(name, pid, endpoint))
        cmd.append("--database=" + database)
        status, output = self.RunWithRetuncode(cmd)
        if status.OK() and output.find("recover table ok") != -1:
            if sync and not self.WaitingOP(database, name, pid).OK():
                return Status(-1, "recovertable failed")
            return Status()
        return status

    def UpdateTableAlive(self, database, name, pid, endpoint, is_alive):
        if is_alive not in ["yes", "no"]:
            return Status(-1, "invalid argument {is_alive}".format(is_alive))
        cmd = list(self.ns_base_cmd)
        cmd.append("--cmd=updatetablealive {} {} {} {}".format(name, pid, endpoint, is_alive))
        cmd.append("--database=" + database)
        status, output = self.RunWithRetuncode(cmd)
        if status.OK() and output.find("update ok") != -1:
            return Status()
        return Status(-1, "update table alive failed")

    def ChangeLeader(self, database, name, pid, endpoint = "auto", sync = False):
        cmd = list(self.ns_base_cmd)
        cmd.append("--cmd=changeleader {} {} {}".format(name, pid, endpoint))
        cmd.append("--database=" + database)
        status, output = self.RunWithRetuncode(cmd)
        if status.OK() and sync and not self.WaitingOP(database, name, pid).OK():
            return Status(-1, "changer leader failed")
        return status

    def ShowOpStatus(self, database, name = '', pid = ''):
        cmd = list(self.ns_base_cmd)
        cmd.append("--cmd=showopstatus {} {} ".format(name, pid))
        cmd.append("--database=" + database)
        status, output = self.RunWithRetuncode(cmd)
        if not status.OK():
            return status, None
        return Status(), self.ParseResult(output)

    def CancelOp(self, database, op_id):
        cmd = list(self.ns_base_cmd)
        cmd.append("--cmd=cancelop {}".format(op_id))
        cmd.append("--database=" + database)
        status, output = self.RunWithRetuncode(cmd)
        return status

    def Migrate(self, database, name, pid, src_endpoint, desc_endpoint, sync = False):
        if src_endpoint == desc_endpoint:
            return Status(-1, "src_endpoint and desc_endpoint is same")
        cmd = list(self.ns_base_cmd)
        cmd.append("--cmd=migrate {} {} {} {}".format(src_endpoint, name, pid, desc_endpoint))
        cmd.append("--database=" + database)
        status, output = self.RunWithRetuncode(cmd)
        if status.OK() and output.find("migrate ok") != -1:
            if sync and not self.WaitingOP(database, name, pid).OK():
                return Status(-1, "migrate failed")
            return Status()
        return status

    def ShowTablet(self):
        cmd = list(self.ns_base_cmd)
        cmd.append("--cmd=showtablet")
        status, output = self.RunWithRetuncode(cmd)
        if not status.OK():
            return status, None
        return Status(), self.ParseResult(output)

    def AddReplica(self, database, name, pid, endpoint, sync = False):
        cmd = list(self.ns_base_cmd)
        cmd.append("--cmd=addreplica {} {} {}".format(name, pid, endpoint))
        cmd.append("--database=" + database)
        status, output = self.RunWithRetuncode(cmd)
        if status.OK() and output.find("ok") != -1:
            if sync and not self.WaitingOP(database, name, pid).OK():
                return Status(-1, "addreplica failed")
            return Status()
        return Status(-1, "add replica failed")

    def DelReplica(self, database, name, pid, endpoint, sync = False):
        cmd = list(self.ns_base_cmd)
        cmd.append("--cmd=delreplica {} {} {}".format(name, pid, endpoint))
        cmd.append("--database=" + database)
        status, output = self.RunWithRetuncode(cmd)
        if status.OK() and output.find("ok") != -1:
            if sync and not self.WaitingOP(database, name, pid).OK():
                return Status(-1, "delreplica failed")
            return Status()
        return Status(-1, "del replica failed")

    def WaitingOP(self, database, name, pid):
        while True:
            error_try_times = 3
            while error_try_times > 0:
                status, result = self.ShowOpStatus(database, name, pid)
                error_try_times -= 1
                if status.OK():
                    break
                elif error_try_times == 0:
                    return Status(-1, "fail to execute showopstatus")
            record = result[-1]
            if record[4] == 'kDoing' or record[4] == 'kInited':
                value = " ".join(record)
                log.info("waiting {value}".format(value = value))
                time.sleep(2)
            elif record[4] == 'kFailed':
                return Status(-1, "job {id} execute failed".format(id = record[0]))
            else:
                break
        return Status()
