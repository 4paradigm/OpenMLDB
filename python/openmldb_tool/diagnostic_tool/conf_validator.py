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

import configparser
import logging
from diagnostic_tool.dist_conf import CONFDIR, DistConf
import os

log = logging.getLogger(__name__)


class DistConfValidator:
    def __init__(self, conf: DistConf):
        self.conf = conf

    def check_path(self, path):
        if not path.startswith("/"):
            return False
        return True

    # it's task of collector
    def check_path_exist(self, path):
        if not os.path.exists(path):
            return False
        return True

    def check_endpoint(self, endpoint):
        arr = endpoint.split(":")
        if len(arr) != 2:
            return False
        if not arr[1].isnumeric():
            return False
        return True

    def validate(self) -> bool:
        """
        server required: endpoint(ip:port), optional: path, is_local
        """
        d = self.conf.count_dict()
        if self.conf.is_cluster():
            # no taskmanager or apiserver is ok
            assert d["tablet"] >= 2 and d["nameserver"] >= 1 and d["zookeeper"] >= 1
        else:
            # standalone just two servers
            assert d["tablet"] == 1 and d["nameserver"] == 1

        # if require_dir, all servers must have field `path`
        # zk is skipped,
        for r, v in self.conf.server_info_map.items():
            if r == "zookeeper":
                continue
            for server in v:
                endpoint = server.endpoint
                assert self.check_endpoint(endpoint), endpoint
                assert server.path, server
        return True


class GflagsConfParser:
    def __init__(self, config_path):
        self.conf_map = {}
        with open(config_path, "r") as stream:
            for line in stream:
                item = line.strip()
                # ref gflag flagfile style CommandLineFlagParser::ProcessOptionsFromStringLocked
                if item == "" or item.startswith("#"):
                    continue
                elif item.startswith("-") and len(item) > 1:
                    skip_leading_hyphen = 2 if item[1] == "-" else 1
                    arr = item[skip_leading_hyphen:].split("=")
                    if len(arr) == 1:  # no =
                        key = arr[0]
                        # --noabc=1 is valid, but --noabc is unsupported
                        assert not key.startswith(
                            "no"
                        ), "cant parse --no<bool_flag> now"
                        self.conf_map[arr[0]] = None
                    else:
                        self.conf_map[arr[0]] = arr[1]
                # else: a filename, not supported

    def conf(self):
        return self.conf_map


class JavaPropertiesConfParser:
    """to support more, try PyProperties/pyjavaproperties/jproperties"""

    def __init__(self, config_path):
        with open(config_path, "r") as f:
            config_string = "[dummy_section]\n" + f.read()
            self.config = configparser.ConfigParser()
            self.config.read_string(config_string)

    def conf(self):
        return dict(self.config["dummy_section"])


class ClusterConfValidator:
    def __init__(self, dist_conf: DistConf, confs_root_dir):
        self.dist_conf = dist_conf
        self.confs_root_dir = confs_root_dir

    def validate(self) -> bool:
        flag = True
        # name server conf
        dist_conf_map = self.dist_conf.server_info_map.map
        # zk cluster should == dist conf
        conf_zks = sorted(
            [":".join(zk.endpoint.split(":")[:2]) for zk in dist_conf_map["zookeeper"]]
        )

        def config_check(config_name, actual, expected):
            if actual != expected:
                print(f"check config {config_name} failed: actual {actual}, expected {expected}")
                return False
            return True

        def zks_check(server_conf, name="zk_cluster") -> bool:
            server_zks = sorted(server_conf[name].split(","))
            return config_check(name, server_zks, conf_zks)

        # zk path should be the same in all server, we use the first nameserver path
        root_path = ""

        def zk_root_path_check(server_conf, name="zk_root_path") -> bool:
            nonlocal root_path
            if not root_path:
                root_path = server_conf[name]
                logging.debug(f"take the first root_path conf in server, {root_path}")
                return True
            return config_check(name, server_conf[name], root_path)

        for ns in dist_conf_map["nameserver"]:
            # read conf
            ns_conf = GflagsConfParser(
                f"{self.confs_root_dir}/{ns.server_dirname()}/{CONFDIR}/{ns.conf_filename()}"
            ).conf()
            logging.debug(f"{ns} flags: {ns_conf}")
            if not zks_check(ns_conf):
                flag = False
            if not zk_root_path_check(ns_conf):
                flag = False
            # ref DEFINE_uint32(system_table_replica_num, 1, ...
            rep_num = ns_conf.get("system_table_replica_num", "1")
            if int(rep_num) > len(dist_conf_map["tablet"]):
                log.warning(
                    f"system_table_replica_num {rep_num} in {ns} is greater than tablets number"
                )
                flag = False
        for tablet in dist_conf_map["tablet"]:
            # read conf
            tablet_conf = GflagsConfParser(
                f"{self.confs_root_dir}/{tablet.server_dirname()}/{CONFDIR}/{tablet.conf_filename()}"
            ).conf()
            logging.debug(f"{tablet} flags: {tablet_conf}")
            if not zks_check(tablet_conf):
                flag = False
            if not zk_root_path_check(tablet_conf):
                flag = False
        for tm in dist_conf_map["taskmanager"]:
            tm_conf = JavaPropertiesConfParser(
                f"{self.confs_root_dir}/{tm.server_dirname()}/{CONFDIR}/{tm.conf_filename()}"
            ).conf()
            logging.debug(f"{tm} flags: {tm_conf}")
            if not zks_check(tm_conf, name="zookeeper.cluster"):
                flag = False
            if not zk_root_path_check(tm_conf, name="zookeeper.root_path"):
                flag = False
            # TODO: ref TaskManagerConfValidator
        return flag


class TaskManagerConfValidator:
    def __init__(self, conf_dict):
        self.conf_dict = conf_dict
        self.default_conf_dict = {
            "server.host": "0.0.0.0",
            "server.port": "9902",
            "zookeeper.cluster": "",
            "zookeeper.root_path": "",
            "spark.master": "local",
            "spark.yarn.jars": "",
            "spark.home": "",
            "prefetch.jobid.num": "1",
            "job.log.path": "../log/",
            "external.function.dir": "./udf/",
            "job.tracker.interval": "30",
            "spark.default.conf": "",
            "spark.eventLog.dir": "",
            "spark.yarn.maxAppAttempts": "1",
            "offline.data.prefix": "file:///tmp/openmldb_offline_storage/",
        }
        self.fill_default_conf()
        self.flag = True

    def fill_default_conf(self):
        for key in self.default_conf_dict:
            if key not in self.conf_dict:
                self.conf_dict[key] = self.default_conf_dict[key]

    def check_noempty(self):
        no_empty_keys = [
            "zookeeper.cluster",
            "zookeeper.root_path",
            "job.log.path",
            "external.function.dir",
            "offline.data.prefix",
        ]
        for item in no_empty_keys:
            if self.conf_dict[item] == "":
                log.warning(f"{item} should not be empty")
                self.flag = False

    def check_port(self):
        if not self.conf_dict["server.port"].isnumeric():
            log.warning("port should be number")
            self.flag = False
            return
        port = int(self.conf_dict["server.port"])
        if port < 1 or port > 65535:
            log.warning("port should be in range of 1 through 65535")
            self.flag = False

    def check_spark(self):
        spark_master = self.conf_dict["spark.master"].lower()
        is_local = spark_master.startswith("local")
        if not is_local and spark_master not in ["yarn", "yarn-cluster", "yarn-client"]:
            log.warning(
                "spark.master should be local, yarn, yarn-cluster or yarn-client"
            )
            self.flag = False
        if spark_master.startswith("yarn"):
            if self.conf_dict["spark.yarn.jars"].startswith("file://"):
                log.warning(
                    "spark.yarn.jars should not use local filesystem for yarn mode"
                )
                self.flag = False
            if self.conf_dict["spark.eventLog.dir"].startswith("file://"):
                log.warning(
                    "spark.eventLog.dir should not use local filesystem for yarn mode"
                )
                self.flag = False
            if self.conf_dict["offline.data.prefix"].startswith("file://"):
                log.warning(
                    "offline.data.prefix should not use local filesystem for yarn mode"
                )
                self.flag = False

        spark_default_conf = self.conf_dict["spark.default.conf"]
        if spark_default_conf != "":
            spark_jars = spark_default_conf.split(";")
            for spark_jar in spark_jars:
                if spark_jar != "":
                    kv = spark_jar.split("=")
                    if len(kv) < 2:
                        log.warning(f"spark.default.conf error format of {spark_jar}")
                        self.flag = False
                    elif not kv[0].startswith("spark"):
                        log.warning(
                            f"spark.default.conf config key should start with 'spark' but get {kv[0]}"
                        )
                        self.flag = False

        if int(self.conf_dict["spark.yarn.maxAppAttempts"]) < 1:
            log.warning("spark.yarn.maxAppAttempts should be larger or equal to 1")
            self.flag = False

    def check_job(self):
        if int(self.conf_dict["prefetch.jobid.num"]) < 1:
            log.warning("prefetch.jobid.num should be larger or equal to 1")
            self.flag = False
        jobs_path = self.conf_dict["job.log.path"]
        if jobs_path.startswith("hdfs") or jobs_path.startswith("s3"):
            log.warning("job.log.path only support local filesystem")
            self.flag = False
        if int(self.conf_dict["job.tracker.interval"]) <= 0:
            log.warning("job.tracker.interval interval should be larger than 0")
            self.flag = False

    def validate(self):
        self.check_noempty()
        self.check_port()
        self.check_spark()
        self.check_job()
        return self.flag
