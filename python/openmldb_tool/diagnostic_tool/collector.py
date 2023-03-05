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
import re

from diagnostic_tool.dist_conf import (
    LOGDIR,
    DistConf,
    CXX_SERVER_ROLES,
    ServerInfo,
    JAVA_SERVER_ROLES,
)
from absl import flags

log = logging.getLogger(__name__)


def parse_config_from_properties(props_str, config_name) -> str:
    f"""
    the config line must start from {config_name}, no comment
    :param props_str:
    :param config_name:
    :return:
    """
    config_name = re.escape(config_name)
    m = re.search(
        rf"^{config_name}.*",
        props_str,
    )
    if not m:
        return ""
    conf_line = m.group(0)  # the whole line
    # TODO(hw): relative path or abs path is ok?
    return conf_line.split("=")[1]


class Collector:
    """
    For each server, if is_local, run cmd in localhost, else use ssh
    We should get job logs by `SHOW JOBLOG`
    """

    def __init__(self, dist_conf: DistConf):
        self.dist_conf = dist_conf

    def ping_all(self) -> bool:
        """
        test ssh
        return False if command got some errors
        throw SSHException if the server fails to execute the command
        :return: bool
        """
        assert not flags.FLAGS.local, "local servers don't need to test ping"

        def ping(server_info: ServerInfo) -> bool:
            res = server_info.cmd_on_host("whoami && pwd")
            log.debug(res)
            if not res:
                log.warning(f"failed to ping {server_info}")
                return False
            return True

        return self.dist_conf.server_info_map.for_each(ping)

    def collect_version(self):
        """
        get the version of components before starts
        :return: {role: [(endpoint, version), ...]}
        """
        version_map = {}

        def extract_version(raw_version):
            return raw_version.split(" ")[2].split("-")[0]

        def extract_java_version(raw_version):
            arr = raw_version.split("-")
            if len(arr) < 2:
                return ""
            return arr[0]

        def run_version(server_info: ServerInfo) -> bool:
            version, _ = server_info.cmd_on_host(
                f"{server_info.path}/bin/openmldb --version"
            )
            if not version:
                log.warning("failed at get version from %s", server_info)
            else:
                version_map.setdefault(server_info.role, [])
                version_map[server_info.role].append(
                    (server_info.endpoint, extract_version(version))
                )
            return True

        self.dist_conf.server_info_map.for_each(run_version, CXX_SERVER_ROLES)

        def jar_version(server_info: ServerInfo) -> bool:
            def get_spark_home(server_info: ServerInfo):
                """
                https://openmldb.ai/docs/zh/main/deploy/install_deploy.html#taskmanager use env, but won't store it
                :param remote_config_file:
                :return: abs path
                """
                tm_conf_path = server_info.conf_path_pair("")[0]
                config_name = "spark.home="
                log.debug("get %s from %s", config_name, tm_conf_path)
                grep_str, _ = server_info.cmd_on_host(f"grep {config_name} {tm_conf_path}")

                if not grep_str:
                    # TODO(hw):no config in file, get env SPARK_HOME?
                    #  what if ssh user is different with server user or it's a temp env?
                    # or force to set spark home in config?
                    # _, stdout, _ = self.ssh_client.exec_command(f'env | grep SPARK_HOME')
                    # env = stdout.read()
                    # if not env:
                    #     raise RuntimeError('no env SPARK_HOME')
                    return ""

                # may have spark home in config(discard if it's in comment)
                return parse_config_from_properties(grep_str, config_name)

            def get_batch_version(server_info: ServerInfo):
                # TODO(hw): check if multi batch jars
                spark_home = get_spark_home(server_info)
                log.debug("spark_home %s", spark_home)
                if not spark_home:
                    spark_home = server_info.path + '/spark'
                    log.debug(f"try local spark in server deploy path: {spark_home}")
                batch_jar_path = f"{spark_home}/jars/openmldb-batch-*"
                return server_info.cmd_on_host(
                    f"java -cp {batch_jar_path} com._4paradigm.openmldb.batch.utils.VersionCli"
                )[0].strip()

            bv = get_batch_version(server_info)
            if bv:
                version = extract_java_version(bv)
                if version != "":
                    version_map.setdefault("openmldb-batch", [])
                    version_map["openmldb-batch"].append((server_info.host, version))
                else:
                    log.warning(f"{bv}")
            else:
                log.warning("failed at get batch version from %s", server_info)

            def get_taskmanager_version(server_info: ServerInfo):
                tm_root_path = server_info.taskmanager_path()
                # TODO(hw): check if multi taskmanager jars
                return server_info.cmd_on_host(
                    f"java -cp {tm_root_path}/lib/openmldb-taskmanager-* com._4paradigm.openmldb.taskmanager.utils.VersionCli",
                )[0].strip()

            tv = get_taskmanager_version(server_info)
            if tv:
                version = extract_java_version(tv)
                if version != "":
                    version_map.setdefault("taskmanager", [])
                    version_map["taskmanager"].append((server_info.host, version))
                else:
                    log.warning(f"{tv}")
            else:
                log.warning("failed at get taskmanager version from %s", server_info)
            return True

        self.dist_conf.server_info_map.for_each(jar_version, JAVA_SERVER_ROLES)
        return version_map

    def pull_config_files(self, dest) -> bool:
        def pull_one(server_info: ServerInfo) -> bool:
            # if taskmanager, pull taskmanager.properties, no log4j
            src_file, dest_path = server_info.conf_path_pair(dest)
            log.debug(f"pull config file from {server_info}: {src_file}->{dest_path}")
            return server_info.smart_cp(src_file, dest_path)

        return self.dist_conf.server_info_map.for_each(pull_one)

    def pull_log_files(self, dest) -> bool:
        def pull_log(server_info: ServerInfo) -> bool:
            """
            After 0.7.0, onebox cluster started by sbin won't use the same root dir
            No need to check tablet2.flags
            """
            # get log path
            def get_config_value(server_info: ServerInfo, conf_path, config_name, default_v):
                log.debug("get %s from %s", config_name, conf_path)
                grep_str, _ = server_info.cmd_on_host(f"grep {config_name} {conf_path}")
                if not grep_str:
                    logging.warning("no config file")
                    return default_v
                tmp = parse_config_from_properties(grep_str, config_name)
                return tmp if tmp else default_v

            if server_info.is_taskmanager():
                log4j_conf_path = server_info.remote_log4j_path()
                log_file_name = get_config_value(
                    server_info, log4j_conf_path, "log4j.appender.file.file=", ""
                ) # ref log4j.properties `log4j.appender.file.file=./logs/taskmanager.log`
                if not log_file_name:
                    logging.warning("taskmanager log4j.properties should have conf `log4j.appender.file.file`")
                    return False
                log_path, file_prefix = os.path.split(log_file_name)
                src, dest_path = server_info.remote_local_pairs(log_path, dest, LOGDIR)
                log.debug(f"pull logs from {server_info} by dir: {src}->{dest_path}")
                # old log files have suffix `log4j.appender.file.DatePattern`
                # e.g. taskmanager.log, taskmanager.log.2023-01-19, ...
                # TODO custom filter
                return server_info.smart_cp(src, dest_path, src_is_dir=True, filter=f"{file_prefix}.*")
            else:
                conf_path = server_info.conf_path_pair("")[0]
                log_path = get_config_value(server_info, conf_path, "openmldb_log_dir=", "./logs") # a dir
                # glog file pattern {role}.info.log*, current log file has suffix too
                # zk sdk log is in {role}.log
                src, dest_path = server_info.remote_local_pairs(log_path, dest, LOGDIR)
                log.debug(f"pull logs from {server_info} by dir: {src}->{dest_path}")
                # skip soft link e.g. tablet.INFO(uppercase)
                # TODO custom filter
                return server_info.smart_cp(src, dest_path, src_is_dir=True, filter=server_info.role + "\\.[a-z]{1}.*")

        return self.dist_conf.server_info_map.for_each(pull_log)
