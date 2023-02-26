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

import logging
import os

from diagnostic_tool.dist_conf import CXX_SERVER_ROLES, JAVA_SERVER_ROLES, LOGDIR, DistConf, ServerInfo

log = logging.getLogger(__name__)


class LogAnalyzer:
    def __init__(self, dist_conf: DistConf, log_root_dir):
        self.dist_conf = dist_conf
        self.log_root_dir = log_root_dir
        self.taskmanager_ignore_errors = [
            "Unable to load native-hadoop library for your platform",
        ]

    def log_files(self, server_info: ServerInfo):
        logs_path = f"{self.log_root_dir}/{server_info.server_dirname()}/{LOGDIR}"
        # files in path
        return [f"{logs_path}{f}" for f in os.listdir(logs_path) if os.path.isfile(f"{logs_path}{f}")]

    def run(self):
        """anaylsis all servers' log file"""
        # cxx server glog
        def grep_log(server_info: ServerInfo) -> bool:
            files = self.log_files(server_info)
            log.debug(files)
            # just print warning logs TODO: what if warning log files are discard when pulling log?
            for f in files:
                if "warning.log" in f:
                    print(f"find warning log {f}:")
                    print(open(f, "r").read(), "\n")
        self.dist_conf.server_info_map.for_each(grep_log, CXX_SERVER_ROLES)
        # exception or custom error
        def grep_log(server_info: ServerInfo) -> bool:
            files = self.log_files(server_info)
            log.debug(files)
            for f in files:
                with open(f, "r") as s:
                    for i, line in enumerate (s):
                        if "exception" in line.lower():
                            print(f"find exception in line {i}, check it in file {f}")
                        for error in self.taskmanager_ignore_errors:
                            if error in line:
                                print(f"find error '{error}' in line {i}, check it in file {f}")
        self.dist_conf.server_info_map.for_each(grep_log, JAVA_SERVER_ROLES)
        # WARN in java
        def grep_log(server_info: ServerInfo) -> bool:
            files = self.log_files(server_info)
            log.debug(files)
            for f in files:
                with open(f, "r") as s:
                    for i, line in enumerate (s):
                        if " WARN " in line:
                            print(f"{i} {line}")
        print(f"warning log in {JAVA_SERVER_ROLES}")
        self.dist_conf.server_info_map.for_each(grep_log, JAVA_SERVER_ROLES)

    def check_warning(self, name, line) -> bool:
        if self.role == "taskmanager":
            if name.startswith("taskmanager"):
                if len(line) < 28:
                    return False
                if not line[:2].isnumeric():
                    return False
                log_level = line[24:28]
                if log_level in ["INFO", "WARN", "ERROR"] and log_level != "INFO":
                    return True
            else:
                if len(line) < 22:
                    return False
                log_level = line[18:22]
                if log_level in ["INFO", "WARN", "ERROR"] and log_level != "INFO":
                    for filter_msg in self.taskmanager_ignore_errors:
                        if line.find(filter_msg) != -1:
                            return False
                    return True
        else:
            if (line.startswith("W") or line.startswith("E")) and line[1].isnumeric():
                return True
        return False

    def analysis_log(self):
        flag = True
        for name, full_path in self.file_list:
            msg = "----------------------------------------\n"
            print_errlog = False
            with open(full_path, "r", encoding="UTF-8") as f:
                line = f.readline()
                while line:
                    line = line.strip()
                    if line != "":
                        if self.check_warning(name, line):
                            flag = False
                            print_errlog = True
                            msg += line + "\n"
                    line = f.readline()
            if print_errlog:
                log.warn(f"{self.role} {self.endpoint} have error logs in {name}:")
                log.info(f"error msg: \n{msg}")
        return flag
