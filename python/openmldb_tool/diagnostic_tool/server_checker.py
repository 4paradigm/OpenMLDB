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
import time
import openmldb.dbapi

log = logging.getLogger(__name__)


class ServerChecker:
    def __init__(self, conf_dict, print_sdk_log):
        self.conf_dict = conf_dict
        self.db_name = "__test_db_xxx_aaa_diagnostic_tool__"
        self.table_name = "__test_table_xxx_aaa_diagnostic_tool__"
        connect_args = (
            {}
        )  # {'database': self.db_name} the db is not guaranteed to exist
        if not print_sdk_log:
            connect_args["zkLogLevel"] = 0
            connect_args["glogLevel"] = 2

        if conf_dict["mode"] == "cluster":
            connect_args["zk"] = conf_dict["zookeeper"]["zk_cluster"]
            connect_args["zkPath"] = conf_dict["zookeeper"]["zk_root_path"]
        else:
            connect_args["host"], connect_args["port"] = conf_dict["nameserver"][0][
                "endpoint"
            ].split(":")
        self.db = openmldb.dbapi.connect(**connect_args)
        self.cursor = self.db.cursor()

    def parse_component(self, component_list):
        component_map = {}
        for (endpoint, component, _, status, role) in component_list:
            component_map.setdefault(component, [])
            component_map[component].append((endpoint, status))
        return component_map

    def check_status(self, component_map):
        for component, value_list in component_map.items():
            for endpoint, status in value_list:
                if status != "online":
                    log.warning(f"{component} endpoint {endpoint} is offline")

    def check_startup(self, component_map):
        for component in ["nameserver", "tablet", "taskmanager"]:
            if self.conf_dict["mode"] != "cluster":
                if component == "taskmanager":
                    continue
                if len(self.conf_dict[component]) > 1:
                    log.warning(f"{component} number is greater than 1")

            for item in self.conf_dict[component]:
                endpoint = item["endpoint"]
                has_found = False
                for cur_endpoint, _ in component_map[component]:
                    if endpoint == cur_endpoint:
                        has_found = True
                        break
                if not has_found:
                    log.warning(f"{component} endpoint {endpoint} has not startup")

    def check_component(self):
        result = self.cursor.execute("SHOW COMPONENTS;").fetchall()
        component_map = self.parse_component(result)
        self.check_status(component_map)
        self.check_startup(component_map)

    def is_exist(self, data, name):
        for item in data:
            if item[0] == name:
                return True
        return False

    def get_job_status(self, job_id):
        try:
            result = self.cursor.execute("SHOW JOB {};".format(job_id)).fetchall()
            return result[0][2]
        except Exception as e:
            log.warning(e)
            return None

    def check_offline_select(self) -> bool:
        if "taskmanager" not in self.conf_dict:
            log.info("no taskmanager installed. skip job test")
            return True
        self.cursor.execute("SET @@execute_mode='offline';")
        # make sure it's an async job, to get the job id, not the select result
        self.cursor.execute("SET @@sync_job=false;")
        result = self.cursor.execute(
            "SELECT * FROM {};".format(self.table_name)
        ).fetchall()
        if len(result) != 1:
            log.warning(f"result is invalid, expect a job info: {result}")
            return False
        # see system table JOB_INFO schema, job id idx is 0
        job_id, try_time = result[0][0], 0
        # it's an empty table in offline, shouldn't wait too long for final states
        while try_time < 60:
            time.sleep(2)
            status = self.get_job_status(job_id)
            if status is None:
                return False
            elif status == "FINISHED":
                return True
            elif status == "FAILED":
                log.warning(f"job execute failed, check job {job_id}")
                return False
            try_time += 1
        return False

    def run_test_sql(self) -> bool:
        self.check_component()
        self.cursor.execute("CREATE DATABASE IF NOT EXISTS {};".format(self.db_name))
        result = self.cursor.execute("SHOW DATABASES;").fetchall()
        if not self.is_exist(result, self.db_name):
            log.warning("create database failed")
            return False
        self.cursor.execute("USE {};".format(self.db_name)).fetchall()
        # If table exists, recreate it, to avoid online check failed.
        # If we added deployment test later, delete deployments too
        if self.table_name in self.cursor.get_tables(self.db_name):
            log.info("table exists, recreate it")
            self.cursor.execute(f"drop table {self.table_name}")
        self.cursor.execute(
            f"CREATE TABLE {self.table_name} (col1 string, col2 string);"
        )
        result = self.cursor.execute("SHOW TABLES;").fetchall()
        if not self.is_exist(result, self.table_name):
            log.warning("create table failed")
            return False

        flag = True
        # offline check
        if self.conf_dict["mode"] == "cluster" and not self.check_offline_select():
            flag = False

        # online check
        self.cursor.execute("SET @@execute_mode='online';")
        self.cursor.execute(
            "INSERT INTO {} VALUES ('aa', 'bb');".format(self.table_name)
        )
        result = self.cursor.execute(
            "SELECT * FROM {};".format(self.table_name)
        ).fetchall()
        if len(result) != 1:
            log.warning("check select data failed")
            flag = False

        self.cursor.execute("DROP TABLE {};".format(self.table_name))
        result = self.cursor.execute("SHOW TABLES;").fetchall()
        if self.is_exist(result, self.table_name):
            log.warning(f"drop table {self.table_name} failed")
            flag = False
        # precheck to avoid the exception of dropping db with tables
        if self.cursor.get_tables(self.db_name):
            log.warning(f"{self.db_name} has tables, skip dropping db")
        else:
            self.cursor.execute("DROP DATABASE {};".format(self.db_name))
        result = self.cursor.execute("SHOW DATABASES;").fetchall()
        if self.is_exist(result, self.db_name):
            log.warning(f"drop database {self.db_name} failed")
            flag = False
        return flag
