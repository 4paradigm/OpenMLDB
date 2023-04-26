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

from absl import flags
from absl import logging
from prettytable import PrettyTable
import re
import requests
import time

from .connector import Connector
from .dist_conf import DistConf, COMPONENT_ROLES, ServerInfo

# rename to checker?


class StatusChecker:
    def __init__(self, conn: Connector):
        self.conn = conn

    def check_components(self):
        """ensure all components in cluster is online"""
        components_map = self._get_components(show=True)
        return self._check_status(components_map)

    def check_connection(self):
        """check all compontents connections"""
        component_map = self._get_components(show=False)
        t = PrettyTable()
        t.title = "Connections"
        t.field_names = ["Endpoint", "Version", "Cost_time", "Extra"]
        err = ""
        taskmanager = component_map.pop("taskmanager")  # extract taskmanager
        other_components = [component for role in component_map.values() for component in role]  # extract other components
        for (endpoint, _) in other_components:
            version, response_time, ex, e = self._get_information(endpoint)
            t.add_row([endpoint, version, response_time, ex])
            err += e
        for (endpoint, _) in taskmanager:
            version, response_time, ex, e = self._get_information_taskmanager(endpoint)
            t.add_row([endpoint, version, response_time, ex])
            err += e
        print(t)
        if err:
            print(err)

    def _get_information(self, endpoint):
        """get informations from components except taskmanager"""
        try:
            response = requests.get(f"http://{endpoint}/status", timeout=1)  # the connection timeout is 1 second
            response.raise_for_status()
        except Exception as e:
            err = endpoint + "\n" + str(e) + "\n"
            return "-", "timeout", "Error: See below", err
        text = response.text
        regex = re.compile("version: (.*?)\\n")
        match = re.search(regex, text)
        version = match.group(1)
        ex = ""
        response_time = str(response.elapsed.microseconds / 1000) + "ms"
        return version, response_time, ex, ""

    def _get_information_taskmanager(self, endpoint):
        """get informations from taskmanager"""
        try:
            response = requests.post(f"http://{endpoint}/openmldb.taskmanager.TaskManagerServer/GetVersion", json={})
            response.raise_for_status()
        except Exception as e:
            err = endpoint + "\n" + str(e) + "\n"
            return "-", "timeout", "Error: See below", err
        js = response.json()
        version = js["taskmanagerVersion"]
        ex = "batchVersion: " + js["batchVersion"][:-1]
        response = requests.get(f"http://{endpoint}", timeout=1)
        response_time = str(response.elapsed.microseconds / 1000) + "ms"
        return version, response_time, ex, ""

    def offline_support(self):
        """True if have taskmanager, else False"""
        components_map = self._get_components(show=False)
        return "taskmanager" in components_map

    def _get_components(self, show=False):
        component_list = self.conn.execfetch("SHOW COMPONENTS", show=show)
        component_map = {}
        for (endpoint, component, _, status, _) in component_list:
            component_map.setdefault(component, [])
            component_map[component].append((endpoint, status))
        return component_map

    def _check_status(self, component_map):
        for component, value_list in component_map.items():
            for endpoint, status in value_list:
                if status != "online":
                    logging.warning(f"{component} endpoint {endpoint} is offline")
                    return False
        return True

    def check_startup(self, dist_conf: DistConf) -> bool:
        """
        Check if all components in conf file are exist in cluster
        Components don't include apiserver
        """
        components_in_cluster = self._get_components(show=False)

        def is_exist(server_info: ServerInfo) -> bool:
            finds = [
                server_in_cluster
                for server_in_cluster in components_in_cluster[server_info.role]
                if server_in_cluster[0] == server_info.endpoint
            ]
            if len(finds) == 0:
                print(f"server {server_info.endpoint} is not exist in cluster")
            elif len(finds) > 1:
                print(
                    f"server {server_info.endpoint} is exist in cluster more than once"
                )
            else:
                if finds[0][1] != "online":
                    print(
                        f"server {server_info.endpoint} is exist in cluster, but status is {finds[0][1]}"
                    )
                else:
                    return True
            return False

        return dist_conf.server_info_map.for_each(is_exist, roles=COMPONENT_ROLES)


class SQLTester:
    def __init__(self, conn: Connector):
        self.db_name = "__test_db_xxx_aaa_diagnostic_tool__"
        self.table_name = "__test_table_xxx_aaa_diagnostic_tool__"
        self.conn = conn

    def setup(self):
        """create db and table"""
        self.conn.execute(f"CREATE DATABASE IF NOT EXISTS {self.db_name}")
        # ensure empty db, test won't create sp/deployment, so it's ok to delete directly
        self.conn.execute(f"USE {self.db_name}")
        tables = self.conn.execfetch(f"SHOW TABLES")
        for t in tables:
            self.conn.execute(f"DROP TABLE {t[0]}")
        assert not self.conn.execfetch(f"SHOW TABLES")
        self.conn.execute(f"CREATE TABLE {self.table_name}(col1 string, col2 string)")
        tables = self.conn.execfetch(f"SHOW TABLES")
        assert (
            len(tables) == 1 and tables[0][0] == self.table_name
        ), f"cur tables: {tables}, > 1 table or not {self.table_name}"

    def online(self):
        """test create, insert, select in online mode"""
        self.conn.execute(f"USE {self.db_name}")
        self.conn.execute("SET @@execute_mode='online';")
        self.conn.execute(f"INSERT INTO {self.table_name} VALUES ('aa', 'bb');")
        result = self.conn.execfetch(f"SELECT * FROM {self.table_name};")
        assert len(result) == 1, f"should be 1 row, but {result}"

    def offline(self):
        """test select in offline mode. We don't load data, so it will be empty"""
        # taskmanager must exist?
        self.conn.execute(f"USE {self.db_name}")
        self.conn.execute("SET @@execute_mode='offline';")
        self.conn.execute("SET @@sync_job=true;")
        result = self.conn.execfetch(f"SELECT * FROM {self.table_name};")
        # empty select still has a result table
        print(result)
        # after 0.7.2, empty select will return empty result, no header
        assert "" == result[0][0]  # more flexible?

    def teardown(self):
        """cleanup test db"""
        self.conn.execute(f"USE {self.db_name}")
        tables = self.conn.execfetch(f"SHOW TABLES")
        for t in tables:
            self.conn.execute(f"DROP TABLE {t[0]}")
        tables = self.conn.execfetch(f"SHOW TABLES")
        assert not tables
        self.conn.execute(f"DROP DATABASE {self.db_name}")  # no need to check
