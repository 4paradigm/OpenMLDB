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
        self.db_name = '__test_db_xxx_aaa_diagnostic_tool__'
        self.table_name = '__test_table_xxx_aaa_diagnostic_tool__'
        connect_args = {} # {'database': self.db_name} the db is not guaranteed to exist
        if not print_sdk_log:
            connect_args['zkLogLevel'] = 0
            connect_args['glogLevel'] = 2

        if conf_dict['mode'] == 'cluster':
            connect_args['zk'] = conf_dict['zookeeper']['zk_cluster']
            connect_args['zkPath'] = conf_dict['zookeeper']['zk_root_path']
        else:
            connect_args['host'], connect_args['port'] = conf_dict['nameserver'][0]['endpoint'].split(
                ":")
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
                if status != 'online':
                    log.warn(f'{component} endpoint {endpoint} is offline')

    def check_startup(self, component_map):
        for component in ['nameserver', 'tablet', 'taskmanager']:
            if self.conf_dict['mode'] != 'cluster':
                if component == 'taskmanager':
                    continue
                if len(self.conf_dict[component]) > 1:
                    log.warn(f'{component} number is greater than 1')

            for item in self.conf_dict[component]:
                endpoint = item['endpoint']
                has_found = False
                for cur_endpoint, _ in component_map[component]:
                    if endpoint == cur_endpoint:
                        has_found = True
                        break
                if not has_found:
                    log.warn(
                        f'{component} endpoint {endpoint} has not startup')

    def check_component(self):
        result = self.cursor.execute('SHOW COMPONENTS;').fetchall()
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
            result = self.cursor.execute(
                'SHOW JOB {};'.format(job_id)).fetchall()
            return result[0][2]
        except Exception as e:
            log.warn(e)
            return None

    def check_run_job(self) -> bool:
        if 'taskmanager' not in self.conf_dict:
            log.info('no taskmanager installed. skip job test')
            return True
        self.cursor.execute('SET @@execute_mode=\'offline\';')
        result = self.cursor.execute(
            'SELECT * FROM {};'.format(self.table_name)).fetchall()
        if len(result) < 1:
            log.warn('run job failed. no job info returned')
            return False
        job_id = result[0][0].split('\n')[3].strip().split(' ')[0]
        time.sleep(2)
        while True:
            status = self.get_job_status(job_id)
            if status is None:
                return False
            elif status == 'FINISHED':
                return True
            elif status == 'FAILED':
                log.warn('job execute failed')
                return False
            time.sleep(2)
        return True

    def run_test_sql(self) -> bool:
        self.check_component()
        self.cursor.execute(
            'CREATE DATABASE IF NOT EXISTS {};'.format(self.db_name))
        result = self.cursor.execute('SHOW DATABASES;').fetchall()
        if not self.is_exist(result, self.db_name):
            log.warn('create database failed')
            return False
        self.cursor.execute('USE {};'.format(self.db_name)).fetchall()
        self.cursor.execute(
            'CREATE TABLE IF NOT EXISTS {} (col1 string, col2 string);'.format(self.table_name))
        result = self.cursor.execute('SHOW TABLES;').fetchall()
        if not self.is_exist(result, self.table_name):
            log.warn('create table failed')
            return False

        flag = True
        if self.conf_dict['mode'] == 'cluster':
            if not self.check_run_job():
                flag = False

        self.cursor.execute('SET @@execute_mode=\'online\';')
        self.cursor.execute(
            'INSERT INTO {} VALUES (\'aa\', \'bb\');'.format(self.table_name))
        result = self.cursor.execute(
            'SELECT * FROM {};'.format(self.table_name)).fetchall()
        if len(result) != 1:
            log.warn('check select data failed')
            flag = False

        self.cursor.execute('DROP TABLE {};'.format(self.table_name))
        result = self.cursor.execute('SHOW TABLES;').fetchall()
        if self.is_exist(result, self.table_name):
            log.warn(f'drop table {self.table_name} failed')
            flag = False
        self.cursor.execute('DROP DATABASE {};'.format(self.db_name))
        result = self.cursor.execute('SHOW DATABASES;').fetchall()
        if self.is_exist(result, self.db_name):
            log.warn(f'drop database {self.db_name} failed')
            flag = False
        return flag
