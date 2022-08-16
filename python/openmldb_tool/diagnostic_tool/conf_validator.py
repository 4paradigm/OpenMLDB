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

log = logging.getLogger(__name__)

class YamlConfValidator:
    def __init__(self, conf_dict):
        self.conf_dict = conf_dict
        self.standalone_role = ['nameserver', 'tablet']
        self.cluster_role = ['nameserver', 'tablet', 'taskmanager', 'zookeeper']

    def check_exist(self, item_list : list, desc_dict : dict) -> bool:
        flag = True
        for item in item_list:
            if item == 'taskmanager':
                continue
            if item not in desc_dict:
                log.warning(f'no {item} in yaml conf')
                flag = False
        return flag

    def check_path(self, path):
        if not path.startswith('/'):
            return False
        return True

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
        if 'mode' not in self.conf_dict:
            log.warning('no mode in yaml conf')
            return False
        if self.conf_dict['mode'] == 'standalone':
            if not self.check_exist(self.standalone_role, self.conf_dict):
                return False
            for role in self.standalone_role:
                if len(self.conf_dict[role]) != 1:
                    log.warning(f'number of {role} should be 1')
                    return False
                for component in self.conf_dict[role]:
                    if not self.check_exist(['endpoint', 'path'], component):
                        return False
                    if not self.check_endpoint(component['endpoint']):
                        log.warning('invalid endpoint ' + component['endpoint'])
                        return False
                    if not self.check_path(component['path']):
                        log.warning('{} should be absolute path'.format(component['path']))
                        return False
                    if not self.check_path_exist(component['path']):
                        log.warning('{} path is not exist'.format(component['path']))
                        return False
        elif self.conf_dict['mode'] == 'cluster':
            if not self.check_exist(self.cluster_role, self.conf_dict):
                return False
        else:
            log.warning('invalid mode %s in yaml conf. mode should be standalone/cluster', self.conf_dict['mode'])
            return False
        return True

class StandaloneConfValidator:
    def __init__(self, ns_conf_dict, tablet_conf_dict):
        self.ns_conf_dict = ns_conf_dict
        self.tablet_conf_dict = tablet_conf_dict

    def validate(self) -> bool:
        flag = True
        if 'tablet' not in self.ns_conf_dict:
            log.warning('no tablet conf in ns conf')
            flag = False
        elif self.ns_conf_dict['tablet'] != self.tablet_conf_dict['endpoint']:
            log.warning('tablet {} in ns conf and endpoint {} in tablet conf do not match'.format(
                self.ns_conf_dict['tablet'], self.tablet_conf_dict['endpoint']))
            flag = False
        if 'system_table_replica_num' in self.ns_conf_dict and int(self.ns_conf_dict['system_table_replica_num']) != 1:
            log.warning('system_table_replica_num in ns conf should be 1')
            flag = False
        return flag

class ClusterConfValidator:
    def __init__(self, yaml_conf_dict, detail_conf_map):
        self.yaml_conf_dict = yaml_conf_dict
        self.detail_conf_map = detail_conf_map

    def check_zk_conf(self, role, conf_dict) -> bool:
        flag = True
        if conf_dict['zk_cluster'] != self.yaml_conf_dict['zookeeper']['zk_cluster']:
            log.warning('zk_cluster of {} {} and yam conf do not match'.format(role, conf_dict['endpoint']))
            flag = False
        if conf_dict['zk_root_path'] != self.yaml_conf_dict['zookeeper']['zk_root_path']:
            log.warning('zk_root_path of {} {} and yam conf do not match'.format(role, conf_dict['endpoint']))
            flag = False
        return flag

    def check_task_manager_zk_conf(self, conf_dict) -> bool:
        flag = True
        if conf_dict['zookeeper.cluster'] != self.yaml_conf_dict['zookeeper']['zk_cluster']:
            if conf_dict['zookeeper.cluster'].split(':')[0] != '0.0.0.0':
                log.warning('zk_cluster of taskmanager {} and yam conf do not match'.format(conf_dict['server.host']))
                flag = False
        if conf_dict['zookeeper.root_path'] != self.yaml_conf_dict['zookeeper']['zk_root_path']:
            log.warning('zk_root_path of taskmanager {} and yam conf do not match'.format(conf_dict['server.host']))
            flag = False
        return flag

    def validate(self):
        flag = True
        for item in self.detail_conf_map['nameserver']:
            if not self.check_zk_conf('nameserver', item) : flag = False
            if 'system_table_replica_num' in item and int(item['system_table_replica_num']) > len(self.yaml_conf_dict['tablet']):
                log.warning('system_table_replica_num {} in {} is greater than tablets number'.format(
                    item['system_table_replica_num'], item['endpoint']))
                flag = False
        for item in self.detail_conf_map['tablet']:
            if not self.check_zk_conf('tablet', item) : flag = False
        for item in self.detail_conf_map['taskmanager']:
            if not self.check_task_manager_zk_conf(item) : flag = False
        return flag


class TaskManagerConfValidator:
    def __init__(self, conf_dict):
        self.conf_dict = conf_dict
        self.default_conf_dict = {
            'server.host' : '0.0.0.0',
            'server.port' : '9902',
            'zookeeper.cluster' : '',
            'zookeeper.root_path' : '',
            'spark.master' : 'local',            
            'spark.yarn.jars' : '',
            'spark.home' : '',
            'prefetch.jobid.num' : '1',
            'job.log.path' : '../log/',
            'external.function.dir' : './udf/',
            'job.tracker.interval' : '30',
            'spark.default.conf' : '',
            'spark.eventLog.dir' : '',
            'spark.yarn.maxAppAttempts' : '1',
            'offline.data.prefix' : 'file:///tmp/openmldb_offline_storage/',
        }
        self.fill_default_conf()
        self.flag = True

    def fill_default_conf(self):
        for key in self.default_conf_dict:
            if key not in self.conf_dict:
                self.conf_dict[key] = self.default_conf_dict[key]

    def check_noempty(self):
        no_empty_keys = ['zookeeper.cluster', 'zookeeper.root_path', 'job.log.path', 'external.function.dir', 'offline.data.prefix']
        for item in no_empty_keys:
            if self.conf_dict[item] == '':
                log.warning(f'{item} should not be empty')
                self.flag = False

    def check_port(self):
        if not self.conf_dict['server.port'].isnumeric():
            log.warning('port should be number')
            self.flag = False
            return
        port = int(self.conf_dict['server.port'])
        if port < 1 or port > 65535:
            log.warning('port should be in range of 1 through 65535')
            self.flag = False

    def check_spark(self):
        spark_master = self.conf_dict['spark.master'].lower()
        is_local = spark_master.startswith('local')
        if not is_local and spark_master not in ['yarn', 'yarn-cluster', 'yarn-client']:
            log.warning('spark.master should be local, yarn, yarn-cluster or yarn-client')
            self.flag = False
        if spark_master.startswith('yarn'):
            if self.conf_dict['spark.yarn.jars'].startswith('file://'):
                log.warning('spark.yarn.jars should not use local filesystem for yarn mode')
                self.flag = False
            if self.conf_dict['spark.eventLog.dir'].startswith('file://'):
                log.warning('spark.eventLog.dir should not use local filesystem for yarn mode')
                self.flag = False
            if self.conf_dict['offline.data.prefix'].startswith('file://'):
                log.warning('offline.data.prefix should not use local filesystem for yarn mode')
                self.flag = False

        spark_default_conf = self.conf_dict['spark.default.conf']
        if spark_default_conf != '':
            spark_jars = spark_default_conf.split(';')
            for spark_jar in spark_jars:
                if spark_jar != '':
                    kv = spark_jar.split('=')
                    if len(kv) < 2:
                        log.warning(f'spark.default.conf error format of {spark_jar}')
                        self.flag = False
                    elif not kv[0].startswith('spark'):
                        log.warning(f'spark.default.conf config key should start with \'spark\' but get {kv[0]}')
                        self.flag = False

        if int(self.conf_dict['spark.yarn.maxAppAttempts']) < 1:
            log.warning('spark.yarn.maxAppAttempts should be larger or equal to 1')
            self.flag = False

    def check_job(self):
        if int(self.conf_dict['prefetch.jobid.num']) < 1:
            log.warning('prefetch.jobid.num should be larger or equal to 1')
            self.flag = False
        jobs_path = self.conf_dict['job.log.path']
        if jobs_path.startswith('hdfs') or jobs_path.startswith('s3'):
            log.warning('job.log.path only support local filesystem')
            self.flag = False
        if int(self.conf_dict['job.tracker.interval']) <= 0:
            log.warning('job.tracker.interval interval should be larger than 0')
            self.flag = False

    def validate(self):
        self.check_noempty()
        self.check_port()
        self.check_spark()
        self.check_job()
        return self.flag
