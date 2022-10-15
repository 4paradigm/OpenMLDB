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

from diagnostic_tool.collector import Collector, LocalCollector
from diagnostic_tool.dist_conf import DistConfReader, ConfParser, DistConf
from diagnostic_tool.conf_validator import YamlConfValidator, StandaloneConfValidator, ClusterConfValidator, TaskManagerConfValidator
from diagnostic_tool.log_analysis import LogAnalysis
from diagnostic_tool.server_checker import ServerChecker
import diagnostic_tool.util as util
import sys
import logging
from absl import app
from diagnostic_tool.conf_option import ConfOption

log = logging.getLogger(__name__)

def check_version(version_map : dict):
    f_version = ''
    f_endpoint = ''
    f_role = ''
    flag = True
    for k, v in version_map.items():
        for endpoint, cur_version in v:
            if f_version == '':
                f_version = cur_version
                f_endpoint = endpoint
                f_role = k
            if cur_version != f_version:
                log.warn(f'version mismatch. {k} {endpoint} version {cur_version}, {f_role} {f_endpoint} version {f_version}')
                flag = False
    return flag, f_version

def check_conf(yaml_conf_dict, conf_map):
    detail_conf_map = {}
    flag = True
    for role, v in conf_map.items():
        for endpoint, values in v.items():
            for _, path in values:
                detail_conf_map.setdefault(role, [])
                cur_conf = ConfParser(path).conf()
                detail_conf_map[role].append(cur_conf)
                if yaml_conf_dict['mode'] == 'cluster' and role == 'taskmanager':
                    taskmanager_validator = TaskManagerConfValidator(cur_conf)
                    if not taskmanager_validator.validate():
                        log.warn(f'taskmanager {endpoint} conf check failed')
                        flag = False

    if yaml_conf_dict['mode'] == 'standalone':
        conf_validator = StandaloneConfValidator(detail_conf_map['nameserver'][0], detail_conf_map['tablet'][0])
    else:
        conf_validator = ClusterConfValidator(yaml_conf_dict, detail_conf_map)
    if conf_validator.validate() and flag:
        log.info('check conf ok')
    else:
        log.warn('check conf failed')

def check_log(yaml_conf_dict, log_map):
    flag = True
    for role, v in log_map.items():
        for endpoint, values in v.items():
            log_analysis = LogAnalysis(role, endpoint, values)
            if not log_analysis.analysis_log() : flag = False
    if flag:
        log.info('check log ok')

def run_test_sql(dist_conf : DistConf, print_sdk_log):
    checker = ServerChecker(dist_conf.full_conf, print_sdk_log)
    if checker.run_test_sql():
        log.info('test sql execute ok.')

def main(argv):
    conf_opt = ConfOption()
    if not conf_opt.init():
        return
    util.clean_dir(conf_opt.data_dir)
    dist_conf = DistConfReader(conf_opt.dist_conf).conf()
    yaml_validator = YamlConfValidator(dist_conf.full_conf)
    if not yaml_validator.validate():
        log.warning("check yaml conf failed")
        sys.exit()
    log.info("check yaml conf ok")

    log.info("mode is {}".format(dist_conf.mode))
    if dist_conf.mode == 'cluster' and conf_opt.env != 'onebox':
        collector = Collector(dist_conf)
        if conf_opt.check_version():
            version_map = collector.collect_version()
        if conf_opt.check_conf():
            collector.pull_config_files(f'{conf_opt.data_dir}/conf')
        if conf_opt.check_log():
            collector.pull_log_files(f'{conf_opt.data_dir}/log')
        if conf_opt.check_conf() or conf_opt.check_log():
            file_map = util.get_files(conf_opt.data_dir)
            log.debug("file_map: %s", file_map)
    else:
        collector = LocalCollector(dist_conf)
        if conf_opt.check_version():
            version_map = collector.collect_version()
        if conf_opt.check_conf() or conf_opt.check_log():
            file_map = collector.collect_files()
            log.debug("file_map: %s", file_map)

    if conf_opt.check_version():
        flag, version = check_version(version_map)
        if flag:
            log.info(f'openmldb version is {version}')
            log.info('check version ok')
        else:
            log.warn('check version failed')

    if conf_opt.check_conf():
        check_conf(dist_conf.full_conf, file_map['conf'])
    if conf_opt.check_log():
        check_log(dist_conf.full_conf, file_map['log'])
    if conf_opt.check_sql():
        run_test_sql(dist_conf, conf_opt.print_sdk_log())

def run():
    app.run(main)

if __name__ == '__main__':
    app.run(main)
