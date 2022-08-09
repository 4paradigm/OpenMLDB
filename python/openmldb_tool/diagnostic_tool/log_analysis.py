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

log = logging.getLogger(__name__)

class LogAnalysis:
    def __init__(self, role, endpoint, file_list):
        self.role = role
        self.endpoint = endpoint
        self.file_list = file_list
        self.taskmanager_ignore_errors = [
                'Unable to load native-hadoop library for your platform',
                ]

    def check_warning(self, name, line) -> bool:
        if self.role == 'taskmanager':
            if name.startswith('taskmanager'):
                if len(line) < 28:
                    return False
                if not line[:2].isnumeric():
                    return False
                log_level = line[24:28]
                if log_level in ['INFO', 'WARN', 'ERROR'] and log_level != 'INFO':
                    return True
            else:
                if len(line) < 22:
                    return False
                log_level = line[18:22]
                if log_level in ['INFO', 'WARN', 'ERROR'] and log_level != 'INFO':
                    for filter_msg in self.taskmanager_ignore_errors:
                        if line.find(filter_msg) != -1:
                            return False
                    return True
        else:
            if (line.startswith('W') or line.startswith('E')) and line[1].isnumeric():
                return True
        return False

    def analysis_log(self):
        flag = True
        for name, full_path in self.file_list:
            msg = '----------------------------------------\n'
            print_errlog = False
            with open(full_path, 'r', encoding='UTF-8') as f:
                line = f.readline()
                while line:
                    line = line.strip()
                    if line != '':
                        if self.check_warning(name, line):
                            flag = False
                            print_errlog = True
                            msg += line + '\n'
                    line = f.readline()
            if print_errlog:
                log.warn(f'{self.role} {self.endpoint} have error logs in {name}:')
                log.info(f'error msg: \n{msg}')
        return flag
