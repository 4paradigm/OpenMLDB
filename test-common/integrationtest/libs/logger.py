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

# coding=utf-8
import logging
import datetime
import os
import libs.conf as conf

log_level = conf.log_level
log_path = os.getenv("testlogpath")

log_format = '%(asctime)s - %(filename)s:%(lineno)d - %(levelname)s - %(message)s'
curDate = datetime.date.today() - datetime.timedelta(days=0)
infoLogName = r'%s/info_%s.log' %(log_path, curDate)
errorLogName = r'%s/error_%s.log' %(log_path, curDate)

formatter = logging.Formatter(log_format)

infoLogger = logging.getLogger("infoLog")
errorLogger = logging.getLogger("errorLog")

infoLogger.setLevel(eval('logging.' + log_level.upper()))
errorLogger.setLevel(logging.ERROR)

infoHandler = logging.FileHandler(infoLogName, 'a')
infoHandler.setFormatter(formatter)

errorHandler = logging.FileHandler(errorLogName, 'a')
errorHandler.setFormatter(formatter)

testHandler = logging.StreamHandler()
testHandler.setLevel(eval('logging.' + log_level.upper()))
testHandler.setFormatter(formatter)

infoLogger.addHandler(infoHandler)
infoLogger.addHandler(testHandler)
errorLogger.addHandler(errorHandler)
