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
import allure
import pytest

from nb_log import LogManager

from common.standalone_test import StandaloneTest
from executor import fedb_executor
from util.test_util import getCases

log = LogManager('python-sdk-test').get_logger_and_add_handlers()

class TestStandaloneDML(StandaloneTest):

    #全部pass
    @pytest.mark.parametrize("testCase", getCases(["/function/dml/test_insert.yaml"]))
    @allure.feature("dml")
    @allure.story("insert")
    def test_insert(self, testCase):
        print(testCase)
        fedb_executor.build(self.connect, testCase).run()

    #全部pass
    @pytest.mark.parametrize("testCase", getCases(["/function/dml/multi_insert.yaml"]))
    @allure.feature("dml")
    @allure.story("multi_insert")
    def test_insert(self, testCase):
        print(testCase)
        fedb_executor.build(self.connect, testCase).run()