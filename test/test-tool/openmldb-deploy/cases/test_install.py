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
# limitations under the License

import pytest
import case_conf 
import openmldb.dbapi
import os
import sys

class TestInstall:
    db = None
    cursor = None

    @classmethod
    def setup_class(cls):
        cls.db = openmldb.dbapi.connect(zk=case_conf.conf["zk_cluster"], zkPath=case_conf.conf["zk_root_path"])
        cls.cursor = cls.db.cursor()
        pass

    def test_component(self):
        COMPONENTS = case_conf.conf["components"]
        components = {}
        result = self.cursor.execute("show components;")
        for (endpoint, role, _, status, _) in result.fetchall():
            assert status == 'online'
            components.setdefault(role, [])
            components[role].append(endpoint)
        
        assert len(COMPONENTS["tablet"]) == len(components["tablet"])
        for endpoint in COMPONENTS["tablet"]:
            assert endpoint in components["tablet"]
        assert len(COMPONENTS["nameserver"]) == len(components["nameserver"])
        for endpoint in COMPONENTS["nameserver"]:
            assert endpoint in components["nameserver"]
        if len(COMPONENTS["apiserver"]) > 0:
            assert len(COMPONENTS["apiserver"]) == len(components["apiserver"])
            for endpoint in COMPONENTS["apiserver"]:
                assert endpoint in components["apiserver"]
        else:
            assert "apiserver" not in components
        if len(COMPONENTS["taskmanager"]) > 0:
            assert len(components["taskmanager"]) > 0
            for endpoint in components["taskmanager"]:
                assert endpoint in COMPONENTS["taskmanager"]
        else:
            assert "taskmanager" not in components

if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", os.path.abspath(__file__)]))
