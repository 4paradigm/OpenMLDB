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

import sys,os

#from fedb import driver
import time
from nb_log import LogManager


log = LogManager('fesql-auto-test').get_logger_and_add_handlers()

def test_smoke():
    print("hello")
    options = driver.DriverOptions("172.27.128.37:16181","/fedb_0903")
    sdk = driver.Driver(options)
    assert sdk.init()
    db_name = "test_zw"
    with open("tables.txt","r") as f:
        for line in f.readlines():
            tableName = line.strip()
            ok, error = sdk.executeDDL(db_name, "drop table " + tableName + ";")
            assert ok == True

    print("end")

if __name__ == "__main__":
    #test_smoke()
    pass


