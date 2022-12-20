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
"""Module of request predict in script"""
import requests

url = "http://127.0.0.1:8887/predict"

req = {"reqId": "200080_5505_2016-03-15 20:43:04",
       "eventTime": 1458045784000,
       "main_id": "681271",
       "pair_id": "200080_5505",
       "user_id": "200080",
       "sku_id": "5505",
       "time": 1458045784000,
       "split_id": 1,
       "time1":"2016-03-15 20:43:04"}

res = requests.post(url, json=req)
print(res.text)

