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
req = {"id": "id0376262",
       "vendor_id": 1,
       "pickup_datetime": 1467302350000,
       "dropoff_datetime": 1467304896000,
       "passenger_count": 2,
       "pickup_longitude": -73.873093,
       "pickup_latitude": 40.774097,
       "dropoff_longitude": -73.926704,
       "dropoff_latitude": 40.856739,
       "store_and_fwd_flag": "N",
       "trip_duration": 1}
r = requests.post(url, json=req)
print(r.text)

assert 'predict trip_duration' in r.text
