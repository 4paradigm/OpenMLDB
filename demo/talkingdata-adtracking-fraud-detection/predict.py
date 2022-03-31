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

import requests
import os
import base64
import random
import time
import hashlib

url = "http://127.0.0.1:8887/predict"
req ={"ip":115115,
	"app":8,
	"device":1,
	"os":13,
	"channel":145,
	"click_time":"2017-11-06 16:07:47",
	"is_attributed":0}
r = requests.post(url, json=req)
print(r.text)
