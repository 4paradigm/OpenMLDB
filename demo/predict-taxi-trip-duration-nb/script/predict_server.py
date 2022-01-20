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

import numpy as np
import tornado.web
import tornado.ioloop
import json
import lightgbm as lgb
import sqlalchemy as db
import requests
import argparse

bst = None

table_schema = [
	("id", "string"),
	("vendor_id", "int"),
	("pickup_datetime", "timestamp"),
	("dropoff_datetime", "timestamp"),
	("passenger_count", "int"),
	("pickup_longitude", "double"),
	("pickup_latitude", "double"),
	("dropoff_longitude", "double"),
	("dropoff_latitude", "double"),
	("store_and_fwd_flag", "string"),
	("trip_duration", "int"),
]

url = ""

def get_schema():
    dict_schema = {}
    for i in table_schema:
        dict_schema[i[0]] = i[1]
    return dict_schema

dict_schema = get_schema()
json_schema = json.dumps(dict_schema)

def build_feature(rs):
    var_Y = [rs[0]]
    var_X = [rs[1:12]]
    return np.array(var_X)

class SchemaHandler(tornado.web.RequestHandler):
    def get(self):
        self.write(json_schema)

class PredictHandler(tornado.web.RequestHandler):
    def post(self):
        row = json.loads(self.request.body)
        data = {}
        data["input"] = []
        row_data = []
        for i in table_schema:
            if i[1] == "string":
                row_data.append(row.get(i[0], ""))
            elif i[1] == "int" or i[1] == "double" or i[1] == "timestamp" or i[1] == "bigint":
                row_data.append(row.get(i[0], 0))
            else:
                row_data.append(None)
        data["input"].append(row_data)       
        rs = requests.post(url, json=data)
        result = json.loads(rs.text)
        for r in result["data"]["data"]:
            ins = build_feature(r)
            self.write("----------------ins---------------\n")
            self.write(str(ins) + "\n")
            duration = bst.predict(ins)
            self.write("---------------predict trip_duration -------------\n")
            self.write("%s s"%str(duration[0]))

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("real time execute sparksql demo")

def make_app():
    return tornado.web.Application([
        (r"/", MainHandler),
        (r"/schema", SchemaHandler),
        (r"/predict", PredictHandler),
    ])

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("endpoint",  help="specify the endpoint of apiserver")
    parser.add_argument("model_path",  help="specify the model path")
    args = parser.parse_args()
    url = "http://%s/dbs/demo_db/deployments/demo" % args.endpoint
    bst = lgb.Booster(model_file=args.model_path)
    app = make_app()
    app.listen(8887)
    tornado.ioloop.IOLoop.current().start()
