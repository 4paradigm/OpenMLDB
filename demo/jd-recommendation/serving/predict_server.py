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

"""Module of predict server"""
import numpy as np
import tornado.web
import tornado.ioloop
import json
import requests
import argparse
from process_res import process_infer

#bst = None

table_schema = [
    ("reqId", "string"),
    ("eventTime", "timestamp"),
    ("main_id", "string"),
    ("pair_id", "string"),
    ("user_id", "string"),
    ("sku_id", "string"),
    ("time", "bigint"),
    ("split_id", "int"),
    ("time1", "string"),
]

apiserver_url = ""
triton_url = ""

def get_schema():
    dict_schema_tmp = {}
    for i in table_schema:
        dict_schema_tmp[i[0]] = i[1]
    return dict_schema_tmp

dict_schema = get_schema()
json_schema = json.dumps(dict_schema)


class SchemaHandler(tornado.web.RequestHandler):
    def get(self):
        self.write(json_schema)


class PredictHandler(tornado.web.RequestHandler):
    """Class of PredictHandler docstring."""
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
        rs = requests.post(apiserver_url, json=data)
        result = json.loads(rs.text)
        for r in result["data"]["data"]:
            res = np.array(r)
            self.write("----------------ins---------------\n")
            self.write(str(res) + "\n")
            pred = process_infer(triton_url, res)
            self.write("---------------predict change of purchase -------------\n")
            self.write(f"{str(pred)}")

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
    parser.add_argument("apiserver", help="specify the endpoint of openmldb apiserver")
    parser.add_argument("triton", help="specify the endpoint of oneflow triton")
    args = parser.parse_args()
    apiserver_url = f"http://{args.apiserver}/dbs/JD_db/deployments/demo"
    triton_url = args.triton
    app = make_app()
    app.listen(8887)
    tornado.ioloop.IOLoop.current().start()
