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
import argparse
import json
import numpy as np
import requests
import tornado.ioloop
import tornado.web
from xgboost.sklearn import XGBClassifier
import logging

logging.basicConfig(encoding="utf-8", level=logging.INFO, format="%(asctime)s-%(name)s-%(levelname)s-%(message)s")

arg_keys = ["endpoint", "database", "deployment", "model_path"]
bst = XGBClassifier()
# schema column type, ref hybridse::sdk::DataTypeName
table_schema = []
url = ""


def build_feature(res):
    """
    The first value in list is the label column, it's dummy.
    Real-time feature has it, cuz the history data in OpenMLDB is the training data too.
    It'll have this column, but no effect to feature extraction.

    :param res: an OpenMLDB reqeust response
    :return: real feature
    """
    # col label is dummy, so start from 1
    return np.array([res[1:]])


class SchemaHandler(tornado.web.RequestHandler):
    def get(self):
        json_schema = json.dumps(table_schema)
        self.write(json_schema)


def request_row_cvt(json_row):
    """
    convert json to array, using table schema
    APIServer request format only has array now
    :param json_row: row in json format
    :return: array
    """
    row_data = []
    for col in table_schema:
        row_data.append(json_row.get(col["name"], "") if col["type"] == "string" else json_row.get(col["name"], 0))
    logging.info("request row: %s", row_data)
    return row_data


def get_result(response):
    result = json.loads(response.text)
    logging.info("request result: %s", result)
    return result["data"]["data"]


class PredictHandler(tornado.web.RequestHandler):
    """Class PredictHandler."""
    def post(self):
        # only one row
        row = json.loads(self.request.body)
        data = {"input": [request_row_cvt(row)]}
        # request to OpenMLDB
        response = requests.post(url, json=data)

        # result is a list, even we just do a single request
        for res in get_result(response):
            ins = build_feature(res)
            logging.info(f"feature: {res}")
            self.write("real-time feature:\n" + str(res) + "\n")
            prediction = bst.predict(ins)
            self.write(
                "---------------predict whether is attributed -------------\n")
            self.write(f"{str(prediction[0])}")
            logging.info(f"prediction: {prediction}")


class MainHandler(tornado.web.RequestHandler):
    """Class of MainHandler."""
    def get(self):
        self.write("real time fe request demo\n")


def args_validator(update_info):
    return any(key in update_info for key in arg_keys)


def make_url():
    return f"http://{global_args['endpoint']}/dbs/{global_args['database']}/deployments/{global_args['deployment']}"


def update_schema():
    r = requests.get(url)
    rs = json.loads(r.text)
    global table_schema
    table_schema = rs["data"]["input_schema"]


def update_model():
    bst.load_model(fname=global_args["model_path"])


def update_all():
    global url
    url = make_url()
    update_schema()
    logging.info("url and schema updated")
    update_model()
    logging.info("model updated")


class UpdateHandler(tornado.web.RequestHandler):
    """Class of UpdateHandler."""
    def post(self):
        update_info = json.loads(self.request.body)
        # must use the full names
        global global_args
        logging.info("before update: %s", global_args)
        # just update if update_info is empty
        # if not, do restrict update
        if update_info and not args_validator(update_info):
            msg = f"invalid arg in {update_info}, valid candidates {arg_keys}"
            self.write(msg)
            return
        global_args = global_args | update_info
        logging.info("update: %s", global_args)
        update_all()
        self.write("ok\n")


def make_app():
    return tornado.web.Application([
        (r"/", MainHandler),
        (r"/schema", SchemaHandler),
        (r"/predict", PredictHandler),
        (r"/update", UpdateHandler),
    ])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-e", "--endpoint", help="specify the endpoint of apiserver", default="localhost:9080")
    parser.add_argument("-m", "--model_path", help="specify the model path", default="/tmp/model.json")
    parser.add_argument("-db", "--database", help="the database you want to request", default="demo_db")
    parser.add_argument("-d", "--deployment", help="the deployment you want to request", default="demo")
    parser.add_argument("-i", "--init", action=argparse.BooleanOptionalAction, help="init immediately, update later",
                        default=True)
    args = parser.parse_args()

    global_args = vars(args)
    print(global_args)
    logging.info("init args: %s", global_args)

    if args.init:
        update_all()
    app = make_app()
    app.listen(8881)
    tornado.ioloop.IOLoop.current().start()
