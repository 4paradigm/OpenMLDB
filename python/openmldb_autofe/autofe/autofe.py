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

# fmt:off
import os
import logging
# disable tensorflow log about avx
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3' 
# import tensorflow as tf
# tf.get_logger().setLevel('ERROR')

# NOTICE: autox import packages may init the log, so we import it on the top and remove the handlers set by autox
# pylint: disable=unused-import
from autox.autox_competition.util import log
logging.getLogger().handlers = []
# fmt:on
from autox.autox_competition.process_data import feature_filter, auto_encoder
from autox.autox_competition.process_data.feature_type_recognition import Feature_type_recognition
from autox.autox_competition.models import CrossLgbRegression, CrossXgbRegression
from autox.autox_competition.models.classifier import CrossLgbBiClassifier, CrossXgbBiClassifier

from pandas.api.types import is_datetime64_dtype
import glob
import re
import requests
import time
import pandas as pd
from sklearn.model_selection import train_test_split
from yaml import FullLoader

default_offline_feature_path = '/tmp/autofe_offline_feature'


class OpenMLDBSQLGenerator():
    def __init__(self, conf, relations=[]):
        self.conf = conf
        self.info_ = {}
        self.info_['target'] = self.conf['label']
        # TODO relations for ?
        self.info_['relations'] = relations
        # better to use file:// prefix if local file
        self.info_['output_file_path'] = self.conf.get(
            'offline_feature_path', f'file://{default_offline_feature_path}')

        # read tables
        self.dfs = {}
        for t in self.conf['tables']:
            # TODO multi format
            self.dfs[t['table']] = pd.read_parquet(t['file_path'])

        self.info_['data_types'] = {}
        if 'data_types' not in self.conf:
            for table_name in self.dfs.keys():
                df = self.dfs[table_name]
                logging.debug(df)

                feature_type_recognition = Feature_type_recognition()
                feature_type = feature_type_recognition.fit(df)
                # AutoX can't recognize datetime64
                for col in feature_type.keys():
                    if feature_type[col] is None and is_datetime64_dtype(
                            df[col].dtype):
                        feature_type[col] = 'datetime'
                self.info_['data_types'][table_name] = feature_type
        else:
            self.info_['data_types'] = self.conf['data_types']

        self.main_table = self.conf['main_table']
        assert self.main_table in self.dfs, 'main table is not in tables'
        logging.debug(self.info_)

    def gen_windows_sql(self):
        sql = ""
        for p, window_now in enumerate(self.window_list):
            sql += window_now["name"]
            sql += " AS ("
            if ('union' in window_now and window_now['union'] != ''):
                sql += " UNION " + window_now["union"]
            sql += " PARTITION BY " + window_now["partition_by"]
            sql += " ORDER BY " + window_now["order_by"]
            sql += " " + window_now["window_type"].upper()
            sql += " BETWEEN " + window_now["start"]
            sql += " AND "
            sql += window_now["end"]
            if p == len(self.window_list) - 1:
                sql += ")\n"
            else:
                sql += "),\n"
        return sql

    def diff_features(self):
        res = []
        # TODO lag diy
        lag_shift = [1, 3, 5, 10, 30, 50]
        for lag_num in lag_shift:
            res.append('lag' + str(lag_num))
            # lagA-B means "lag(,A) - lag(,B)"
            res.append('lag' + str(lag_num) + '-0')
        return res

    @staticmethod
    def multi_op_trans(multi_op, function, col_name):
        assert function.startswith(multi_op)
        sql = multi_op + '(' + col_name + ","
        # e.g. lag3-0 -> [lag3, -, 0]; lag0 -> [lag0]
        func_splited = re.split("([-+*/])", function)
        # get the first shift
        sql += func_splited[0][len(multi_op):]
        op_list = ['-', '+', '*', '/']
        if len(func_splited) > 1:
            assert func_splited[1] in op_list and len(
                func_splited) == 3, "unsupported"
            sql += ')' + func_splited[1] + multi_op + \
                '(' + col_name + ',' + func_splited[2]
        sql += ')'
        return sql

    def time_series_feature_sql(self):
        '''
        recipe is as follows:
        SELECT col1,
        sum(col1) over w1 as col1_w1
        col2 over w2 as col2_w2
        ...
        from table1
        WINDOW
        w1 AS ..rows_range/rows BETWEEN []
        w2 AS ..
        INTO OUTFILE ..
        '''
        # shift_dict = {}
        # shift_dict['year'] = [1, 2, 3, 4, 5, 10, 20]
        # shift_dict['month'] = [1, 2, 3, 4, 8, 12, 24, 60, 120]
        # shift_dict['day'] =
        # shift_dict['minute'] = [1, 2, 3, 5, 10,
        #                         15, 30, 45, 60, 120, 240, 720, 1440]

        # classify by type
        type_col_map = {}
        type_col_map['datetime'] = []
        type_col_map['num'] = []
        type_col_map['cat'] = []
        for k, v in self.info_['data_types'][self.main_table].items():
            if (k) not in type_col_map[v]:
                type_col_map[v].append(k)

        multi_operator_func_list = ['lag']
        function_list = ['sum', 'avg', 'min', 'max',
                         'count', 'log', 'lag0']  # lag0 means lag(,0)

        gen_diff_features = True
        if gen_diff_features:
            function_list += self.diff_features()

        # -1 means it's not multi op, otherwise the multi op idx in multi_operator_func_list
        is_multi_op_func = [-1] * len(function_list)
        for idx, func in enumerate(function_list):
            for multi_op_idx, multi_op in enumerate(multi_operator_func_list):
                if func.startswith(multi_op):
                    assert is_multi_op_func[idx] == -1, 'reassign'
                    is_multi_op_func[idx] = multi_op_idx

        '''
        w AS (PARTITION BY vendor_id ORDER BY pickup_datetime ROWS_RANGE BETWEEN 1d PRECEDING AND CURRENT ROW),
        w2 AS (PARTITION BY passenger_count ORDER BY pickup_datetime ROWS_RANGE BETWEEN 1d PRECEDING AND CURRENT ROW);
        '''
        # window range input
        self.window_list = self.conf['windows']

        sql = "SELECT "
        # column names -> column define part
        # e.g. sum(c1) over w1 as c1_sum_xx: c1_sum_xx -> sum(c1) over w1
        self.column_name_to_sql = {}

        # the main table columns are usually valuable, so add them to features
        # TODO include string feature?
        self.processed_column_name_list = self.dfs[self.main_table].columns.values.tolist(
        )
        for col_name in self.processed_column_name_list:
            self.column_name_to_sql[col_name] = col_name
            sql += col_name + ","
        logging.debug(f'original columns to features: {sql}')

        # function list will apply on every window
        for window in self.window_list:
            # only num cols do agg in window
            for col_name in type_col_map['num']:
                if col_name == self.info_['target']:
                    continue
                # gen all func for this col
                for idx, func in enumerate(function_list):
                    func_processed_name = func.replace(
                        "-", "minus").replace("+", "add").replace("*", "multiply").replace("/", "divide")
                    column_sql = ""
                    '''
                    if have_multi_op:
                        sql+=multi_operator_func_list[multi_op_index]
                    '''
                    multi_op_idx = is_multi_op_func[idx]
                    if multi_op_idx == -1:
                        column_sql += func + '(' + col_name + ')'
                    else:
                        # multi op
                        multi_op = multi_operator_func_list[multi_op_idx]
                        column_sql += self.multi_op_trans(
                            multi_op, func, col_name)

                    new_column_name = (
                        self.main_table + "__" + func_processed_name + "__" + col_name + "__" + window["name"])

                    column_sql += ' OVER ' + \
                        window["name"] + " AS " + new_column_name
                    sql += column_sql
                    self.column_name_to_sql[new_column_name] = column_sql
                    self.processed_column_name_list.append(new_column_name)
                    sql += ","
                    sql += "\n "

        sql += " FROM " + self.main_table + " \n"

        # window definations
        sql += " WINDOW "
        sql += self.gen_windows_sql()

        # offline feature store
        feature_save_path = self.info_['output_file_path'] + '/first_features'
        # overwrite so we can retry
        save_sql = f"INTO OUTFILE '{feature_save_path}' OPTIONS(format='parquet', mode='overwrite');"
        sql += save_sql
        return sql, feature_save_path

    def decode_time_series_feature_sql_column(self, topk_feature_list):
        sql = "SELECT "
        for feature_column_name in topk_feature_list:
            # feature column names x -> real sql part "xxx over xx as x"
            sql += self.column_name_to_sql[feature_column_name] + ",\n"

        sql += " FROM " + self.main_table + " \n"
        sql += " WINDOW "
        # TODO: remove unused windows
        sql += self.gen_windows_sql()
        # sql += ";"
        return sql


class OpenMLDBHttpHelper:
    def __init__(self, http_addr, db) -> None:
        self.db = db
        self.url = f'http://{http_addr}/dbs/{db}'
        self.headers = {'content-type': 'application/json',
                        'Accept-Charset': 'UTF-8'}
        self.table_list = []
        self.deployment = ""

    def execute(self, sql, mode):
        r = requests.post(
            self.url, json={"sql": sql, "mode": mode}, headers=self.headers)
        if r.json()["code"] != 0:
            raise Exception(f'openmldb return error: {r.json()}')
        return r.text

    def ddl(self, sql):
        # ddl mode can be anyone
        return self.execute(sql, "online")

    def ping(self):
        return self.execute("show components", "online")

    def setup_db(self):
        self.ddl(f'create database if not exists {self.db}')

    def offline_load(self, table, path):
        # source format
        return self.execute(
            f"load data infile '{path}' into table {table} options(mode='overwrite', format='parquet')", "offsync")

    def deploy(self, deployment_name, sql):
        self.deployment = deployment_name
        return self.ddl(f"DEPLOY {deployment_name} {sql}")

    def setup_tables(self, table_list):
        self.table_list = table_list
        for t in table_list:
            self.ddl(f'create table if not exists {t["table"]}({t["schema"]})')
            # load data offline
            self.offline_load(t["table"], t["file_path"])

    def cleanup(self):
        self.ddl(f'drop deployment {self.deployment}')
        for t in self.table_list:
            self.ddl(f'drop table {t["table"]}')
        # db may have other tables, it's ok if cannot drop db
        try:
            self.ddl(f'drop database {self.db}')
        except Exception:
            pass


class AutoXTrain:
    def __init__(self, debug=False) -> None:
        self.debug = debug
    # TODO read from yaml?
    def get_top_features(self, train_set, test_set, id_list, label, offline_feature_path, k=10):
        # TODO auto_encoder?
        logging.info("feature filter")
        # won't use id col and label col
        # import feature_filter with log, don't move it to the top
        
        used_features = feature_filter(train_set, test_set, id_list, label)
        logging.info(f"used_features: {used_features}")

        # 模型训练
        task_type = 'regression'
        metric = 'rmse'
        if train_set[label].nunique() == 2:
            task_type = 'binary'

        logging.info(f"start training lightgbm model, type {task_type}")
        n_fold = 5
        if self.debug:
            logging.info("debug mode, train faster")
            n_fold = 2

        if task_type == 'regression':
            model_lgb = CrossLgbRegression(metric=metric, n_fold=n_fold)
            model_lgb.fit(
                train_set[used_features],
                train_set[label],
                tuning=False,)
        elif task_type == 'binary':
            model_lgb = CrossLgbBiClassifier(n_fold=n_fold)
            model_lgb.fit(train_set[used_features],
                          train_set[label], tuning=False,)

        feature_importance = model_lgb.feature_importances_
        logging.info(f"feature_importance: {feature_importance}")
        feature_importance.to_csv(
            offline_feature_path + '/feature_importance.csv')
        topk_in_bound = min(k, len(list(feature_importance['feature'])))
        return [x for x in list(
            feature_importance['feature'])][:topk_in_bound]


def main():
    import yaml
    import sys
    f = open(sys.argv[1])
    data = f.read()
    yaml_reader = yaml.load(data, Loader=FullLoader)
    logging.basicConfig(format='[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
                        level=yaml_reader['log_level'])
    
    sql_generator = OpenMLDBSQLGenerator(yaml_reader)
    print('generate all features')
    output_sql, feature_path = sql_generator.time_series_feature_sql()
    logging.info(output_sql)

    print('do feature extraction')
    helper = OpenMLDBHttpHelper(yaml_reader['apiserver'], yaml_reader['db'])
    helper.setup_db()
    helper.setup_tables(yaml_reader['tables'])
    helper.execute(output_sql, "offsync")

    # offline feature data process
    label = yaml_reader['label']
    # load all files in feature_path, parquet is better

    def remove_prefix(text, prefix): return text[len(
        prefix):] if text.startswith(prefix) else text
    feature_path = remove_prefix(feature_path, 'file://')
    logging.info(f'load {feature_path}')
    train_df = pd.concat(map(pd.read_parquet, glob.glob(
        os.path.join('', feature_path + '/*.parquet'))))

    train_set, test_set_with_y = train_test_split(train_df, train_size=0.8)
    test_set = test_set_with_y.drop(columns=label)
    logging.info(train_set)
    logging.info(test_set)

    offline_feature_path = yaml_reader.get(
        'offline_feature_path', default_offline_feature_path)

    # save for backup
    train_name = 'train.parquet'
    test_name = 'test.parquet'
    train_set.to_parquet(offline_feature_path + '/' + train_name, index=False)
    test_set.to_parquet(offline_feature_path + '/' + test_name, index=False)

    id_list = ['id']
    topk = yaml_reader.get('topk')
    print(f'get top {topk} features')
    topk_features = AutoXTrain(debug=yaml_reader.get('debug', False)).get_top_features(
        train_set, test_set, id_list, label, offline_feature_path, topk)
    print(f'top {len(topk_features)} feas: {topk_features}')
    logging.info(train_set[id_list + topk_features])
    logging.info(test_set[id_list + topk_features])

    print('final sql')
    # decode feature to final sql
    final_sql = sql_generator.decode_time_series_feature_sql_column(
        topk_features)
    print(final_sql)

    # deploy the final sql to ensure that the sql can be deployed online
    deploy_service_name = "feature_data_test_auto_sql_generator" + \
        str(int(time.time()))
    helper.deploy(deploy_service_name, final_sql)

    # cleanup
    helper.cleanup()

if __name__ == "__main__":
    main()
