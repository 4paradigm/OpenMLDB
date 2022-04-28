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

import lightgbm as lgb
import pandas as pd
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import train_test_split
import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument("feature_path", help="specify the feature path")
parser.add_argument("model_path", help="specify the model path")
args = parser.parse_args()

feature_path = args.feature_path
# merge file
if os.path.isdir(feature_path):
    path_list = os.listdir(feature_path)
    new_file = "/tmp/merged_feature.csv"
    with open(new_file, 'w') as wf:
        has_write_header = False
        for filename in path_list:
            if filename == "_SUCCESS" or filename.startswith('.'):
                continue
            with open(os.path.join(feature_path, filename), 'r') as f:
                first_line = True
                for line in f.readlines():
                    if first_line is True:
                        first_line = False
                        if has_write_header is False:
                            has_write_header = True
                        else:
                            continue
                    wf.writelines(line)
    feature_path = new_file

# run batch sql and get instances
df = pd.read_csv(feature_path);
train_set, predict_set = train_test_split(df, test_size=0.2)
y_train = train_set['trip_duration']
x_train = train_set.drop(columns=['trip_duration'])
y_predict = predict_set['trip_duration']
x_predict = predict_set.drop(columns=['trip_duration'])


# training model with regression
print('Starting training...')
lgb_train = lgb.Dataset(x_train, y_train)
lgb_eval = lgb.Dataset(x_predict, y_predict, reference=lgb_train)

# specify your configurations as a dict
params = {
    'boosting_type': 'gbdt',
    'objective': 'regression',
    'metric': {'l2', 'l1'},
    'num_leaves': 31,
    'learning_rate': 0.05,
    'feature_fraction': 0.9,
    'bagging_fraction': 0.8,
    'bagging_freq': 5,
    'verbose': 0
}

gbm = lgb.train(params,
                lgb_train,
                num_boost_round=20,
                valid_sets=lgb_eval,
                early_stopping_rounds=5)

gbm.save_model(args.model_path)
print("save model.txt done")
