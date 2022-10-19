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

import glob
import os

import pandas as pd
from sklearn.metrics import accuracy_score
from sklearn.metrics import classification_report
from sklearn.model_selection import train_test_split
from xgboost.sklearn import XGBClassifier


def read_dataset(train_feature_path):
    if train_feature_path.startswith("/"):
        # local file
        if '*' in train_feature_path:
            return pd.concat(map(pd.read_csv, glob.glob(os.path.join('', train_feature_path))))
        else:
            return pd.read_csv(train_feature_path)
    else:
        raise Exception("remote files is unsupported")


# assume that the first column is the label
def prepare_dataset(train_df, seed, test_size):
    # drop column label
    X_data = train_df.drop('is_attributed', axis=1)
    y = train_df.is_attributed

    # Split the dataset into train and Test
    return train_test_split(
        X_data, y, test_size=test_size, random_state=seed
    )


def xgboost_train(X_train, X_test, y_train, y_test, model_path):
    print('Training by xgb')
    # default is binary:logistic
    train_model = XGBClassifier(use_label_encoder=False).fit(X_train, y_train)
    pred = train_model.predict(X_test)
    print('Classification report:\n', classification_report(y_test, pred))
    auc = accuracy_score(y_test, pred) * 100
    print(f'Accuracy score: {auc}')

    print('Save model to ', model_path)
    train_model.save_model(model_path)
    return auc


# only csv now
def train(train_feature_path, model_path, seed=7, test_size=0.25):
    train_df = read_dataset(train_feature_path)
    X_train, X_test, y_train, y_test = prepare_dataset(train_df, seed, test_size)
    return xgboost_train(X_train, X_test, y_train, y_test, model_path)


def train_task(*op_args, **op_kwargs):
    return train(op_args[0], op_args[1])


if __name__ == '__main__':
    print(glob.glob(os.path.join('', '/tmp/feature_data/*.csv')))
    train('/tmp/feature_data/*.csv', '/tmp/model.json')
