import gc
import os
from matplotlib.pyplot import table
import pandas as pd
import time
import numpy as np
from sklearn.model_selection import train_test_split
import lightgbm as lgb  # mac needs `brew install libomp`
import sqlalchemy as db


def lgb_modelfit_nocv(params, dtrain, dvalid, predictors, target='target', objective='binary', metrics='auc',
                      feval=None, early_stopping_rounds=20, num_boost_round=3000, verbose_eval=10,
                      categorical_features=None):
    lgb_params = {
        'boosting_type': 'gbdt',
        'objective': objective,
        'metric': metrics,
        'learning_rate': 0.01,
        # 'is_unbalance': 'true',  #because training data is unbalance (replaced with scale_pos_weight)
        'num_leaves': 31,  # we should let it be smaller than 2^(max_depth)
        'max_depth': -1,  # -1 means no limit
        # Minimum number of data need in a child(min_data_in_leaf)
        'min_child_samples': 20,
        'max_bin': 255,  # Number of bucketed bin for feature values
        'subsample': 0.6,  # Subsample ratio of the training instance.
        'subsample_freq': 0,  # frequence of subsample, <=0 means no enable
        # Subsample ratio of columns when constructing each tree.
        'colsample_bytree': 0.3,
        # Minimum sum of instance weight(hessian) needed in a child(leaf)
        'min_child_weight': 5,
        'subsample_for_bin': 200000,  # Number of samples for constructing bin
        'min_split_gain': 0,  # lambda_l1, lambda_l2 and min_gain_to_split to regularization
        'reg_alpha': 0,  # L1 regularization term on weights
        'reg_lambda': 0,  # L2 regularization term on weights
        'nthread': 8,
        'verbose': 0,
        'metric': metrics
    }

    lgb_params.update(params)

    print("preparing validation datasets")

    xgtrain = lgb.Dataset(dtrain[predictors].values, label=dtrain[target].values,
                          feature_name=predictors,
                          categorical_feature=categorical_features
                          )
    xgvalid = lgb.Dataset(dvalid[predictors].values, label=dvalid[target].values,
                          feature_name=predictors,
                          categorical_feature=categorical_features
                          )

    evals_results = {}

    bst1 = lgb.train(lgb_params,
                     xgtrain,
                     valid_sets=[xgtrain, xgvalid],
                     valid_names=['train', 'valid'],
                     evals_result=evals_results,
                     num_boost_round=num_boost_round,
                     early_stopping_rounds=early_stopping_rounds,
                     verbose_eval=10,
                     feval=feval)

    n_estimators = bst1.best_iteration
    print("\nModel Report")
    print("n_estimators : ", n_estimators)
    print(metrics + ":", evals_results['valid'][metrics][n_estimators - 1])

    return bst1

# def xgb_modelfit_nocv(params, dtrain, dvalid, predictors, target='target', objective='binary', metrics='auc',
#                       feval=None,num_boost_round=3000,early_stopping_rounds=20):
#     xgb_params = {
#         'booster': 'gbtree',
#         'obj': objective,
#         'eval_metric': metrics,
#         'num_leaves': 31,  # we should let it be smaller than 2^(max_depth)
#         'max_depth': -1,  # -1 means no limit
#         'max_bin': 255,  # Number of bucketed bin for feature values
#         'subsample': 0.6,  # Subsample ratio of the training instance.
#         'colsample_bytree': 0.3,
#         'min_child_weight': 5,
#         'alpha': 0,  # L1 regularization term on weights
#         'lambda': 0,  # L2 regularization term on weights
#         'nthread': 8,
#         'verbosity': 0,
#     }
#     xgb_params.update(params)

#     print("preparing validation datasets")
#     xgtrain = xgb.DMatrix(dtrain[predictors].values, label=dtrain[target].values)
#     xgvalid = xgb.DMatrix(dvalid[predictors].values, label=dvalid[target].values)

#     evals_results = {}

#     bst1 = xgb.train(xgb_params,
#                      xgtrain,
#                      evals=xgvalid,
#                      evals_result=evals_results,
#                      num_boost_round=num_boost_round,
#                      early_stopping_rounds=early_stopping_rounds,
#                      verbose_eval=10,
#                      feval=feval)

#     n_estimators = bst1.best_iteration
#     print("\nModel Report")
#     print("n_estimators : ", n_estimators)
#     print(metrics + ":", evals_results['valid'][metrics][n_estimators - 1])

#     return bst1


path = 'data/'

# use pandas extension types to support NA in integer column
dtypes = {
    'ip': 'UInt32',
    'app': 'UInt16',
    'device': 'UInt16',
    'os': 'UInt16',
    'channel': 'UInt16',
    'is_attributed': 'UInt8',
    'click_id': 'UInt32'
}

common_schema = [('ip', 'int'), ('app', 'int'), ('device', 'int'),
                 ('os', 'int'), ('channel', 'int'), ('click_time', 'timestamp')]
train_schema = common_schema + [('is_attributed', 'int')]
test_schema = common_schema + [('click_id', 'int')]
print('Prepare train data...')

train_df = pd.read_csv(path + "train.csv", nrows=4000000,
                       dtype=dtypes, usecols=[c[0] for c in train_schema])
len_train = len(train_df)
# take a portion from train sample data
train_df.to_csv("train_sample.csv", index=False)


def column_string(col_tuple) -> str:
    return ' '.join(col_tuple)


schema_string = ','.join(list(map(column_string, train_schema)))

del train_df
gc.collect()


db_name = "demo_db"
table_name = "talkingdata" + str(int(time.time()))
print("Prepare openmldb, db {} table {}".format(db_name, table_name))

engine = db.create_engine(
    'openmldb:///{}?zk=127.0.0.1:6181&zkPath=/openmldb'.format(db_name))
connection = engine.connect()


def nothrow_execute(sql):
    # only used for create table, cuz 'create table if not exist' is not supported now
    try:
        print("execute " + sql)
        ok, rs = connection.execute(sql)
        print(rs)
    except Exception as e:
        print(e)


connection.execute("CREATE DATABASE IF NOT EXISTS {};".format(db_name))
nothrow_execute("CREATE TABLE {}({});".format(table_name, schema_string))

print("Load data to offline storage for training(hard copy)")

connection.execute("SET @@execute_mode='offline';")
# use sync offline job, to make sure `LOAD DATA` finished
connection.execute("SET @@sync_job=true;")
connection.execute("SET @@job_timeout=120000;")
# use soft link after https://github.com/4paradigm/OpenMLDB/issues/1565 fixed
connection.execute("LOAD DATA INFILE 'file://{}' INTO TABLE {}.{} OPTIONS(format='csv',header=true);".format(
    os.path.abspath("train_sample.csv"), db_name, table_name))


print('Feature extraction')
current_path=os.path.abspath(".")
train_feature_file = current_path + "/train_feature.csv"
sql_part = """
select ip, day(click_time) as day, hour(click_time) as hour, 
count(channel) over w1 as qty, 
count(channel) over w2 as ip_app_count, 
count(channel) over w3 as ip_app_os_count
from {}.{} 
window 
w1 as (partition by ip order by click_time ROWS_RANGE BETWEEN 1h PRECEDING AND CURRENT ROW), 
w2 as(partition by ip, app order by click_time ROWS_RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
w3 as(partition by ip, app, os order by click_time ROWS_RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
""".format(db_name, table_name)
# extraction will take time
connection.execute("SET @@job_timeout=1200000;")
connection.execute("{} INTO OUTFILE '{}';".format(
    sql_part, os.path.abspath(train_feature_file)))

# load features from train_feature_file
train_df = pd.read_csv(os.path.abspath(train_feature_file))
val_df = train_df[(len_train - 3000000):len_train]
train_df = train_df[:(len_train - 3000000)]

print("train size: ", len(train_df))
print("valid size: ", len(val_df))

target = 'is_attributed'
predictors = ['app', 'device', 'os', 'channel', 'hour',
              'day', 'qty', 'ip_app_count', 'ip_app_os_count']
categorical = ['app', 'device', 'os', 'channel', 'hour']

gc.collect()

print("Training...")
params = {
    'learning_rate': 0.1,
    # 'is_unbalance': 'true', # replaced with scale_pos_weight argument
    'num_leaves': 7,  # we should let it be smaller than 2^(max_depth)
    'max_depth': 3,  # -1 means no limit
    # Minimum number of data need in a child(min_data_in_leaf)
    'min_child_samples': 100,
    'max_bin': 100,  # Number of bucketed bin for feature values
    'subsample': 0.7,  # Subsample ratio of the training instance.
    'subsample_freq': 1,  # frequence of subsample, <=0 means no enable
    # Subsample ratio of columns when constructing each tree.
    'colsample_bytree': 0.7,
    # Minimum sum of instance weight(hessian) needed in a child(leaf)
    'min_child_weight': 0,
    'scale_pos_weight': 99  # because training data is extremely unbalanced
}

# params_xgb = {
#     'num_leaves': 7,  # we should let it be smaller than 2^(max_depth)
#     'max_depth': 3,  # -1 means no limit
#     'min_child_samples': 100,
#     'max_bin': 100,  # Number of bucketed bin for feature values
#     'subsample': 0.7,  # Subsample ratio of the training instance.
#     # Subsample ratio of columns when constructing each tree.
#     'colsample_bytree': 0.7,
#     # Minimum sum of instance weight(hessian) needed in a child(leaf)
#     'min_child_weight': 0
# }

bst = lgb_modelfit_nocv(params,
                        train_df,
                        val_df,
                        predictors,
                        target,
                        objective='binary',
                        metrics='auc',
                        early_stopping_rounds=50,
                        verbose_eval=True,
                        num_boost_round=300,
                        categorical_features=categorical)

# bst = xgb_modelfit_nocv(params_xgb,
#                         train_df,
#                         val_df,
#                         predictors,
#                         target,
#                         objective='binary',
#                         metrics='auc',
#                         num_boost_round=300,
#                         early_stopping_rounds=50)

del train_df
del val_df
gc.collect()

print("Save model.txt")
bst.save_model("./model.txt")


print("Prepare online serving")

print("Deploy sql")
connection.execute("SET @@execute_mode='online';")
connection.execute("USE {}".format(db_name))
connection.execute("DEPLOY demo " + sql_part)
print("Import data to online")
# online feature extraction needs history data
# set job_timeout bigger if the `LOAD DATA` job timeout
connection.execute("LOAD DATA INFILE 'file://{}' INTO TABLE {}.{} OPTIONS(mode='append',format='csv',header=true);".format(
    os.path.abspath("train_sample.csv"), db_name, table_name))
