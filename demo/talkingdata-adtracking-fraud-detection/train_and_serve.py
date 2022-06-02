"""Module of docstring"""
import gc
import os
import time
import glob
import requests
# fmt:off
import sqlalchemy as db
import pandas as pd
import xgboost as xgb

# fmt:on

# openmldb cluster configs
zk = '127.0.0.1:2181'
zk_path = '/openmldb'

# db, deploy name and model_path will update to predict server. You only need to modify here.
db_name = 'demo_db'
deploy_name = 'demo'
# save model to
model_path = '/tmp/model.json'

table_name = 'talkingdata' + str(int(time.time()))
# make sure that taskmanager can access the path
train_feature_dir = '/tmp/train_feature'

predict_server = 'localhost:8881'


def column_string(col_tuple) -> str:
    return ' '.join(col_tuple)


def xgb_modelfit_nocv(params, dtrain, dvalid, objective='binary:logistic', metrics='auc',
                      feval=None, num_boost_round=3000, early_stopping_rounds=20):
    xgb_params = {
        'booster': 'gbtree',
        'obj': objective,
        'eval_metric': metrics,
        'num_leaves': 31,  # we should let it be smaller than 2^(max_depth)
        'max_depth': -1,  # -1 means no limit
        'max_bin': 255,  # Number of bucketed bin for feature values
        'subsample': 0.6,  # Subsample ratio of the training instance.
        'colsample_bytree': 0.3,
        'min_child_weight': 5,
        'alpha': 0,  # L1 regularization term on weights
        'lambda': 0,  # L2 regularization term on weights
        'nthread': 8,
        'verbosity': 0,
    }
    xgb_params.update(params)

    print('preparing validation datasets')

    evals_results = {}

    bst1 = xgb.train(xgb_params,
                     dtrain,
                     evals=dvalid,
                     evals_result=evals_results,
                     num_boost_round=num_boost_round,
                     early_stopping_rounds=early_stopping_rounds,
                     verbose_eval=10,
                     feval=feval)

    n_estimators = bst1.best_iteration
    print('\nModel Report')
    print('n_estimators : ', n_estimators)
    print(metrics + ':', evals_results['eval'][metrics][n_estimators - 1])

    return bst1


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

train_schema = [('ip', 'int'), ('app', 'int'), ('device', 'int'),
                ('os', 'int'), ('channel', 'int'), ('click_time', 'timestamp'), ('is_attributed', 'int')]


def cut_data():
    data_path = 'data/'
    sample_cnt = 10000  # you can prepare sample data by yourself
    print(f'Prepare train data, use {sample_cnt} rows, save it as train_sample.csv')
    train_df_tmp = pd.read_csv(data_path + 'train.csv', nrows=sample_cnt,
                               dtype=dtypes, usecols=[c[0] for c in train_schema])
    assert len(train_df_tmp) == sample_cnt
    # take a portion from train sample data
    train_df_tmp.to_csv('train_sample.csv', index=False)
    del train_df_tmp
    gc.collect()


def nothrow_execute(sql):
    # only used for drop deployment, cuz 'if not exist' is not supported now
    try:
        print('execute ' + sql)
        _, rs = connection.execute(sql)
        print(rs)
        # pylint: disable=broad-except
    except Exception as e:
        print(e)


print(f'Prepare openmldb, db {db_name} table {table_name}')
# cut_data()
engine = db.create_engine(
    f'openmldb:///{db_name}?zk={zk}&zkPath={zk_path}')
connection = engine.connect()

connection.execute(f'CREATE DATABASE IF NOT EXISTS {db_name};')
schema_string = ','.join(list(map(column_string, train_schema)))
connection.execute(f'CREATE TABLE IF NOT EXISTS {table_name}({schema_string});')

print('Load train_sample data to offline storage for training(hard copy)')
connection.execute(f'USE {db_name}')
connection.execute("SET @@execute_mode='offline';")
# use sync offline job, to make sure `LOAD DATA` finished
connection.execute('SET @@sync_job=true;')
connection.execute('SET @@job_timeout=1200000;')
# use soft link after https://github.com/4paradigm/OpenMLDB/issues/1565 fixed
connection.execute(f"LOAD DATA INFILE 'file://{os.path.abspath('train_sample.csv')}' "
                   f"INTO TABLE {table_name} OPTIONS(format='csv',header=true);")

print('Feature extraction')
# the first column `is_attributed` is the label
sql_part = f"""
select is_attributed, ip, app, device, os, channel, hour(click_time) as hour, day(click_time) as day, 
count(channel) over w1 as qty, 
count(channel) over w2 as ip_app_count, 
count(channel) over w3 as ip_app_os_count  
from {table_name} 
window 
w1 as (partition by ip order by click_time ROWS_RANGE BETWEEN 1h PRECEDING AND CURRENT ROW), 
w2 as(partition by ip, app order by click_time ROWS_RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
w3 as(partition by ip, app, os order by click_time ROWS_RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
"""
# extraction will take time
connection.execute('SET @@job_timeout=1200000;')
connection.execute(f"{sql_part} INTO OUTFILE '{train_feature_dir}' OPTIONS(mode='overwrite');")

print(f'Load features from feature dir {train_feature_dir}')
# train_feature_dir has multi csv files
train_df = pd.concat(map(pd.read_csv, glob.glob(os.path.join('', train_feature_dir + '/*.csv'))))
print('peek:')
print(train_df.head())
len_train = len(train_df)
train_row_cnt = int(len_train * 3 / 4)
train_df = train_df[(len_train - train_row_cnt):len_train]
val_df = train_df[:(len_train - train_row_cnt)]

print('train size: ', len(train_df))
print('valid size: ', len(val_df))

target = 'is_attributed'
predictors = ['app', 'device', 'os', 'channel', 'hour',
              'day', 'qty', 'ip_app_count', 'ip_app_os_count']

gc.collect()

print('Training by xgb')
params_xgb = {
    'num_leaves': 7,  # we should let it be smaller than 2^(max_depth)
    'max_depth': 3,  # -1 means no limit
    'min_child_samples': 100,
    'max_bin': 100,  # Number of bucketed bin for feature values
    'subsample': 0.7,  # Subsample ratio of the training instance.
    # Subsample ratio of columns when constructing each tree.
    'colsample_bytree': 0.7,
    # Minimum sum of instance weight(hessian) needed in a child(leaf)
    'min_child_weight': 0
}
xgtrain = xgb.DMatrix(train_df[predictors].values,
                      label=train_df[target].values)
xgvalid = xgb.DMatrix(val_df[predictors].values, label=val_df[target].values)
watchlist = [(xgvalid, 'eval'), (xgtrain, 'train')]

bst = xgb_modelfit_nocv(params_xgb,
                        xgtrain,
                        watchlist,
                        objective='binary:logistic',
                        metrics='auc',
                        num_boost_round=300,
                        early_stopping_rounds=50)

del train_df
del val_df
gc.collect()

print('Save model.json to ', model_path)
bst.save_model(model_path)

print('Prepare online serving')

print('Deploy sql')
connection.execute("SET @@execute_mode='online';")
connection.execute(f'USE {db_name}')
nothrow_execute(f'DROP DEPLOYMENT {deploy_name}')
deploy_sql = f"""DEPLOY {deploy_name} {sql_part}"""
print(deploy_sql)
connection.execute(deploy_sql)
print('Import data to online')
# online feature extraction needs history data
# set job_timeout bigger if the `LOAD DATA` job timeout
connection.execute(
    f"LOAD DATA INFILE 'file://{os.path.abspath('train_sample.csv')}' "
    f"INTO TABLE {db_name}.{table_name} OPTIONS(mode='append',format='csv',header=true);")

print('Update model to predict server')
infos = {'database': db_name, 'deployment': deploy_name, 'model_path': model_path}
requests.post('http://' + predict_server + '/update', json=infos)
