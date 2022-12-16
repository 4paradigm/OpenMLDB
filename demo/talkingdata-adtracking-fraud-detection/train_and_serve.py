"""Module of training and request to predict server"""
import gc
import os
import time
import glob
# fmt:off
# pylint: disable=unused-import
import openmldb
import sqlalchemy as db
import pandas as pd
import xgboost as xgb
from xgboost.sklearn import XGBClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
from sklearn.metrics import accuracy_score
import requests

# fmt:on

# openmldb cluster configs
ZK = '127.0.0.1:2181'
ZK_PATH = '/openmldb'

# db, deploy name and model_path will update to predict server. You only need to modify here.
DB_NAME = 'demo_db'
DEPLOY_NAME = 'demo'
# save model to
MODEL_PATH = '/tmp/model.json'

TABLE_NAME = 'talkingdata' + str(int(time.time()))
# make sure that taskmanager can access the path
TRAIN_FEATURE_DIR = '/tmp/train_feature'

PREDICT_SERVER = 'localhost:8881'


def column_string(col_tuple) -> str:
    """convert to str, used by CREATE TABLE DDL"""
    return ' '.join(col_tuple)


# NOTE: ignore column 'attributed_time'
train_schema = [('ip', 'int'), ('app', 'int'), ('device', 'int'),
                ('os', 'int'), ('channel', 'int'), ('click_time', 'timestamp'),
                ('is_attributed', 'int')]


def cut_data():
    """prepare sample data, use train_schema, not the origin schema"""
    data_path = 'data/'
    sample_cnt = 10000  # you can prepare sample data by yourself

    print(
        f'Prepare train data, use {sample_cnt} rows, save it as train_sample.csv')
    df = pd.read_csv(data_path + 'train.csv',
                     usecols=[c[0] for c in train_schema])

    # take a portion from train sample data
    df_tmp = df.sample(n=sample_cnt)
    assert len(df_tmp) == sample_cnt
    attr_count = df_tmp.is_attributed.value_counts()
    print(attr_count)
    # 'is_attributed' must have two values: 0, 1
    assert attr_count.count() > 1
    df_tmp.to_csv('train_sample.csv', index=False)
    del df_tmp
    del df
    gc.collect()


def nothrow_execute(sql):
    """only used for drop deployment, cuz 'if not exist' is not supported now"""
    try:
        print('execute ' + sql)
        _, rs = connection.execute(sql)
        print(rs)
        # pylint: disable=broad-except
    except Exception as e:
        print(e)


# skip preparing sample data
# cut_data()


print(f'Prepare openmldb, db {DB_NAME} table {TABLE_NAME}')
engine = db.create_engine(
    f'openmldb:///?zk={ZK}&zkPath={ZK_PATH}')
connection = engine.connect()

connection.execute(f'CREATE DATABASE IF NOT EXISTS {DB_NAME};')
connection.execute(f'USE {DB_NAME};')
schema_string = ','.join(list(map(column_string, train_schema)))
connection.execute(
    f'CREATE TABLE IF NOT EXISTS {TABLE_NAME}({schema_string});')

# use soft copy after 9391eaab8f released
print(f'Load train_sample data {os.path.abspath("train_sample.csv")} to offline storage for training(hard copy)')
connection.execute(f'USE {DB_NAME}')
connection.execute("SET @@execute_mode='offline';")
# use sync offline job, to make sure `LOAD DATA` finished
connection.execute('SET @@sync_job=true;')
connection.execute('SET @@job_timeout=1200000;')
connection.execute(f"LOAD DATA INFILE 'file://{os.path.abspath('train_sample.csv')}' "
                   f"INTO TABLE {TABLE_NAME} OPTIONS(format='csv',header=true, deep_copy=true);")

print('Feature extraction')
# the first column `is_attributed` is the label
sql_part = f"""
select is_attributed, app, device, os, channel, hour(click_time) as hour, day(click_time) as day, 
count(channel) over w1 as qty, 
count(channel) over w2 as ip_app_count, 
count(channel) over w3 as ip_app_os_count  
from {TABLE_NAME} 
window 
w1 as (partition by ip order by click_time ROWS_RANGE BETWEEN 1h PRECEDING AND CURRENT ROW), 
w2 as(partition by ip, app order by click_time ROWS_RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
w3 as(partition by ip, app, os order by click_time ROWS_RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
"""
# extraction will take time
connection.execute(
    f"{sql_part} INTO OUTFILE '{TRAIN_FEATURE_DIR}' OPTIONS(mode='overwrite');")

print(f'Load features from feature dir {TRAIN_FEATURE_DIR}')
# train_feature_dir has multi csv files
# all int, so no need to set read types
train_df = pd.concat(map(pd.read_csv, glob.glob(
    os.path.join('', TRAIN_FEATURE_DIR + '/*.csv'))))

# drop column label
X_data = train_df.drop('is_attributed', axis=1)
y = train_df.is_attributed

# Split the dataset into train and Test
SEED = 7
TEST_SIZE = 0.25
X_train, X_test, y_train, y_test = train_test_split(
    X_data, y, test_size=TEST_SIZE, random_state=SEED)

gc.collect()

print('Training by xgb')

# default is binary:logistic
train_model = XGBClassifier(use_label_encoder=False).fit(X_train, y_train)
pred = train_model.predict(X_test)
print('Classification report:\n', classification_report(y_test, pred))
print(f'Accuracy score: {accuracy_score(y_test, pred) * 100}')

del train_df
gc.collect()

print('Save model to ', MODEL_PATH)
train_model.save_model(MODEL_PATH)

print('Prepare online serving')

print('Deploy sql')
connection.execute("SET @@execute_mode='online';")
connection.execute(f'USE {DB_NAME}')
nothrow_execute(f'DROP DEPLOYMENT {DEPLOY_NAME}')
deploy_sql = f"""DEPLOY {DEPLOY_NAME} {sql_part}"""
print(deploy_sql)
connection.execute(deploy_sql)
print('Import data to online')
# online feature extraction needs history data
# set job_timeout bigger if the `LOAD DATA` job timeout
connection.execute(
    f"LOAD DATA INFILE 'file://{os.path.abspath('train_sample.csv')}' "
    f"INTO TABLE {DB_NAME}.{TABLE_NAME} OPTIONS(mode='append',format='csv',header=true);")

print('Update model to predict server')
infos = {'database': DB_NAME,
         'deployment': DEPLOY_NAME, 'model_path': MODEL_PATH}
requests.post('http://' + PREDICT_SERVER + '/update', json=infos)
