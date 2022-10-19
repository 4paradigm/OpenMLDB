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

"""
Example use of OpenMLDB related operators.
"""
import os
from datetime import datetime

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from openmldb_provider.operators.openmldb_operator import (
    Mode,
    OpenMLDBLoadDataOperator,
    OpenMLDBSelectIntoOperator, OpenMLDBSQLOperator, OpenMLDBDeployOperator,
)

import xgboost_train_sample

# cp example_dags/train_sample.csv to /tmp first
PATH_TO_DATA_FILE = os.environ.get('OPENMLDB_PATH_TO_DATA_FILE', '/tmp/train_sample.csv')
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_openmldb_complex"

with DAG(
        dag_id=DAG_ID,
        start_date=datetime(2021, 1, 1),
        default_args={'openmldb_conn_id': 'openmldb_conn_id'},
        max_active_runs=1,
        tags=['example'],
        catchup=False,
) as dag:
    database = "example_db"
    table = "example_table"

    create_database = OpenMLDBSQLOperator(
        task_id='create-db',
        db=database, mode=Mode.OFFSYNC,
        sql=f'create database if not exists {database}'
    )

    create_table = OpenMLDBSQLOperator(
        task_id='create-table',
        db=database, mode=Mode.OFFSYNC,
        sql=f'create table if not exists {table}(ip int, app int, device int, os int, channel int, '
            f'click_time timestamp, is_attributed int)'
    )

    # [START load_data_and_extract_feature_offline]
    load_data = OpenMLDBLoadDataOperator(
        task_id='load-data',
        db=database,
        mode=Mode.OFFSYNC,
        table=table,
        file=PATH_TO_DATA_FILE,
        options="mode='overwrite'",
    )

    sql = f"SELECT is_attributed, app, device, os, channel, hour(click_time) as hour, day(click_time) as day, " \
          f"count(channel) over w1 as qty " \
          f"FROM {table} " \
          f"WINDOW " \
          f"w1 as(partition by ip order by click_time ROWS_RANGE BETWEEN 1h PRECEDING AND CURRENT ROW)"

    feature_path = "/tmp/feature_data"
    feature_extract = OpenMLDBSelectIntoOperator(
        task_id='feature-extract',
        db=database,
        mode=Mode.OFFSYNC,
        sql=sql,
        file=feature_path,
        options="mode='overwrite'",
    )
    # [END load_data_and_extract_feature_offline]

    model_path = "/tmp/model.json"
    # return auc
    train = PythonOperator(task_id="train",
                           python_callable=xgboost_train_sample.train_task,
                           op_args=[f"{feature_path}/*.csv", model_path], )


    def branch_func(**kwargs):
        ti = kwargs['ti']
        xcom_value = int(ti.xcom_pull(task_ids='train'))
        if xcom_value >= 99.0:
            return "deploy-sql"
        else:
            return "fail-report"


    branching = BranchPythonOperator(
        task_id="branching",
        python_callable=branch_func,
    )

    predict_server = "127.0.0.1:8881"
    deploy_name = "demo"

    # success: deploy sql and model
    deploy_sql = OpenMLDBDeployOperator(task_id="deploy-sql", db=database, deploy_name=deploy_name, sql=sql, )


    def update_req():
        import requests
        requests.post('http://' + predict_server + '/update', json={
            'database': database,
            'deployment': deploy_name, 'model_path': model_path
        })


    deploy = PythonOperator(task_id="deploy", python_callable=update_req)

    deploy_sql >> deploy

    # fail: report
    fail_report = PythonOperator(task_id="fail-report", python_callable=lambda: print('fail'))

    create_database >> create_table >> load_data >> feature_extract >> train >> branching >> [deploy_sql,
                                                                                              fail_report]
