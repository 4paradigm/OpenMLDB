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
Unittest module to test Operators.

Requires the unittest, pytest, and requests-mock Python libraries.

Run test:

    python3 -m unittest tests.operators.test_openmldb_operator.TestOpenMLDBOperator

"""

import logging
import unittest
from unittest import mock, skip

import pytest
import requests

from openmldb_provider.hooks.openmldb_hook import OpenMLDBHook
from openmldb_provider.operators.openmldb_operator import (OpenMLDBSQLOperator, Mode, OpenMLDBDeployOperator,
                                                           OpenMLDBSelectIntoOperator, OpenMLDBLoadDataOperator)

log = logging.getLogger(__name__)

MOCK_TASK_ID = "test-openmldb-operator"
MOCK_DB = "mock_db"
MOCK_TABLE = "mock_table"
MOCK_FILE = "mock_file_name"
MOCK_OPENMLDB_CONN_ID = "mock_openmldb_conn"


@mock.patch.dict('os.environ', AIRFLOW_CONN_MOCK_OPENMLDB_CONN='http://http%3A%2F%2F1.2.3.4%3A9080%2F')
class TestOpenMLDBLoadDataOperator:
    @mock.patch.object(OpenMLDBHook, "submit_job")
    def test_execute(self, mock_submit_job):
        operator = OpenMLDBLoadDataOperator(
            task_id=MOCK_TASK_ID,
            openmldb_conn_id=MOCK_OPENMLDB_CONN_ID,
            db=MOCK_DB,
            mode=Mode.OFFSYNC,
            table=MOCK_TABLE,
            file=MOCK_FILE,
            disable_response_check=True,
        )
        operator.execute({})

        mock_submit_job.assert_called_once_with(
            db=MOCK_DB,
            mode=Mode.OFFSYNC.value,
            sql=f"LOAD DATA INFILE '{MOCK_FILE}' INTO " f"TABLE {MOCK_TABLE}",
        )

    @mock.patch.object(OpenMLDBHook, "submit_job")
    def test_execute_with_options(self, mock_submit_job):
        response = requests.Response()
        response.status_code = 200
        response._content = b'{"code": 0, "msg": "ok"}'
        mock_submit_job.return_value = response

        options = "mode='overwrite'"
        operator = OpenMLDBLoadDataOperator(
            task_id=MOCK_TASK_ID,
            openmldb_conn_id=MOCK_OPENMLDB_CONN_ID,
            db=MOCK_DB,
            mode=Mode.OFFSYNC,
            table=MOCK_TABLE,
            file=MOCK_FILE,
            options=options,
        )
        operator.execute({})
        mock_submit_job.assert_called_once_with(
            db=MOCK_DB,
            mode=Mode.OFFSYNC.value,
            sql=f"LOAD DATA INFILE '{MOCK_FILE}' INTO " f"TABLE {MOCK_TABLE} OPTIONS" f"({options})",
        )


@mock.patch.dict('os.environ', AIRFLOW_CONN_MOCK_OPENMLDB_CONN='http://http%3A%2F%2F1.2.3.4%3A9080%2F')
class TestOpenMLDBSelectOutOperator:
    @mock.patch.object(OpenMLDBHook, "submit_job")
    def test_execute(self, mock_submit_job):
        fe_sql = (
            "SELECT id, ts, sum(c1) over w1 FROM t1 WINDOW w1 as "
            "(PARTITION BY id ORDER BY ts BETWEEN 20s PRECEDING AND CURRENT ROW)"
        )
        operator = OpenMLDBSelectIntoOperator(
            task_id=MOCK_TASK_ID,
            openmldb_conn_id=MOCK_OPENMLDB_CONN_ID,
            db=MOCK_DB,
            mode=Mode.OFFSYNC,
            sql=fe_sql,
            file=MOCK_FILE,
            disable_response_check=True,
        )
        operator.execute({})

        mock_submit_job.assert_called_once_with(
            db=MOCK_DB, mode=Mode.OFFSYNC.value, sql=f"{fe_sql} INTO OUTFILE '{MOCK_FILE}'"
        )

    @mock.patch.object(OpenMLDBHook, "submit_job")
    def test_execute_with_options(self, mock_submit_job):
        response = requests.Response()
        response.status_code = 200
        response._content = b'{"code": 0, "msg": "ok"}'
        mock_submit_job.return_value = response

        fe_sql = (
            "SELECT id, ts, sum(c1) over w1 FROM t1 WINDOW w1 as "
            "(PARTITION BY id ORDER BY ts BETWEEN 20s PRECEDING AND CURRENT ROW)"
        )
        options = "mode='errorifexists', delimiter='-'"
        operator = OpenMLDBSelectIntoOperator(
            task_id=MOCK_TASK_ID,
            openmldb_conn_id=MOCK_OPENMLDB_CONN_ID,
            db=MOCK_DB,
            mode=Mode.OFFSYNC,
            sql=fe_sql,
            file=MOCK_FILE,
            options=options,
            disable_response_check=True,
        )
        operator.execute({})

        mock_submit_job.assert_called_once_with(
            db=MOCK_DB,
            mode=Mode.OFFSYNC.value,
            sql=f"{fe_sql} INTO OUTFILE '{MOCK_FILE}' OPTIONS({options})",
        )


@mock.patch.dict('os.environ', AIRFLOW_CONN_MOCK_OPENMLDB_CONN='http://http%3A%2F%2F1.2.3.4%3A9080%2F')
class TestOpenMLDBDeployOperator:
    @mock.patch.object(OpenMLDBHook, "submit_job")
    def test_execute(self, mock_submit_job):
        fe_sql = (
            "SELECT id, ts, sum(c1) over w1 FROM t1 WINDOW w1 as "
            "(PARTITION BY id ORDER BY ts BETWEEN 20s PRECEDING AND CURRENT ROW)"
        )
        deploy_name = "demo"
        operator = OpenMLDBDeployOperator(
            task_id=MOCK_TASK_ID,
            openmldb_conn_id=MOCK_OPENMLDB_CONN_ID,
            db=MOCK_DB,
            deploy_name=deploy_name,
            sql=fe_sql,
            disable_response_check=True,
        )
        operator.execute({})

        mock_submit_job.assert_called_once_with(
            db=MOCK_DB, mode=Mode.ONLINE.value, sql=f"DEPLOY {deploy_name} {fe_sql}"
        )


@mock.patch.dict('os.environ', AIRFLOW_CONN_MOCK_OPENMLDB_CONN='http://http%3A%2F%2F1.2.3.4%3A9080%2F')
class TestOpenMLDBSQLOperator:
    @mock.patch.object(OpenMLDBHook, "submit_job")
    @pytest.mark.parametrize(
        "sql, mode",
        [
            ("create database if not exists test_db", Mode.OFFSYNC),
            ("SHOW JOBS", Mode.ONLINE),
            ("SELECT 1", Mode.OFFSYNC),
            ("SELECT 1", Mode.ONLINE),
        ],
    )
    def test_execute(self, mock_submit_job, sql, mode):
        operator = OpenMLDBSQLOperator(
            task_id=MOCK_TASK_ID,
            openmldb_conn_id=MOCK_OPENMLDB_CONN_ID,
            db=MOCK_DB,
            mode=mode,
            sql=sql,
            disable_response_check=True,
        )
        operator.execute({})

        mock_submit_job.assert_called_once_with(db=MOCK_DB, mode=mode.value, sql=sql)


@skip
@mock.patch.dict('os.environ', AIRFLOW_CONN_OPENMLDB_DEFAULT='http://http%3A%2F%2F127.0.0.1%3A9080%2F')
class TestOpenMLDBOperatorIT(unittest.TestCase):
    """
    Test OpenMLDB Operator.
    """

    def test_operator_with_empty_sql(self):
        operator = OpenMLDBSQLOperator(
            task_id='run_operator', db='foo', mode=Mode.ONLINE,
            sql='', response_check=lambda response: (response.json()['code'] == 2000) and (
                    'sql trees is null or empty' in response.json()['msg']))
        operator.execute({})

    def test_operator_with_sql(self):
        test_db = "airflow_test_db"
        test_table = "airflow_test_table"

        OpenMLDBSQLOperator(task_id='setup-database', db=test_db,
                            mode=Mode.OFFSYNC,
                            sql=f'create database if not exists {test_db}').execute({})
        OpenMLDBSQLOperator(task_id='setup-table', db=test_db,
                            mode=Mode.OFFSYNC,
                            sql=f'create table if not exists {test_table}(c1 int)').execute({})
        # TODO(hw): response doesn't have the result data now, so just do an offline query here.
        #  But you can check status.
        OpenMLDBSQLOperator(task_id='feature-extraction-offline', db=test_db,
                            mode=Mode.OFFSYNC,
                            sql=f'select * from {test_table}', ).execute({})
        # do an online query
        OpenMLDBSQLOperator(task_id='feature-extraction-online', db=test_db,
                            mode=Mode.ONLINE,
                            sql=f'select * from {test_table}').execute({})

        OpenMLDBSQLOperator(task_id='feature-extraction-online-bad', db=test_db,
                            mode=Mode.ONLINE,
                            sql='select * from not_exist_table',
                            response_check=lambda response: (response.json()['code'] == -1) and (
                                    "not exists" in response.json()['msg'])).execute({})
