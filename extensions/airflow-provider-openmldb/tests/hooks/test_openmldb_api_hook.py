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
Unittest module to test Hooks.

Requires the unittest, pytest, and requests-mock Python libraries.

Run test:

    python3 -m unittest tests.hooks.test_sample_hook.TestSampleHook

"""
import json
import logging
import unittest
from unittest import mock, skip

import requests_mock
from airflow.models import Connection
from airflow.utils import db

# Import Hook
from openmldb_provider.hooks.openmldb_hook import OpenMLDBHook

log = logging.getLogger(__name__)


class TestOpenMLDBHook(unittest.TestCase):
    openmldb_conn_id = 'openmldb_conn_id_test'
    test_db_endpoint = 'http://127.0.0.1:9080/dbs/test_db'

    _mock_job_status_success_response_body = {'code': 0, 'msg': 'ok'}

    def setUp(self):
        db.merge_conn(
            Connection(
                conn_id='openmldb_conn_id_test', conn_type='openmldb', host='http://127.0.0.1', port=9080
            )
        )
        self.hook = OpenMLDBHook(openmldb_conn_id=self.openmldb_conn_id)

    @requests_mock.mock()
    def test_submit_offsync_job(self, m):
        m.post(self.test_db_endpoint, status_code=200, json=self._mock_job_status_success_response_body)
        resp = self.hook.submit_job('test_db', 'offsync', 'select * from t1')
        assert resp.status_code == 200
        assert resp.json() == self._mock_job_status_success_response_body


@skip
# Mock the `conn_sample` Airflow connection
@mock.patch.dict('os.environ', AIRFLOW_CONN_CONN_SAMPLE='http://https%3A%2F%2Fwww.httpbin.org%2F')
@mock.patch.dict('os.environ',
                 AIRFLOW_CONN_OPENMLDB_DEFAULT='http://http%3A%2F%2F127.0.0.1%3A9080%2Fdbs%2Fairflow_test')
class TestOpenMLDBAPIHook(unittest.TestCase):
    """
    Test OpenMLDB API Hook.
    """

    @requests_mock.mock()
    def test_post(self, m):
        # Mock endpoint
        m.post('https://www.httpbin.org/', json={'data': 'mocked response'})

        # Instantiate hook
        hook = OpenMLDBHook(
            openmldb_conn_id='conn_sample',
            method='post'
        )

        # Sample Hook's run method executes an API call
        response = hook.run()

        # Retrieve response payload
        payload = response.json()

        # Assert success status code
        assert response.status_code == 200

        # Assert the API call returns expected mocked payload
        assert payload['data'] == 'mocked response'

    @requests_mock.mock()
    def test_get(self, m):
        # Mock endpoint
        m.get('https://www.httpbin.org/', json={'data': 'mocked response'})

        # Instantiate hook
        hook = OpenMLDBHook(
            openmldb_conn_id='conn_sample',
            method='get'
        )

        # Sample Hook's run method executes an API call
        response = hook.run()

        # Retrieve response payload
        payload = response.json()

        # Assert success status code
        assert response.status_code == 200

        # Assert the API call returns expected mocked payload
        assert payload['data'] == 'mocked response'

    def test_query_api_server_without_data(self):
        hook = OpenMLDBHook()
        # no data
        response = hook.run()
        res = json.loads(response.text)
        assert res == {'code': -1, 'msg': 'Request body json parse failed'}

    def test_query_api_server_with_sql(self):
        hook = OpenMLDBHook()
        response = hook.run(data='{"sql":"select 1", "mode":"offsync"}')
        res = json.loads(response.text)
        assert res == {'code': 0, 'msg': 'ok'}

    def test_query_api_server_without_mode(self):
        hook = OpenMLDBHook()
        response = hook.run(data='{"sql":"select 1"}')
        res = json.loads(response.text)
        assert res['code'] == -1
        assert res['msg'].startswith('Request body json parse failed')

    def test_query_api_server(self):
        hook = OpenMLDBHook()
        # We can send ddl by post too, but not recommended for users.
        # Here just do it for tests, mode won't affect
        response = hook.run(data='{"sql": "create database if not exists airflow_test", "mode": "online"}',
                            headers={"content-type": "application/json"})
        res = json.loads(response.text)
        assert res == {'code': 0, 'msg': 'ok'}

        response = hook.run(data='{"sql":"create table if not exists airflow_table(c1 int)", "mode":"online"}',
                            headers={"content-type": "application/json"})
        res = json.loads(response.text)
        assert res == {'code': 0, 'msg': 'ok'}

        # an offline sync query
        response = hook.run(data='{"sql":"select * from airflow_table", "mode":"offsync"}',
                            headers={"content-type": "application/json"})
        res = json.loads(response.text)
        assert res == {'code': 0, 'msg': 'ok'}

        # an online query(always sync)
        response = hook.run(data='{"sql":"select * from airflow_table", "mode":"online"}',
                            headers={"content-type": "application/json"})
        res = json.loads(response.text)
        assert res == {'code': 0, 'msg': 'ok'}


if __name__ == '__main__':
    unittest.main()
