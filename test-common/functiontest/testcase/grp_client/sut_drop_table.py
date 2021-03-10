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

# -*- coding: utf-8 -*-
from framework.test_suite import TestSuite
from job_helper import JobHelper, RtidbClient

class DropTable(TestSuite):
    """
    create_table 操作
    """
    def setUp(self):
        """

        :return:
        """
        pass

    def tearDown(self):
        """

        :return:
        """
        pass

    def testDropTableCommon(self):
        """

        drop操作，正确处理
        """
        jobHelper = JobHelper()
        jobHelper.append(jobHelper.rtidbClient.create_table)
        jobHelper.append(jobHelper.rtidbClient.put)
        jobHelper.append(jobHelper.rtidbClient.put)
        jobHelper.append(jobHelper.rtidbClient.scan)
        jobHelper.append(jobHelper.rtidbClient.drop_table)
        retStatus = jobHelper.run()
        self.assertTrue(retStatus)

    def testDropTableTidInvalid(self):
        """

        不存在的table_id进行drop
        """
        jobHelper = JobHelper()
        jobHelper.append(jobHelper.rtidbClient.create_table, tid=123)
        jobHelper.append(jobHelper.rtidbClient.scan, tid=123)
        jobHelper.append(jobHelper.rtidbClient.drop_table, tid=123456)
        retStatus = jobHelper.run(failonerror=False, autoidentity=False)
        self.assertFalse(retStatus)

    def testDropTableRedo(self):
        """

        已经drop的表再次执行drop
        """
        jobHelper = JobHelper()
        jobHelper.append(jobHelper.rtidbClient.create_table)
        jobHelper.append(jobHelper.rtidbClient.drop_table)
        jobHelper.append(jobHelper.rtidbClient.drop_table)
        retStatus = jobHelper.run(failonerror=False)
        self.assertFalse(retStatus)

    def testDropTableLoop(self):
        """

        相同名称已经id的表，连续执行create -> drop -> create -> drop
        """
        jobHelper = JobHelper()
        jobHelper.append(jobHelper.rtidbClient.create_table)
        jobHelper.append(jobHelper.rtidbClient.drop_table)
        jobHelper.append(jobHelper.rtidbClient.create_table)
        jobHelper.append(jobHelper.rtidbClient.drop_table)
        jobHelper.append(jobHelper.rtidbClient.create_table)
        jobHelper.append(jobHelper.rtidbClient.drop_table)
        retStatus = jobHelper.run(failonerror=False)
        self.assertTrue(retStatus)
