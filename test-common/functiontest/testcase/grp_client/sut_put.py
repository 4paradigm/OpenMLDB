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

class Put(TestSuite):
    """
    put 操作
    """
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testPutInvalidTid(self):
        """

        不存在的table_id执行put
        """
        jobHelper = JobHelper()

        jobHelper.append(jobHelper.rtidbClient.create_table, tid=123)
        jobHelper.append(jobHelper.rtidbClient.put, tid=456)
        jobHelper.append(jobHelper.rtidbClient.scan, tid=123)
        retStatus = jobHelper.run(failonerror=False, autoidentity=False)
        self.assertFalse(retStatus)
        self.assertEqual(0, len(jobHelper.scanout_message()))

    def testPutDropedTable(self):
        """

        已经drop的表执行put
        """
        jobHelper = JobHelper()

        jobHelper.append(jobHelper.rtidbClient.create_table)
        jobHelper.append(jobHelper.rtidbClient.drop_table)
        jobHelper.append(jobHelper.rtidbClient.put)
        jobHelper.append(jobHelper.rtidbClient.scan)
        retStatus = jobHelper.run(failonerror=False, autoidentity=False)
        self.assertFalse(retStatus)

    def testPutTidEmpty(self):
        """

        table_id为空
        """
        jobHelper = JobHelper()

        jobHelper.append(jobHelper.rtidbClient.create_table)
        jobHelper.append(jobHelper.rtidbClient.put, tid='')
        jobHelper.append(jobHelper.rtidbClient.scan)
        retStatus = jobHelper.run(failonerror=False, autoidentity=False)
        self.assertFalse(retStatus)
        self.assertEqual(0, len(jobHelper.scanout_message()))

    def testPutTid0(self):
        """

        table_id=0
        """
        jobHelper = JobHelper()

        jobHelper.append(jobHelper.rtidbClient.create_table, tid=0)
        jobHelper.append(jobHelper.rtidbClient.put, tid=0)
        jobHelper.append(jobHelper.rtidbClient.scan)
        retStatus = jobHelper.run(failonerror=False, autoidentity=False)
        self.assertFalse(retStatus)

    def testPutPkEmpty(self):
        """

        partition_key为空
        """
        jobHelper = JobHelper()

        jobHelper.append(jobHelper.rtidbClient.create_table)
        jobHelper.append(jobHelper.rtidbClient.put, pk='')
        jobHelper.append(jobHelper.rtidbClient.scan)
        retStatus = jobHelper.run(autoidentity=False)
        self.assertTrue(retStatus)
        self.assertEqual(0, len(jobHelper.scanout_message()))

    def testPutTimeStampInvalid(self):
        """

        timestamp为空
        """
        jobHelper = JobHelper()

        jobHelper.append(jobHelper.rtidbClient.create_table)
        jobHelper.append(jobHelper.rtidbClient.put, time=0)
        jobHelper.append(jobHelper.rtidbClient.scan)
        retStatus = jobHelper.run(failonerror=False, autoidentity=False)
        self.assertTrue(retStatus)
        self.assertEqual(0, len(jobHelper.scanout_message()))

    def testPutValueInit(self):
        """

        value是int数据
        """
        jobHelper = JobHelper()

        jobHelper.append(jobHelper.rtidbClient.create_table)
        jobHelper.append(jobHelper.rtidbClient.put, value=123)
        jobHelper.append(jobHelper.rtidbClient.scan)
        retStatus = jobHelper.run()
        self.assertTrue(retStatus)

    def testPutValueFloat(self):
        """

        value是float数据
        """
        jobHelper = JobHelper()

        jobHelper.append(jobHelper.rtidbClient.create_table)
        jobHelper.append(jobHelper.rtidbClient.put, value=1.23)
        jobHelper.append(jobHelper.rtidbClient.scan)
        retStatus = jobHelper.run(failonerror=False, autoidentity=False)
        self.assertTrue(retStatus)
        self.assertEqual(1, len(jobHelper.scanout_message()))

    def testPutValueBool(self):
        """

        value是Bool
        """
        jobHelper = JobHelper()

        jobHelper.append(jobHelper.rtidbClient.create_table)
        jobHelper.append(jobHelper.rtidbClient.put, value=True)
        jobHelper.append(jobHelper.rtidbClient.scan)
        retStatus = jobHelper.run(failonerror=False, autoidentity=False)
        self.assertTrue(retStatus)
        self.assertEqual(1, len(jobHelper.scanout_message()))

    def testPutValueBig(self):
        """

        value是1M string
        """
        jobHelper = JobHelper()

        jobHelper.append(jobHelper.rtidbClient.create_table)
        jobHelper.append(jobHelper.rtidbClient.put, value='a' * 1024 * 1025)
        jobHelper.append(jobHelper.rtidbClient.scan)
        retStatus = jobHelper.run()
        self.assertTrue(retStatus)

    def testPutValueEncode(self):
        """

        value是特殊编码数据
        """
        jobHelper = JobHelper()

        jobHelper.append(jobHelper.rtidbClient.create_table)
        jobHelper.append(jobHelper.rtidbClient.put, value='ボールト')
        jobHelper.append(jobHelper.rtidbClient.scan)
        retStatus = jobHelper.run()
        self.assertTrue(retStatus)

    def testPutValueEmpty(self):
        """

        value是空
        """
        jobHelper = JobHelper()

        jobHelper.append(jobHelper.rtidbClient.create_table)
        jobHelper.append(jobHelper.rtidbClient.put, value='')
        jobHelper.append(jobHelper.rtidbClient.scan)
        retStatus = jobHelper.run()
        self.assertTrue(retStatus)

    def testPutValueNone(self):
        """

        value是空
        """
        jobHelper = JobHelper()

        jobHelper.append(jobHelper.rtidbClient.create_table)
        jobHelper.append(jobHelper.rtidbClient.put, value=None)
        jobHelper.append(jobHelper.rtidbClient.scan)
        retStatus = jobHelper.run(failonerror=False, autoidentity=False)
        self.assertTrue(retStatus)
        self.assertEqual(1, len(jobHelper.scanout_message()))
