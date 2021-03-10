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

class Sequence(TestSuite):
    """
    scan 操作
    """
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testSequenceSameTs(self):
        """

        put 多条相同时间戳数据
        """
        jobHelper = JobHelper()
        jobHelper.append(jobHelper.rtidbClient.create_table)
        jobHelper.append(jobHelper.rtidbClient.put, time=1494496520)
        jobHelper.append(jobHelper.rtidbClient.put, time=1494496521)
        jobHelper.append(jobHelper.rtidbClient.put, time=1494496521)
        jobHelper.append(jobHelper.rtidbClient.put, time=1494496521)
        jobHelper.append(jobHelper.rtidbClient.put, time=1494496521)
        jobHelper.append(jobHelper.rtidbClient.put, time=1494496525)
        jobHelper.append(jobHelper.rtidbClient.put, time=1494496526)
        jobHelper.append(jobHelper.rtidbClient.scan,
                         stime=1494496522,
                         etime=1494496520)
        jobHelper.run(autoidentity=False)
        self.assertEqual(4, len(jobHelper.scanout_message()))

    def testDisorderPut(self):
        """

        乱序put，scan结果时序正确
        """
        jobHelper = JobHelper()

        jobHelper.append(jobHelper.rtidbClient.create_table)
        jobHelper.append(jobHelper.rtidbClient.put, time=1494496522)
        jobHelper.append(jobHelper.rtidbClient.put, time=1494496520)
        jobHelper.append(jobHelper.rtidbClient.put, time=1494496521)
        jobHelper.append(jobHelper.rtidbClient.put, time=1494496525)
        jobHelper.append(jobHelper.rtidbClient.put, time=1494496526)
        jobHelper.append(jobHelper.rtidbClient.put, time=1494496524)
        jobHelper.append(jobHelper.rtidbClient.put, time=1494496523)
        jobHelper.append(jobHelper.rtidbClient.scan,
                         stime=1494496525,
                         etime=1494496521)
        jobHelper.run(autoidentity=False)
        retStatus, retMsg = jobHelper.identify(jobHelper.input_message(),
                                               jobHelper.scanout_message(),
                                               inputJunkFunc=lambda x: 1494496521 < x['time'] <= 1494496525)
        self.assertTrue(retStatus, retMsg)
