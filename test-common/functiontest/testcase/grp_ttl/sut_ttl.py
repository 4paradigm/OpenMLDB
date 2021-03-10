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
import time
from framework.test_suite import TestSuite
from job_helper import JobHelper, RtidbClient

class Ttl(TestSuite):
    """
    scan 操作
    """
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testTtlCommon(self):
        """

        配置ttl时间，put 10条时序数据，sleep直到t0-t4过期，scan结果
        """
        jobHelper = JobHelper()
        put_time = long(time.time() * 1000)
        jobHelper.append(jobHelper.rtidbClient.create_table, ttl=10)
        jobHelper.append(jobHelper.rtidbClient.put, time=put_time)
        jobHelper.append(jobHelper.rtidbClient.put, time=put_time - 20l)
        jobHelper.append(jobHelper.rtidbClient.put, time=put_time - 11l * 60 * 1000)
        jobHelper.append(jobHelper.rtidbClient.scan,
                         stime=put_time,
                         etime=put_time - 12l * 60 * 1000)
        jobHelper.run(autoidentity=False)
        retStatus, retMsg = jobHelper.identify(jobHelper.input_message(),
                                               jobHelper.scanout_message(),
                                               inputJunkFunc=lambda x:  x['time'] > put_time - 11l * 60 * 1000)
        self.assertTrue(retStatus, retMsg)

