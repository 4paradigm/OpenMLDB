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
        now = int(time.time())
        jobHelper.append(jobHelper.rtidbClient.create_table, ttl=10)
        jobHelper.append(jobHelper.rtidbClient.put, time=now - 25)
        jobHelper.append(jobHelper.rtidbClient.put, time=now - 20)
        jobHelper.append(jobHelper.rtidbClient.put, time=now - 5)
        jobHelper.append(jobHelper.rtidbClient.put, time=now)
        jobHelper.append(jobHelper.rtidbClient.put, time=now + 5)
        jobHelper.append(jobHelper.rtidbClient.scan,
                         stime=now + 5,
                         etime=now - 25)
        jobHelper.run(autoidentity=False)
        retStatus, retMsg = jobHelper.identify(jobHelper.input_message(),
                                               jobHelper.scanout_message(),
                                               inputJunkFunc=lambda x: now - 25 <= x['time'] <= now + 5)
        self.assertTrue(retStatus, retMsg)

