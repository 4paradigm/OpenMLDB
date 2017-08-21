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
