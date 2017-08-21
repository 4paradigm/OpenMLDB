# -*- coding: utf-8 -*-
from framework.test_suite import TestSuite
from job_helper import JobHelper, RtidbClient

class Scan(TestSuite):
    """
    scan 操作
    """
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testScanCommon(self):
        """

        scan操作，正确处理
        """
        jobHelper = JobHelper()
        jobHelper.append(jobHelper.rtidbClient.create_table)
        jobHelper.append(jobHelper.rtidbClient.put)
        jobHelper.append(jobHelper.rtidbClient.scan)
        retStatus = jobHelper.run()
        self.assertTrue(retStatus)

    def testScanMulti(self):
        """

        scan操作，命中多条stime,etime内数据
        """
        jobHelper = JobHelper()

        jobHelper.append(jobHelper.rtidbClient.create_table)
        jobHelper.append(jobHelper.rtidbClient.put, time=1494496520)
        jobHelper.append(jobHelper.rtidbClient.put, time=1494496521)
        jobHelper.append(jobHelper.rtidbClient.put, time=1494496522)
        jobHelper.append(jobHelper.rtidbClient.put, time=1494496523)
        jobHelper.append(jobHelper.rtidbClient.put, time=1494496524)
        jobHelper.append(jobHelper.rtidbClient.put, time=1494496525)
        jobHelper.append(jobHelper.rtidbClient.put, time=1494496526)
        jobHelper.append(jobHelper.rtidbClient.scan,
                         stime=1494496525,
                         etime=1494496521)
        jobHelper.run(autoidentity=False)
        retStatus, retMsg = jobHelper.identify(jobHelper.input_message(),
                                               jobHelper.scanout_message(),
                                               inputJunkFunc=lambda x: 1494496521 < x['time'] <= 1494496525)
        self.assertTrue(retStatus, retMsg)

    def testScanTidInvalid(self):
        """

        table_id不存在的表进行scan
        """
        jobHelper = JobHelper()
        jobHelper.append(jobHelper.rtidbClient.create_table, tid=123)
        jobHelper.append(jobHelper.rtidbClient.put, tid=123)
        jobHelper.append(jobHelper.rtidbClient.scan, tid=456)
        retStatus = jobHelper.run(autoidentity=False, logcheck=False)
        self.assertTrue(retStatus)
        self.assertEqual(0, len(jobHelper.scanout_message()))

    def testScanDroped(self):
        """

        已经drop的表进行scan
        """
        jobHelper = JobHelper()
        jobHelper.append(jobHelper.rtidbClient.create_table)
        jobHelper.append(jobHelper.rtidbClient.put)
        jobHelper.append(jobHelper.rtidbClient.drop_table)
        jobHelper.append(jobHelper.rtidbClient.scan)
        retStatus = jobHelper.run(failonerror=False, autoidentity=False, logcheck=False)
        self.assertTrue(retStatus)
        self.assertEqual(0, len(jobHelper.scanout_message()))

    def testScanPkInvalid(self):
        """

        不存在的partition_key进行scan
        """
        jobHelper = JobHelper()
        jobHelper.append(jobHelper.rtidbClient.create_table)
        jobHelper.append(jobHelper.rtidbClient.put, pk='123')
        jobHelper.append(jobHelper.rtidbClient.scan, pk='456')
        retStatus = jobHelper.run(failonerror=False, autoidentity=False)
        self.assertTrue(retStatus)
        self.assertEqual(0, len(jobHelper.scanout_message()))

    def testScanStimeEmpty(self):
        """

        timestamp_start为空
        """
        jobHelper = JobHelper()
        jobHelper.append(jobHelper.rtidbClient.create_table)
        jobHelper.append(jobHelper.rtidbClient.put)
        jobHelper.append(jobHelper.rtidbClient.scan, stime=0)
        retStatus = jobHelper.run(failonerror=False, autoidentity=False)
        self.assertTrue(retStatus)
        self.assertEqual(0, len(jobHelper.scanout_message()))

    def testScanEtimeEmpty(self):
        """

        timestamp_end为空
        """
        jobHelper = JobHelper()
        jobHelper.append(jobHelper.rtidbClient.create_table)
        jobHelper.append(jobHelper.rtidbClient.put)
        jobHelper.append(jobHelper.rtidbClient.scan, etime=0)
        retStatus = jobHelper.run(failonerror=False, autoidentity=False)
        self.assertTrue(retStatus)
        self.assertEqual(1, len(jobHelper.scanout_message()))

    def testScanTimeEqual(self):
        """

        timestamp_start = timestamp_end
        """
        jobHelper = JobHelper()

        jobHelper.append(jobHelper.rtidbClient.create_table)
        jobHelper.append(jobHelper.rtidbClient.put, time=1494496520)
        jobHelper.append(jobHelper.rtidbClient.put, time=1494496521)
        jobHelper.append(jobHelper.rtidbClient.put, time=1494496522)
        jobHelper.append(jobHelper.rtidbClient.put, time=1494496523)
        jobHelper.append(jobHelper.rtidbClient.scan,
                         stime=1494496521,
                         etime=1494496521)
        jobHelper.run(autoidentity=False)
        retStatus, retMsg = jobHelper.identify(jobHelper.input_message(),
                                               jobHelper.scanout_message(),
                                               inputJunkFunc=lambda x: x['time'] == 1494496521)
        self.assertFalse(retStatus, retMsg)

    def testScanTimeDisorderly(self):
        """

        timestamp_start < timestamp_end
        """
        jobHelper = JobHelper()

        jobHelper.append(jobHelper.rtidbClient.create_table)
        jobHelper.append(jobHelper.rtidbClient.put, time=1494496520)
        jobHelper.append(jobHelper.rtidbClient.put, time=1494496521)
        jobHelper.append(jobHelper.rtidbClient.put, time=1494496522)
        jobHelper.append(jobHelper.rtidbClient.put, time=1494496523)
        jobHelper.append(jobHelper.rtidbClient.scan,
                         stime=1494496521,
                         etime=1494496522)
        retStatus = jobHelper.run(autoidentity=False)
        self.assertTrue(retStatus)
        self.assertEqual(0, len(jobHelper.scanout_message()))

    def testScanTimeLongYear(self):
        """

        timestamp_start ，timestamp_end之间跨越100年
        """
        jobHelper = JobHelper()

        jobHelper.append(jobHelper.rtidbClient.create_table)
        jobHelper.append(jobHelper.rtidbClient.put, time=1494496520)
        jobHelper.append(jobHelper.rtidbClient.put, time=1494496521)
        jobHelper.append(jobHelper.rtidbClient.put, time=1494496522)
        jobHelper.append(jobHelper.rtidbClient.put, time=1494496523)
        jobHelper.append(jobHelper.rtidbClient.scan,
                         stime=1494496521 + 10 * 365 * 24 * 3600,
                         etime=1494496521)
        retStatus = jobHelper.run(autoidentity=False)
        self.assertTrue(retStatus)
        retStatus, retMsg = jobHelper.identify(jobHelper.input_message(),
                                               jobHelper.scanout_message(),
                                               inputJunkFunc=lambda x: x['time'] > 1494496521)
        self.assertTrue(retStatus, retMsg)

    def testScanMiss(self):
        """

        scan时间段内无数据
        """
        jobHelper = JobHelper()

        jobHelper.append(jobHelper.rtidbClient.create_table)
        jobHelper.append(jobHelper.rtidbClient.put, time=1494496520)
        jobHelper.append(jobHelper.rtidbClient.put, time=1494496521)
        jobHelper.append(jobHelper.rtidbClient.put, time=1494496522)
        jobHelper.append(jobHelper.rtidbClient.put, time=1494496523)
        jobHelper.append(jobHelper.rtidbClient.scan,
                         stime=1494496511,
                         etime=1494496512)
        retStatus = jobHelper.run(autoidentity=False)
        self.assertTrue(retStatus)
        self.assertEqual(0, len(jobHelper.scanout_message()))
