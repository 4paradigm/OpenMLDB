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
        # retStatus = jobHelper.run(failonerror=False, autoidentity=False)
        # self.assertFalse(retStatus)
        self.assertFalse(True)
        self.assertEqual(0, len(jobHelper.scanout_message()))

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

        jobHelper.append(jobHelper.rtidbClient.create_table)
        jobHelper.append(jobHelper.rtidbClient.put, tid=0)
        jobHelper.append(jobHelper.rtidbClient.scan)
        retStatus = jobHelper.run(autoidentity=False)
        self.assertTrue(retStatus)
        self.assertEqual(0, len(jobHelper.scanout_message()))

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
        jobHelper.append(jobHelper.rtidbClient.put, time='')
        jobHelper.append(jobHelper.rtidbClient.scan)
        retStatus = jobHelper.run(failonerror=False, autoidentity=False)
        self.assertFalse(retStatus)
        self.assertEqual(0, len(jobHelper.scanout_message()))

    def testPutValueInit(self):
        """

        value是int数据
        """
        jobHelper = JobHelper()

        jobHelper.append(jobHelper.rtidbClient.create_table)
        jobHelper.append(jobHelper.rtidbClient.put, value=123)
        jobHelper.append(jobHelper.rtidbClient.scan)
        # retStatus = jobHelper.run()
        # self.assertTrue(retStatus)
        self.assertTrue(False)

    def testPutValueFloat(self):
        """

        value是float数据
        """
        jobHelper = JobHelper()

        jobHelper.append(jobHelper.rtidbClient.create_table)
        jobHelper.append(jobHelper.rtidbClient.put, value=1.23)
        jobHelper.append(jobHelper.rtidbClient.scan)
        retStatus = jobHelper.run(failonerror=False, autoidentity=False)
        self.assertFalse(retStatus)
        self.assertEqual(0, len(jobHelper.scanout_message()))

    def testPutValueBool(self):
        """

        value是Bool
        """
        jobHelper = JobHelper()

        jobHelper.append(jobHelper.rtidbClient.create_table)
        jobHelper.append(jobHelper.rtidbClient.put, value=True)
        jobHelper.append(jobHelper.rtidbClient.scan)
        # retStatus = jobHelper.run(failonerror=False, autoidentity=False)
        # self.assertFalse(retStatus)
        # self.assertEqual(0, len(jobHelper.scanout_message()))
        self.assertFalse(True)

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
        # retStatus = jobHelper.run(failonerror=False, autoidentity=False)
        # self.assertFalse(retStatus)
        # self.assertEqual(0, len(jobHelper.scanout_message()))
        self.assertFalse(True)
