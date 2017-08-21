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
