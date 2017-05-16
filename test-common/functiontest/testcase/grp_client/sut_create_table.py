# -*- coding: utf-8 -*-
from framework.test_suite import TestSuite
from job_helper import JobHelper, RtidbClient

class CreateTable(TestSuite):
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

    def testCreateTableCommon(self):
        """

        create_table操作，正确处理
        """
        jobHelper = JobHelper()
        jobHelper.append(jobHelper.rtidbClient.create_table)
        jobHelper.append(jobHelper.rtidbClient.put)
        jobHelper.append(jobHelper.rtidbClient.scan)
        retStatus = jobHelper.run()
        self.assertTrue(retStatus)

    def testCreateTableDuplicate(self):
        """

        已经存在的table名，再次执行create操作失败
        """
        jobHelper = JobHelper()
        jobHelper.append(jobHelper.rtidbClient.create_table, name='table_duplacate')
        jobHelper.append(jobHelper.rtidbClient.create_table, name='table_duplacate')
        retStatus = jobHelper.run(failonerror=False)
        self.assertFalse(retStatus)
