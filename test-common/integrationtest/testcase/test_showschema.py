# -*- coding: utf-8 -*-
from testcasebase import TestCaseBase
from libs.deco import multi_dimension
from libs.test_loader import load


class TestShowSchema(TestCaseBase):

    @multi_dimension(True)
    def test_showschema_tid_not_exist(self):
        """
        tid不存在，查看schema
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid, 144000, 2, 'true',
                          card='string:index', merchant='string:index', amt='double:index')
        self.assertTrue('Create table ok' in rs1)
        schema = self.run_client(self.leader, 'showschema {} {}'.format(self.tid, self.pid))
        schema_d = self.parse_sechema(schema)
        self.assertEqual(schema_d['card'], ['string', 'yes'])
        self.assertEqual(schema_d['merchant'], ['string', 'yes'])
        self.assertEqual(schema_d['amt'], ['double', 'yes'])
        schema = self.run_client(self.leader, 'showschema {} {}'.format(self.tid + 1, self.pid))
        self.assertTrue('No schema for table' in schema)
        schema = self.run_client(self.leader, 'showschema {} {}'.format(self.tid, self.pid + 1))
        self.assertTrue('No schema for table' in schema)


if __name__ == "__main__":
    load(TestShowSchema)
