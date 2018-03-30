# -*- coding: utf-8 -*-
from testcasebase import TestCaseBase
from libs.deco import multi_dimension
from libs.test_loader import load
from libs.logger import infoLogger


class TestShowSchema(TestCaseBase):

    @multi_dimension(True)
    def test_showschema_tid_not_exist(self):
        """
        tid不存在，查看schema
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid, 144000, 2, 'true',
                          card='string:index', merchant='string:index', amt='double:index')
        self.assertIn('Create table ok', rs1)
        schema = self.showschema(self.leader, self.tid, self.pid)
        self.assertEqual(schema, {'merchant': ['string', 'yes'],
                                  'amt': ['double', 'yes'],
                                  'card': ['string', 'yes']})
        schema = self.run_client(self.leader, 'showschema {} {}'.format(self.tid + 1, self.pid))
        self.assertIn('No schema for table', schema)
        schema = self.run_client(self.leader, 'showschema {} {}'.format(self.tid, self.pid + 1))
        self.assertIn('No schema for table', schema)


if __name__ == "__main__":
    load(TestShowSchema)
