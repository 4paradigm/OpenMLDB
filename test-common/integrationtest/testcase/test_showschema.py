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

    # @multi_dimension(True)
    def test_showschema_type(self):
        """
        测试showschema是否支持所有字段类型
        :return:
        """
        rs1 = self.create(self.leader, 'table', self.tid, self.pid, 144000, 2, 'true',
                          type_int32='int32:index',
                          type_int64='int64:index',
                          type_uint32='uint32:index',
                          type_uint64='uint64:index',
                          type_double='double:index',
                          type_float='float:index',
                          type_string='string:index',
                          type_timestamp='timestamp:index',
                          type_date='date:index',
                          type_int16='int16:index',
                          type_uint16='uint16:index',
                        #   type_bool='bool:index'
                          )
        self.assertIn('Create table ok' ,rs1)
        schema_d = self.showschema(self.leader, self.tid, self.pid)
        self.assertEqual(schema_d['type_int32'], ['int32', 'yes'])
        self.assertEqual(schema_d['type_int64'], ['int64', 'yes'])
        self.assertEqual(schema_d['type_uint32'], ['uint32', 'yes'])
        self.assertEqual(schema_d['type_uint64'], ['uint64', 'yes'])
        self.assertEqual(schema_d['type_double'], ['double', 'yes'])
        self.assertEqual(schema_d['type_float'], ['float', 'yes'])
        self.assertEqual(schema_d['type_string'], ['string', 'yes'])
        self.assertEqual(schema_d['type_timestamp'], ['timestamp', 'yes'])
        self.assertEqual(schema_d['type_date'], ['date', 'yes'])
        self.assertEqual(schema_d['type_int16'], ['int16', 'yes'])
        self.assertEqual(schema_d['type_uint16'], ['uint16', 'yes'])
        # self.assertEqual(schema_d['type_bool'], ['bool', 'yes'])




if __name__ == "__main__":
    load(TestShowSchema)
