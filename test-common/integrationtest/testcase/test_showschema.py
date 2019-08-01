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
                          card='string:index', merchant='string:index', amt='double')
        self.assertIn('Create table ok', rs1)
        (schema, column_key) = self.showschema(self.leader, self.tid, self.pid)
        self.assertEqual(len(schema), 3)
        self.assertEqual(schema[0], ['0', 'merchant', 'string', 'yes'])
        self.assertEqual(schema[1], ['1', 'amt', 'double', 'yes'])
        self.assertEqual(schema[2], ['2', 'card', 'string', 'yes'])
        schema = self.run_client(self.leader, 'showschema {} {}'.format(self.tid + 1, self.pid))
        self.assertIn('ShowSchema failed', schema)
        schema = self.run_client(self.leader, 'showschema {} {}'.format(self.tid, self.pid + 1))
        self.assertIn('ShowSchema failed', schema)

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
                          type_double='double',
                          type_float='float',
                          type_string='string:index',
                          type_timestamp='timestamp:index',
                          type_date='date:index',
                          type_int16='int16:index',
                          type_uint16='uint16:index',
                          type_bool='bool:index'
                          )
        self.assertIn('Create table ok' ,rs1)
        (schema, column_key) = self.showschema(self.leader, self.tid, self.pid)
        self.assertEqual(len(column_key), 0)
        self.assertEqual(schema[0], ['0', 'type_string', 'string', 'yes'])
        self.assertEqual(schema[1], ['1', 'type_bool', 'bool', 'yes'])
        self.assertEqual(schema[2], ['2', 'type_int64', 'int64', 'yes'])
        self.assertEqual(schema[3], ['3', 'type_uint64', 'uint64', 'yes'])
        self.assertEqual(schema[4], ['4', 'type_timestamp', 'timestamp', 'yes'])
        self.assertEqual(schema[5], ['5', 'type_int16', 'int16', 'yes'])
        self.assertEqual(schema[6], ['6', 'type_double', 'double', 'yes'])
        self.assertEqual(schema[7], ['7', 'type_uint16', 'uint16', 'yes'])
        self.assertEqual(schema[8], ['8', 'type_date', 'date', 'yes'])
        self.assertEqual(schema[9], ['9', 'type_int32', 'int32', 'yes'])
        self.assertEqual(schema[10], ['10', 'type_float', 'float', 'yes'])
        self.assertEqual(schema[11], ['11', 'type_uint32', 'uint32', 'yes'])

if __name__ == "__main__":
    load(TestShowSchema)
