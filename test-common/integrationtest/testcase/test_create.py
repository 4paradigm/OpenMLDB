# -*- coding: utf-8 -*-
import unittest
from testcasebase import TestCaseBase
import xmlrunner
from libs.deco import *
import libs.conf as conf
from libs.test_loader import load
import libs.ddt as ddt
from libs.test_loader import load


class TestCreateTable(TestCaseBase):

    @multi_dimension(False)
    def test_create_table(self):
        """
        创建带有主从关系的表成功
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid, 144000, 2, 'true', self.slave1, self.slave2)
        self.assertTrue('Create table ok' in rs1)
        table_status = self.get_table_status(self.leader, self.tid, self.pid)
        self.assertEqual(table_status, ['0', 'kTableLeader', 'kTableNormal', 'true', '144000min', '0s'])

        rs2 = self.create(self.slave1, 't', self.tid, self.pid, 144000, 2, 'false', self.slave1, self.slave2)
        self.assertTrue('Create table ok' in rs2)
        table_status = self.get_table_status(self.slave1, self.tid, self.pid)
        self.assertEqual(table_status, ['0', 'kTableFollower', 'kTableNormal', 'true', '144000min', '0s'])

        rs3 = self.create(self.slave2, 't', self.tid, self.pid, 144000, 2, 'false', self.slave1, self.slave2)
        self.assertTrue('Create table ok' in rs3)
        table_status = self.get_table_status(self.slave1, self.tid, self.pid)
        self.assertEqual(table_status, ['0', 'kTableFollower', 'kTableNormal', 'true', '144000min', '0s'])


    @multi_dimension(True)
    def test_screate_table_allindex(self):
        """
        创建高维表，所有schema字段都是index
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


    @multi_dimension(True)
    def test_screate_table_1index(self):
        """
        创建高维表，1个index，检查schema
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid, 144000, 2, 'true',
                          card='string:index', merchant='string')
        self.assertTrue('Create table ok' in rs1)
        schema = self.run_client(self.leader, 'showschema {} {}'.format(self.tid, self.pid))
        schema_d = self.parse_sechema(schema)
        self.assertEqual(schema_d['card'], ['string', 'yes'])
        self.assertEqual(schema_d['merchant'], ['string', 'no'])


    @multi_dimension(True)
    def test_screate_table_0index(self):
        """
        创建高维表，无index，检查schema
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid, 144000, 2, 'true',
                          card='string', merchant='string')
        self.assertTrue('Create table ok' in rs1)
        schema = self.run_client(self.leader, 'showschema {} {}'.format(self.tid, self.pid))
        schema_d = self.parse_sechema(schema)
        self.assertEqual(schema_d['card'], ['string', 'no'])
        self.assertEqual(schema_d['merchant'], ['string', 'no'])


    @multi_dimension(True)
    def test_screate_table_noschema(self):
        """
        创建高维表，无schema
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid, 144000, 2, 'true', **{'': ''})
        self.assertTrue('Create table ok' in rs1)
        schema = self.run_client(self.leader, 'showschema {} {}'.format(self.tid, self.pid))
        self.assertTrue('No schema for table' in schema)


    @multi_dimension(True)
    def test_screate_table_1schema(self):
        """
        创建高维表，仅有1个schema字段，创建成功
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid, 144000, 2, 'true', card='string:index')
        self.assertTrue('Create table ok' in rs1)
        schema = self.run_client(self.leader, 'showschema {} {}'.format(self.tid, self.pid))
        schema_d = self.parse_sechema(schema)
        self.assertEqual(schema_d['card'], ['string', 'yes'])


    @multi_dimension(True)
    def test_screate_table_repeatschema(self):
        """
        创建高维表，schema字段重复，创建失败
        :return:
        """
        rs1 = self.run_client(self.leader, 'screate t {} {} 144000 2 true card:string:index card:string:index'.format(
            self.tid, self.pid))
        self.assertTrue('Duplicated column card' in rs1)


if __name__ == "__main__":
    import libs.test_loader
    load(TestCreateTable)
