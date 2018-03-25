# -*- coding: utf-8 -*-
from testcasebase import TestCaseBase
from libs.deco import multi_dimension
from libs.test_loader import load
from libs.logger import infoLogger


class TestCreateTable(TestCaseBase):

    @multi_dimension(False)
    def test_create_table_success(self):
        """
        创建带有主从关系的表成功
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid, 144000, 2, 'true', self.slave1, self.slave2)
        self.assertIn('Create table ok' ,rs1)
        table_status = self.get_table_status(self.leader, self.tid, self.pid)
        self.assertEqual(table_status[:6], ['0', 'kTableLeader', 'kTableNormal', 'true', '144000min', '0s'])

        rs2 = self.create(self.slave1, 't', self.tid, self.pid, 144000, 2, 'false', self.slave1, self.slave2)
        self.assertIn('Create table ok' ,rs2)
        table_status = self.get_table_status(self.slave1, self.tid, self.pid)
        self.assertEqual(table_status[:6], ['0', 'kTableFollower', 'kTableNormal', 'true', '144000min', '0s'])

        rs3 = self.create(self.slave2, 't', self.tid, self.pid, 144000, 2, 'false', self.slave1, self.slave2)
        self.assertIn('Create table ok' ,rs3)
        table_status = self.get_table_status(self.slave1, self.tid, self.pid)
        self.assertEqual(table_status[:6], ['0', 'kTableFollower', 'kTableNormal', 'true', '144000min', '0s'])


    @multi_dimension(True)
    def test_screate_table_allindex(self):
        """
        创建高维表，所有schema字段都是index
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid, 144000, 2, 'true',
                          card='string:index', merchant='string:index', amt='double:index')
        self.assertIn('Create table ok' ,rs1)
        schema_d = self.showschema(self.leader, self.tid, self.pid)
        self.assertEqual(schema_d['card'], ['string', 'yes'])
        self.assertEqual(schema_d['merchant'], ['string', 'yes'])
        self.assertEqual(schema_d['amt'], ['double', 'yes'])


    @multi_dimension(True)
    def test_screate_table_schema_toolong(self):
        """
        创建高维表，所有schema字段都是index
        :return:
        """
        self.multidimension_vk = {'a' * 126: ('string:index', '1'),
                                  'b': ('string:index', '2'),
                                  'c': ('string', '3')}
        rs1 = self.create(self.leader, 't', self.tid, self.pid)
        self.assertIn('Create table ok' ,rs1)
        self.multidimension_vk = {'a' * 127: ('string:index', '1'),
                                  'b': ('string:index', '2'),
                                  'c': ('string', '3')}
        rs2 = self.create(self.leader, 't', self.tid, self.pid)
        self.assertIn('Fail to create table', rs2)


    @multi_dimension(True)
    def test_screate_table_1index(self):
        """
        创建高维表，1个index，检查schema
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid, 144000, 2, 'true',
                          card='string:index', merchant='string')
        self.assertIn('Create table ok' ,rs1)
        schema_d = self.showschema(self.leader, self.tid, self.pid)
        self.assertEqual(schema_d['card'], ['string', 'yes'])
        self.assertEqual(schema_d['merchant'], ['string', 'no'])
        rs2 = self.get_table_meta(self.leaderpath, self.tid, self.pid)
        self.assertEqual(rs2['ttl'], '144000')
        self.assertEqual(rs2['ttl_type'], 'kAbsoluteTime')
        self.assertEqual(rs2['dimensions'], '"card"')
        self.assertEqual(rs2['schema'], '"\\000\\000\\010merchant\\000\\001\\004card"')


    @multi_dimension(True)
    def test_screate_table_0index(self):
        """
        创建高维表，无index，检查schema
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid, 144000, 2, 'true',
                          card='string', merchant='string')
        self.assertIn('Create table ok' ,rs1)
        schema_d = self.showschema(self.leader, self.tid, self.pid)
        self.assertEqual(schema_d['card'], ['string', 'no'])
        self.assertEqual(schema_d['merchant'], ['string', 'no'])


    @multi_dimension(True)
    def test_screate_table_noschema(self):
        """
        创建高维表，无schema
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid, 144000, 2, 'true', **{'': ''})
        self.assertIn('Create table ok' ,rs1)
        schema = self.run_client(self.leader, 'showschema {} {}'.format(self.tid, self.pid))
        self.assertIn('No schema for table', schema)


    @multi_dimension(True)
    def test_screate_table_1schema(self):
        """
        创建高维表，仅有1个schema字段，创建成功
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid, 144000, 2, 'true', card='string:index')
        self.assertIn('Create table ok' ,rs1)
        schema_d = self.showschema(self.leader, self.tid, self.pid)
        self.assertEqual(schema_d['card'], ['string', 'yes'])


    @multi_dimension(True)
    def test_screate_table_repeatschema(self):
        """
        创建高维表，schema字段重复，创建失败
        :return:
        """
        rs1 = self.run_client(self.leader, 'screate t {} {} 144000 2 true card:string:index card:string:index'.format(
            self.tid, self.pid))
        self.assertIn('Duplicated column card', rs1)


    @multi_dimension(True)
    def test_screate_table_latest_ttl(self):
        """
        创建高维表，ttl是latest
        :return:
        """
        rs1 = self.run_client(
            self.leader,
            'screate t {} {} latest:10 2 true k1:string:index k2:string:index k3:string:index'.format(
            self.tid, self.pid))
        self.assertIn('Create table ok' ,rs1)
        schema_d = self.showschema(self.leader, self.tid, self.pid)
        self.assertEqual(schema_d['k1'], ['string', 'yes'])
        self.assertEqual(schema_d['k2'], ['string', 'yes'])
        self.assertEqual(schema_d['k3'], ['string', 'yes'])
        rs2 = self.get_table_meta(self.leaderpath, self.tid, self.pid)
        infoLogger.info(rs2)
        self.assertEqual(rs2['ttl_type'], 'kLatestTime')
        self.assertEqual(rs2['ttl'], '10')
        self.assertEqual(rs2['dimensions'], '"k3"|"k2"|"k1"')
        self.assertEqual(rs2['schema'], '"\\000\\001\\002k1\\000\\001\\002k2\\000\\001\\002k3"')


if __name__ == "__main__":
    load(TestCreateTable)
