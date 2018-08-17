# -*- coding: utf-8 -*-
import time
from testcasebase import TestCaseBase
from libs.deco import *
import libs.conf as conf
from libs.test_loader import load
import libs.ddt as ddt
import libs.utils as utils
from libs.logger import infoLogger
import collections


@ddt.ddt
class TestCreateTableByNsClient(TestCaseBase):

    leader, slave1, slave2 = (i[1] for i in conf.tb_endpoints)

    @multi_dimension(True)
    @ddt.data(
        ('Create table ok',
        ('column_desc', '"card"', '"string"', 'true')),

        ('no index',
        ('column_desc', '"card"', '"double"', 'false')),

        ('no index',
        ('column_desc', '"k1"', '"string"', 'false'),
        ('column_desc', '"k2"', '"string"', 'false'),
        ('column_desc', '"k3"', '"double"', 'false')),

        ('Create table ok',
        ('column_desc', '"k1"', '"string"', 'true'),
        ('column_desc', '"k2"', '"string"', 'false'),
        ('column_desc', '"k3"', '"double"', 'false')),

        ('Create table ok',
        ('column_desc', '"k1"', '"string"', 'true'),
        ('column_desc', '"k2"', '"string"', 'true'),
        ('column_desc', '"k3"', '"double"', 'true')),

        ('check column_desc name failed. name is card',
        ('column_desc', '"card"', '"string"', 'true'),
        ('column_desc', '"card"', '"double"', 'false')),

        ('Create table ok',
        ('column_desc', '"k1"', '"string"', 'true'),
        ('column_desc', '"k2"', '"float"', 'false'),
        ('column_desc', '"k3"', '"double"', 'false'),
        ('column_desc', '"k4"', '"int32"', 'false'),
        ('column_desc', '"k5"', '"uint32"', 'false'),
        ('column_desc', '"k6"', '"int64"', 'false'),
        ('column_desc', '"k7"', '"uint64"', 'false')),

        ('Create table ok',
        ('column_desc', '"k1"', '"string"', 'true'),
        ('column_desc', '"k2"', '"float"', 'true'),
        ('column_desc', '"k3"', '"double"', 'true'),
        ('column_desc', '"k4"', '"int32"', 'true'),
        ('column_desc', '"k5"', '"uint32"', 'true'),
        ('column_desc', '"k6"', '"int64"', 'true'),
        ('column_desc', '"k7"', '"uint64"', 'true')),

        ('type double2 is invalid',
        ('column_desc', '"k1"', '"string"', 'true'),
        ('column_desc', '"k2"', '"double2"', 'true')),
    )
    @ddt.unpack
    def test_create_compressed_table(self, exp_msg, *column_descs):
        """
        column_desc参数测试
        :param exp_msg:
        :param column_descs:
        :return:
        """
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        m = utils.gen_table_metadata(
            '"tname{}"'.format(time.time()), '"kAbsoluteTime"', 144000, 8,
            ('table_partition', '"{}"'.format(self.leader), '"0"', 'true'),
            ('table_partition', '"{}"'.format(self.slave1), '"0"', 'false'),
            ('table_partition', '"{}"'.format(self.slave2), '"0"', 'false'),
            *column_descs)
        m[0].append(("compress_type",'"snappy"'))
        utils.gen_table_metadata_file(m, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        infoLogger.info(rs)
        self.assertIn(exp_msg, rs)
        if exp_msg == 'Create table ok':
            rs1 = self.showtable(self.ns_leader)
            infoLogger.info(rs1)
            for v in rs1.values():
                self.assertEqual(v[3], "kSnappy")

            # for edp in (self.leader, self.slave1, self.slave2):
            #     schema = self.showschema(edp, tid, 2)
            #     infoLogger.info(schema)
            #     self.assertEqual(len(schema), len(column_descs))
            #     for i in column_descs:
            #         key = i[1][1:-1]
            #         type = i[2][1:-1]
            #         index = 'yes' if i[3] == 'true' else 'no'
            #         self.assertEqual(schema[key], [type, index])

    @ddt.data(
        ('Create table ok', '0', '8', '3', ''),
        ('Create table ok', 'latest:10', '8', '3', ''),
        ('Create table ok', '144000', '8', '3', 'k1:string:index k2:double k3:int32:index'),
        ('partition_num should be large than zero', '144000', '0', '3', ''),
    )
    @ddt.unpack
    def test_create_cmd(self, exp_msg, ttl, partition_num, replica_num, schema):
        """
        不用文件, 直接在命令行指定建表信息
        :return:
        """
        tname = 'tname{}'.format(time.time())
        rs = self.ns_create_cmd(self.ns_leader, tname, ttl, partition_num, replica_num, schema)
        infoLogger.info(rs)
        self.assertIn(exp_msg, rs)
        if (schema != ''):
            self.assertIn(exp_msg, rs)
            rs1 = self.showtable(self.ns_leader)
            tid = rs1.keys()[0][1]
            infoLogger.info(rs1)
            schema = self.showschema(self.slave1, tid, 0)
            infoLogger.info(schema)
            self.assertEqual(len(schema), 3)
            self.assertEqual(schema['k1'], ['string', 'yes'])
            self.assertEqual(schema['k2'], ['double', 'no'])
            self.assertEqual(schema['k3'], ['int32', 'yes'])


if __name__ == "__main__":
    load(TestCreateTableByNsClient)
