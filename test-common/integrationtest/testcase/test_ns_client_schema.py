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
import libs.conf as conf


@ddt.ddt
class TestSchema(TestCaseBase):

    leader, slave1, slave2 = (i for i in conf.tb_endpoints)

    @multi_dimension(True)
    def test_schema(self):
        name = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        m = utils.gen_table_metadata(
            '"{}"'.format(name), '"kAbsoluteTime"', 144000, 8,
            ('table_partition', '"{}"'.format(self.leader), '"0-2"', 'true'),
            ('table_partition', '"{}"'.format(self.slave1), '"0-1"', 'false'),
            ('table_partition', '"{}"'.format(self.slave2), '"1-2"', 'false'),
            ('column_desc', '"k1"', '"string"', 'true'),
            ('column_desc', '"k2"', '"int16"', 'true'),
            ('column_desc', '"k3"', '"uint16"', 'false'),
            ('column_desc', '"k4"', '"int32"', 'false'),
            ('column_desc', '"k5"', '"uint32"', 'false'),
            ('column_desc', '"k6"', '"int64"', 'false'),
            ('column_desc', '"k7"', '"uint64"', 'false'),
            ('column_desc', '"k8"', '"bool"', 'false'),
            ('column_desc', '"k9"', '"float"', 'false'),
            ('column_desc', '"k10"', '"double"', 'false'),
            ('column_desc', '"k11"', '"timestamp"', 'false'),
            ('column_desc', '"k12"', '"date"', 'false'),
        )
        utils.gen_table_metadata_file(m, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs)

        row = ['testvalue0', '-100', '100','-1000', '1000', '-10000', '10000', 'true', '1.5', '1123.65', '1545724145000', '1545724145001']
        self.ns_put_multi(self.ns_leader, name, self.now(), row)
        time.sleep(0.5)
        rs = self.ns_scan_multi(self.ns_leader, name, 'testvalue0', 'k1', self.now() + 100, 0)
        self.assertEqual(rs[0]['k1'], 'testvalue0')
        self.assertEqual(rs[0]['k2'], '-100')
        self.assertEqual(rs[0]['k3'], '100')
        self.assertEqual(rs[0]['k4'], '-1000')
        self.assertEqual(rs[0]['k5'], '1000')
        self.assertEqual(rs[0]['k6'], '-10000')
        self.assertEqual(rs[0]['k7'], '10000')
        self.assertEqual(rs[0]['k8'], 'true')
        self.assertAlmostEqual(float(rs[0]['k9']), float('1.5'))
        self.assertAlmostEqual(float(rs[0]['k10']), float('1123.65'))
        self.assertEqual(rs[0]['k11'], '1545724145000')
        self.assertEqual(rs[0]['k12'], '1545724145001')

        rs1 = self.ns_get_multi(self.ns_leader, name, 'testvalue0', 'k1', 0)
        self.assertEqual(rs1['k1'], 'testvalue0')
        self.assertEqual(rs1['k2'], '-100')
        self.assertEqual(rs1['k3'], '100')
        self.assertEqual(rs1['k4'], '-1000')
        self.assertEqual(rs1['k5'], '1000')
        self.assertEqual(rs1['k6'], '-10000')
        self.assertEqual(rs1['k7'], '10000')
        self.assertEqual(rs1['k8'], 'true')
        self.assertAlmostEqual(float(rs1['k9']), float('1.5'))
        self.assertAlmostEqual(float(rs1['k10']), float('1123.65'))
        self.assertEqual(rs1['k11'], '1545724145000')
        self.assertEqual(rs1['k12'], '1545724145001')
        
if __name__ == "__main__":
    load(TestSchema)
