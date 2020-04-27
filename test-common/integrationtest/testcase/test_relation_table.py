# -*- coding: utf-8 -*-
from testcasebase import TestCaseBase
from libs.test_loader import load
import libs.ddt as ddt
import time
import libs.utils as utils

class TestRelationTable(TestCaseBase):

    def test_relation_table(self):
        name = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
                "name": name,
                "table_type" : "Relational",
               "column_desc":[
                   {"name": "id", "type": "bigint", "not_null": "true"},
                   {"name": "name", "type": "string", "not_null": "true"},
                   {"name": "mcc", "type": "int", "not_null": "true"},
                   {"name": "attribute", "type": "varchar", "not_null": "true"},
                   {"name": "image", "type": "varchar", "not_null": "true"},
                   ],
               "index":[
                   {"index_name":"idx1", "col_name":["id", "name"], "index_type":["PrimaryKey"]},
                   {"index_name":"idx2", "col_name":["mcc"], "index_type":["nounique"]},
                   ]
               }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs)
        (schema, column_key) = self.ns_showschema(self.ns_leader, name)
        self.assertEqual(len(schema), 5)
 
        rs1 = self.ns_put_relation(self.ns_leader, name, "id=11 name=n1 mcc=1 attribute=a1 image=i1")
        self.assertIn('put ok', rs1)
        rs1 = self.ns_put_relation(self.ns_leader, name, "id=12 name=n2 mcc=2 attribute=a2 image=i2")
        self.assertIn('put ok', rs1)
        rs1 = self.ns_put_relation(self.ns_leader, name, "id=12 name=n3 mcc=2 attribute=a3 image=i3")
        self.assertIn('put ok', rs1)
        
        rs = self.ns_preview(self.ns_leader, name)
        self.assertEqual(len(rs), 3)
        self.assertEqual(rs[0]['id'], '11')
        self.assertEqual(rs[0]['name'], 'n1')
        self.assertEqual(rs[0]['mcc'], '1')
        self.assertEqual(rs[0]['attribute'], 'a1')
        self.assertEqual(rs[0]['image'], 'i1')
        self.assertEqual(rs[1]['id'], '12')
        self.assertEqual(rs[1]['name'], 'n2')
        self.assertEqual(rs[1]['mcc'], '2')
        self.assertEqual(rs[1]['attribute'], 'a2')
        self.assertEqual(rs[1]['image'], 'i2')
        self.assertEqual(rs[2]['id'], '12')
        self.assertEqual(rs[2]['name'], 'n3')
        self.assertEqual(rs[2]['mcc'], '2')
        self.assertEqual(rs[2]['attribute'], 'a3')
        self.assertEqual(rs[2]['image'], 'i3')

        rs = self.ns_query(self.ns_leader, name, "* where id=11 name=n1")
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0]['id'], '11')
        self.assertEqual(rs[0]['name'], 'n1')
        self.assertEqual(rs[0]['mcc'], '1')
        self.assertEqual(rs[0]['attribute'], 'a1')
        self.assertEqual(rs[0]['image'], 'i1')

        rs = self.ns_query(self.ns_leader, name, "* where mcc=2")
        self.assertEqual(len(rs), 2)
        self.assertEqual(rs[0]['id'], '12')
        self.assertEqual(rs[0]['name'], 'n2')
        self.assertEqual(rs[0]['mcc'], '2')
        self.assertEqual(rs[0]['attribute'], 'a2')
        self.assertEqual(rs[0]['image'], 'i2')
        self.assertEqual(rs[1]['id'], '12')
        self.assertEqual(rs[1]['name'], 'n3')
        self.assertEqual(rs[1]['mcc'], '2')
        self.assertEqual(rs[1]['attribute'], 'a3')
        self.assertEqual(rs[1]['image'], 'i3')
if __name__ == "__main__":
    load(TestRelationTable)
