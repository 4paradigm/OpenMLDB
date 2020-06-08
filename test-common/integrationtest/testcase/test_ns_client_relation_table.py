# -*- coding: utf-8 -*-
from testcasebase import TestCaseBase
from libs.test_loader import load
import libs.ddt as ddt
import time
import libs.utils as utils
from libs.deco import multi_dimension

@multi_dimension(False)
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
                   {"name": "sex", "type": "bool", "not_null": "true"},
                   {"name": "mcc", "type": "int", "not_null": "true"},
                   {"name": "attribute", "type": "varchar", "not_null": "true"},
                   {"name": "image", "type": "varchar", "not_null": "true"},
                   {"name": "date", "type": "date", "not_null": "true"},
                   {"name": "ts", "type": "timestamp", "not_null": "true"},
                   ],
               "index":[
                   {"index_name":"idx1", "col_name":["id", "name"], "index_type":["PrimaryKey"]},
                   {"index_name":"idx2", "col_name":["mcc"], "index_type":["nounique"]},
                   {"index_name":"idx3", "col_name":["date", "ts"], "index_type":["unique"]},
                   ]
               }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs)
        (schema, column_key) = self.ns_showschema(self.ns_leader, name)
        self.assertEqual(len(schema), 8)
        # put 
        rs1 = self.ns_put_relation(self.ns_leader, name, "id=11 name=n1 sex=true mcc=1 attribute=a1 image=i1 date=2020-2-21 ts=1588756531")
        self.assertIn('put ok', rs1)
        rs1 = self.ns_put_relation(self.ns_leader, name, "id=12 name=n2 sex=false mcc=2 attribute=a2 image=i2 date=2020-2-22 ts=1588756532")
        self.assertIn('put ok', rs1)
        rs1 = self.ns_put_relation(self.ns_leader, name, "id=12 name=n3 sex=true mcc=2 attribute=a3 image=i3 date=2020-2-23 ts=1588756533")
        self.assertIn('put ok', rs1)
        # preview
        rs = self.ns_preview(self.ns_leader, name)
        self.assertEqual(len(rs), 3)
        self.assertEqual(rs[0]['id'], '11')
        self.assertEqual(rs[0]['name'], 'n1')
        self.assertEqual(rs[0]['sex'], "true")
        self.assertEqual(rs[0]['mcc'], '1')
        self.assertEqual(rs[0]['attribute'], 'a1')
        self.assertEqual(rs[0]['image'], 'i1')
        self.assertEqual(rs[0]['date'], "2020-02-21")
        self.assertEqual(rs[0]['ts'], "1588756531")
        self.assertEqual(rs[1]['id'], '12')
        self.assertEqual(rs[1]['name'], 'n2')
        self.assertEqual(rs[1]['sex'], "false")
        self.assertEqual(rs[1]['mcc'], '2')
        self.assertEqual(rs[1]['attribute'], 'a2')
        self.assertEqual(rs[1]['image'], 'i2')
        self.assertEqual(rs[1]['date'], "2020-02-22")
        self.assertEqual(rs[1]['ts'], "1588756532")
        self.assertEqual(rs[2]['id'], '12')
        self.assertEqual(rs[2]['name'], 'n3')
        self.assertEqual(rs[0]['sex'], "true")
        self.assertEqual(rs[2]['mcc'], '2')
        self.assertEqual(rs[2]['attribute'], 'a3')
        self.assertEqual(rs[2]['image'], 'i3')
        self.assertEqual(rs[2]['date'], "2020-02-23")
        self.assertEqual(rs[2]['ts'], "1588756533")
        # query    
        rs = self.ns_query(self.ns_leader, name, "* where id=11 name=n1")
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0]['id'], '11')
        self.assertEqual(rs[0]['name'], 'n1')
        self.assertEqual(rs[0]['sex'], "true")
        self.assertEqual(rs[0]['mcc'], '1')
        self.assertEqual(rs[0]['attribute'], 'a1')
        self.assertEqual(rs[0]['image'], 'i1')
        self.assertEqual(rs[0]['date'], "2020-02-21")
        self.assertEqual(rs[0]['ts'], "1588756531")
        rs = self.ns_query(self.ns_leader, name, "* where mcc=2")
        self.assertEqual(len(rs), 2)
        self.assertEqual(rs[0]['id'], '12')
        self.assertEqual(rs[0]['name'], 'n2')
        self.assertEqual(rs[0]['sex'], "false")
        self.assertEqual(rs[0]['mcc'], '2')
        self.assertEqual(rs[0]['attribute'], 'a2')
        self.assertEqual(rs[0]['image'], 'i2')
        self.assertEqual(rs[0]['date'], "2020-02-22")
        self.assertEqual(rs[0]['ts'], "1588756532")
        self.assertEqual(rs[1]['id'], '12')
        self.assertEqual(rs[1]['name'], 'n3')
        self.assertEqual(rs[1]['sex'], "true")
        self.assertEqual(rs[1]['mcc'], '2')
        self.assertEqual(rs[1]['attribute'], 'a3')
        self.assertEqual(rs[1]['image'], 'i3')
        self.assertEqual(rs[1]['date'], "2020-02-23")
        self.assertEqual(rs[1]['ts'], "1588756533")
        # update
        rs = self.ns_update(self.ns_leader, name, "id=13 where id=11 name=n1")
        self.assertIn('update ok', rs)
        rs = self.ns_preview(self.ns_leader, name)
        self.assertEqual(len(rs), 3)
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
        self.assertEqual(rs[2]['id'], '13')
        self.assertEqual(rs[2]['name'], 'n1')
        self.assertEqual(rs[2]['sex'], "true")
        self.assertEqual(rs[2]['mcc'], '1')
        self.assertEqual(rs[2]['attribute'], 'a1')
        self.assertEqual(rs[2]['image'], 'i1')
        self.assertEqual(rs[2]['date'], "2020-02-21")
        self.assertEqual(rs[2]['ts'], "1588756531")
        rs = self.ns_update(self.ns_leader, name, "id=14 mcc=3 where mcc=2")
        rs = self.ns_preview(self.ns_leader, name)
        self.assertEqual(len(rs), 3)
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
        self.assertEqual(rs[2]['id'], '13')
        self.assertEqual(rs[2]['name'], 'n1')
        self.assertEqual(rs[2]['sex'], "true")
        self.assertEqual(rs[2]['mcc'], '1')
        self.assertEqual(rs[2]['attribute'], 'a1')
        self.assertEqual(rs[2]['image'], 'i1')
        self.assertEqual(rs[2]['date'], "2020-02-21")
        self.assertEqual(rs[2]['ts'], "1588756531")
        #delete
        rs = self.ns_delete_relation(self.ns_leader, name, "where id=13 name=n1")
        self.assertIn('delete ok', rs)
        rs = self.ns_preview(self.ns_leader, name)
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
        rs = self.ns_delete_relation(self.ns_leader, name, "where mcc=3")
        self.assertIn('delete ok', rs)

    def test_put(self):
        name = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
                "name": name,
                "table_type" : "Relational",
               "column_desc":[
                   {"name": "id", "type": "bigint", "not_null": "true"},
                   {"name": "attribute", "type": "varchar", "not_null": "true"},
                   {"name": "image", "type": "varchar", "not_null": "true"},
                   ],
               "index":[
                   {"index_name":"idx1", "col_name":["id"], "index_type":["AutoGen"]},
                   ]
               }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        (schema, column_key) = self.ns_showschema(self.ns_leader, name)
        self.assertEqual(len(schema), 3)
        # put 
        rs = self.ns_put_relation(self.ns_leader, name, "id=11 attribute=a1 image=i1")
        self.assertIn('put format error', rs)
        rs = self.ns_put_relation(self.ns_leader, name, "attribute=a1 image=i1")
        self.assertIn('put ok', rs)
        # preview
        rs = self.ns_preview(self.ns_leader, name)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0]['image'], 'i1')
        self.assertEqual(rs[0]['attribute'], 'a1')

    def test_load(self):
        name = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
                "name": name,
                "table_type" : "Relational",
               "column_desc":[
                   {"name": "id", "type": "bigint", "not_null": "true"},
                   {"name": "attribute", "type": "varchar", "not_null": "true"},
                   {"name": "image", "type": "varchar", "not_null": "true"},
                   ],
               "index":[
                   {"index_name":"idx1", "col_name":["id"], "index_type":["PrimaryKey"]},
                   ]
               }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs)
        (schema, column_key) = self.ns_showschema(self.ns_leader, name)
        self.assertEqual(len(schema), 3)
        # put 
        rs = self.ns_put_relation(self.ns_leader, name, "id=11 attribute=a1 image=i1")
        self.assertIn('put ok', rs)
        # preview
        rs = self.ns_preview(self.ns_leader, name)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0]['id'], "11")
        self.assertEqual(rs[0]['image'], 'i1')
        self.assertEqual(rs[0]['attribute'], 'a1')
        table_info = self.showtable(self.ns_leader, name)
        tid = table_info.keys()[0][1]
        pid = 0 
     
        self.stop_client(self.leader)
        self.start_client(self.leader)
        time.sleep(5)
        rs = self.ns_preview(self.ns_leader, name)
        self.assertEqual(len(rs), 0)

        rs = self.load_relation_table(self.leader, tid, pid, "hdd");
        self.assertIn('LoadTable ok', rs)
        # preview
        rs = self.ns_preview(self.ns_leader, name)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0]['id'], "11")
        self.assertEqual(rs[0]['image'], 'i1')
        self.assertEqual(rs[0]['attribute'], 'a1')

if __name__ == "__main__":
    load(TestRelationTable)
