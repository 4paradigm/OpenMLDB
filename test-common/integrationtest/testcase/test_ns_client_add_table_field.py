# -*- coding: utf-8 -*-
from testcasebase import TestCaseBase
import threading
import time
from libs.deco import multi_dimension
from libs.logger import infoLogger
import libs.ddt as ddt
from libs.test_loader import load
import libs.utils as utils

@ddt.ddt
@multi_dimension(False)
class TestAddTableField(TestCaseBase):

    def test_add_table_field_with_columnkey(self):
        """
        指定时间列的schema表统计pk下的条数
        :return:
        """
        name = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
                "name": name,
                "ttl": 0,
               "column_desc":[
                   {"name": "card", "type": "string", "add_ts_idx": "true"},
                   {"name": "mcc", "type": "string", "add_ts_idx": "true"},
                   {"name": "amt", "type": "double", "add_ts_idx": "false"},
                   {"name": "ts1", "type": "int64", "add_ts_idx": "false", "is_ts_col": "true"},
                   {"name": "ts2", "type": "int64", "add_ts_idx": "false", "is_ts_col": "true"},
                   ],
               "column_key":[
                   {"index_name":"card", "ts_name":["ts1", "ts2"]},
                   {"index_name":"mcc", "ts_name":["ts2"]},
                   ]
               }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs)
        (schema, column_key) = self.ns_showschema(self.ns_leader, name)
        self.assertEqual(len(schema), 5)
        self.assertEqual(schema[0], ['0', 'card', 'string'])
        self.assertEqual(schema[1], ['1', 'mcc', 'string'])
        self.assertEqual(schema[2], ['2', 'amt', 'double'])
        self.assertEqual(schema[3], ['3', 'ts1', 'int64'])
        self.assertEqual(schema[4], ['4', 'ts2', 'int64'])
        self.assertEqual(len(column_key), 3)
        self.assertEqual(column_key[0], ["0", "card", "card", "ts1", "0min"])
        self.assertEqual(column_key[1], ["1", "card", "card", "ts2", "0min"])
        self.assertEqual(column_key[2], ["2", "mcc", "mcc", "ts2", "0min"])
        
        self.ns_add_table_field(self.ns_leader, name, 'aa', 'string');
        (schema, column_key) = self.ns_showschema(self.ns_leader, name)
        self.assertEqual(len(schema), 6)
        self.assertEqual(schema[0], ['0', 'card', 'string'])
        self.assertEqual(schema[1], ['1', 'mcc', 'string'])
        self.assertEqual(schema[2], ['2', 'amt', 'double'])
        self.assertEqual(schema[3], ['3', 'ts1', 'int64'])
        self.assertEqual(schema[4], ['4', 'ts2', 'int64'])
        self.assertEqual(schema[5], ['5', 'aa', 'string'])
        self.assertEqual(len(column_key), 3)
        self.assertEqual(column_key[0], ["0", "card", "card", "ts1", "0min"])
        self.assertEqual(column_key[1], ["1", "card", "card", "ts2", "0min"])
        self.assertEqual(column_key[2], ["2", "mcc", "mcc", "ts2", "0min"])

        self.ns_drop(self.ns_leader, name)
    
    def test_add_table_field_without_columnkey(self):
        name = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
                "name": name,
                "ttl": 14400,
                "column_desc":[
                    {"name": "card", "type": "string", "add_ts_idx": "true"},
                    {"name": "mcc", "type": "string", "add_ts_idx": "true"},
                    {"name": "amt", "type": "double", "add_ts_idx": "false"},
                    {"name": "ts1", "type": "int64", "add_ts_idx": "false"}
                    ]
                }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs)
        (schema, column_key) = self.ns_showschema(self.ns_leader, name)
        self.assertEqual(len(schema), 4)
        self.assertEqual(schema[0], ["0", "card", "string", "yes"])
        self.assertEqual(schema[1], ["1", "mcc", "string", "yes"])
        self.assertEqual(schema[2], ["2", "amt", "double", "no"])
        self.assertEqual(schema[3], ["3", "ts1", "int64", "no"])
        self.ns_add_table_field(self.ns_leader, name, 'aa', 'string');
        (schema, column_key) = self.ns_showschema(self.ns_leader, name)
        self.assertEqual(len(schema), 5)
        self.assertEqual(schema[0], ["0", "card", "string", "yes"])
        self.assertEqual(schema[1], ["1", "mcc", "string", "yes"])
        self.assertEqual(schema[2], ["2", "amt", "double", "no"])
        self.assertEqual(schema[3], ["3", "ts1", "int64", "no"])
        self.assertEqual(schema[4], ["4", "aa", "string", "no"])

        self.ns_drop(self.ns_leader, name)
if __name__ == "__main__":
    load(TestAddTableField)

