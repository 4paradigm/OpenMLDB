#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright 2021 4Paradigm
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
import logging
import time
from datetime import date
from datetime import datetime

import sqlalchemy as db

logging.basicConfig(level=logging.WARNING)
class TestOpenMLDBClient(unittest.TestCase):

  def test_basic(self):
    ddl = "create table tsql1010 ( col1 bigint, col2 date, col3 string, col4 string, col5 int, index(key=col3, ts=col1));"
    logging.info("test_basic ...")
    engine = db.create_engine('openmldb:///db_test?zk=127.0.0.1:6181&zkPath=/onebox')
    
    connection = engine.connect()
    try:
      connection.execute("create database db_test;")
    except Exception as e:
      pass
    try:
      connection.execute("drop table tsql1010;")
    except Exception as e:
      pass

    time.sleep(2)
    
    connection.execute(ddl)
    self.check_has_table(connection)
    insert1 = "insert into tsql1010 values(1000, '2020-12-25', 'guangdon', '广州', 1);"
    insert2 = "insert into tsql1010 values(1001, '2020-12-26', 'hefei', ?, ?);" # anhui 2
    insert3 = "insert into tsql1010 values(1002, '2020-12-27', ?, ?, 3);" # fujian fuzhou
    insert4 = "insert into tsql1010 values(?, ?, ?, ?, ?);" # 1003 2020-11-28 jiangxi nanchang 4
    insert5 = "insert into tsql1010 values(1004, ?, 'hubei', 'wuhan', 5);" # 2020-11-29
    connection.execute(insert1);
    connection.execute(insert2, ({"col4":"anhui", "col5":2}));
    connection.execute(insert3, ({"col3":"fujian", "col4":"fuzhou"}));
    connection.execute(insert4, ({"col1":1003, "col2":"2020-12-28", "col3":"jiangxi", "col4":"nanchang", "col5":4}));
    connection.execute(insert5, ({"col2":"2020-12-29"}));
    
    self.check_fetchmany(connection)
    self.check_fetchall(connection)
    self.check_exectute_many(connection,insert4)

    data = {1000 : [1000, '2020-12-25', 'guangdon', '广州', 1],
        1001 : [1001, '2020-12-26', 'hefei', 'anhui', 2],
        1002 : [1002, '2020-12-27', 'fujian', 'fuzhou', 3],
        1003 : [1003, '2020-12-28', 'jiangxi', 'nanchang', 4],
        1004 : [1004, '2020-12-29', 'hubei', 'wuhan', 5]}
    try:
      connection.execute(insert2, (2, 2))
      self.assertTrue(False)
    except Exception as e:
      pass
    # test select all
    try:
      rs = connection.execute("select * from tsql1010;");
    except Exception as e:
      logging.warning("error occured {}".format(e))
      self.assertTrue(False)
    logging.info("[Execute]: select * from tsql1010;")

    rs = list(rs)
    expectRows = [
        (1000,'2020-12-25', 'guangdon', '广州', 1),
        (1001, '2020-12-26', 'hefei', 'anhui', 2),
        (1002, '2020-12-27', 'fujian', 'fuzhou', 3),
        (1003, '2020-12-28', 'jiangxi', 'nanchang', 4),
        (1004, '2020-12-29', 'hubei', 'wuhan', 5),
    ]
    self.check_result(rs, expectRows, 0);
    # test condition select
    rs = connection.execute("select * from tsql1010 where col3 = 'hefei';");
    logging.info("[Execute]: select * from tsql1010 where col3 = 'hefei';")
    rs = list(rs)
    expectRows = [
        (1001, '2020-12-26', 'hefei', 'anhui', 2)
    ]
    self.check_result(rs, expectRows, 0);
    # test request mode
    rs = connection.execute("select * from tsql1010;", ({"col1":9999, "col2":'2020-12-27', "col3":'zhejiang', "col4":'hangzhou', "col5":100}))
    logging.info("[Request Execute]: select * from tsql1010;")
    rs = list(rs)
    expectRows = [
        (9999, "2020-12-27", "zhejiang", "hangzhou", 100)
    ]
    self.check_result(rs, expectRows, 0);

    # test parameterized query in batch mode
    logging.info("[Execute]: select * from tsql1010 where col3 = ?; ('hefei')")
    rs = connection.execute("select * from tsql1010 where col3 = ?;", ('hefei'))
    rs = list(rs)
    expectRows = [
        (1001, '2020-12-26', 'hefei', 'anhui', 2),
    ]
    self.check_result(rs, expectRows, 0);

    # test storage produce
    try:
      connection.execute("drop procedure sp;")
    except Exception as e:
      pass
    time.sleep(2)
    connection.execute("create procedure sp (col1 bigint, col2 date, col3 string, col4 string, col5 int) begin select * from tsql1010; end;")
    raw_connection = engine.raw_connection()
    mouse = raw_connection.cursor()
    rs = mouse.callproc("sp", ({"col1":1002, "col2":'2020-12-27', "col3":'fujian', "col4":'fuzhou', "col5":3}))
    logging.info("[Execute]: create procedure sp (col1 bigint, col2 date, col3 string, col4 string, col5 int) begin select * from tsql1010; end;")
    self.assertTrue(rs.rowcount == 1)
    for i in range(rs.rowcount):
      i = rs.fetchone()
      logging.info(i)
      if i == None: break
      j = 0
      line = data[i[0]]
      for d in i:
        self.assertTrue(d == line[j])
        j+=1
    try:
      connection.execute("drop procedure sp;")
    except Exception as e:
      pass

     # test batch request mode
    mouse2 = raw_connection.cursor()
    rs = mouse2.batch_row_request("select * from tsql1010;", (), ({"col1":1002, "col2":'2020-12-27', "col3":'fujian', "col4":'fuzhou', "col5":3}))
    for i in range(rs.rowcount):
      i = rs.fetchone()
      logging.info(i)
      if i == None: break
      j = 0
      line = data[i[0]]
      for d in i:
        self.assertTrue(d == line[j])
        j+=1
    mouse3 = raw_connection.cursor()
    rs = mouse3.batch_row_request("select * from tsql1010;", (), ({"col1":1002, "col2":'2020-12-27', "col3":'fujian', "col4":'fuzhou', "col5":3}, {"col1":1003, "col2":"2020-12-28", "col3":"jiangxi", "col4":"nanchang", "col5":4}))
    for i in range(rs.rowcount):
      i = rs.fetchone()
      logging.info(i)
      if i == None: break
      j = 0
      line = data[i[0]]
      for d in i:
        self.assertTrue(d == line[j])
        j+=1
  
  def execute_insert_sqls(self, connection, sqls):
    for sql in sqls:
      try:
        connection.execute(sql);
      except Exception as e:
        self.assertTrue(False)
 
  def check_has_table(self, connection):
    try:
      if not connection.dialect.has_table(connection, 'tsql1010', schema=None):
        logging.info('check table name that already exists')
        self.assertTrue(False)
      if connection.dialect.has_table(connection, 'testsql1', schema=None):
        logging.info('check new table name')
        self.assertTrue(False)
    except:
      pass

  def show_result_list(self, rs):
    logging.info("result size: %d", len(rs))
    for row in rs:
      logging.info(row)

  def check_result(self, rs, expectRows, orderIdx = 0):
    rs = sorted(rs, key=lambda x: x[orderIdx])
    expectRows = sorted(expectRows, key=lambda x: x[orderIdx])
    self.show_result_list(rs)
    self.assertEqual(len(rs), len(expectRows))
    i = 0;
    for row in rs:
      self.assertEqual(row, expectRows[i], "not equal row: {}\n{}".format(row, expectRows[i]))
      i+=1

  def check_exectute_many(self,connection,sql):
    try:
      connection.execute(sql,[{"col1":1005, "col2":"2020-12-29", "col3":"shandong", "col4":"jinan", "col5":6},
                              {"col1":1006, "col2":"2020-12-30", "col3":"fujian", "col4":"fuzhou", "col5":7}]);
      self.assertTrue(False)
    except Exception as e:
      pass

  def check_fetchmany(self,connection):
    try:
      result = connection.execute("select * from tsql1010;")
      print(result.fetchmany(size=2))
      self.assertTrue(False)
    except Exception as e:
      pass
      
  def check_fetchall(self,connection):
    try:
      result = connection.execute("select * from tsql1010;")
      print(result.fetchall())
      self.assertTrue(False)
    except Exception as e:
      pass

  def test_parameterized_query(self):
    logging.info("test_parameterized_query...")
    engine = db.create_engine('openmldb:///db_test?zk=127.0.0.1:6181&zkPath=/onebox')
    connection = engine.connect()
    try:
      connection.execute("create database db_test;")
    except Exception as e:
      pass
    try:
      logging.info("drop table tsql1010;")
      connection.execute("drop table tsql1010;")
    except Exception as e:
      pass

    time.sleep(2)

    ddl = "create table tsql1010 ( col1 bigint, col2 date, col3 string, col4 string, col5 int, col6 timestamp, index(key=col3, ts=col1), index(key=col3, ts=col6));"
    connection.execute(ddl)
    insert_sqls = [
      "insert into tsql1010 values(1000, '2020-12-25', 'province1', 'city1', 1, 1590738990000);",
      "insert into tsql1010 values(1001, '2020-12-26', 'province1', 'city2', 2, 1590738991000);",
      "insert into tsql1010 values(1002, '2020-12-27', 'province1', 'city3', 3, 1590738992000);",
      "insert into tsql1010 values(1003, '2020-12-28', 'province2', 'city4', 4, 1590738993000);",
      "insert into tsql1010 values(1004, '2020-12-29', 'province2', 'city5', 5, 1590738994000);",
      "insert into tsql1010 values(1005, '2020-12-30', 'province2', 'city6', 6, 1590738995000);",
      "insert into tsql1010 values(1006, '2020-12-31', 'province3', 'city7', 7, 1590738996000);",
      "insert into tsql1010 values(1007, '2021-01-01', 'province3', 'city8', 8, 1590738997000);",
      "insert into tsql1010 values(1008, '2021-01-02', 'province3', 'city9', 9, 1590738998000);",
      "insert into tsql1010 values(1009, '2021-01-03', 'province3', 'city10', 10, 1590738999000);"
    ]
    self.execute_insert_sqls(connection, insert_sqls)

    # test parameterized query in batch mode case 1
    logging.info("[Execute]: select * from tsql1010 where col3 = ?; ('province1')")
    rs = connection.execute("select * from tsql1010 where col3 = ?;", ('province1'))
    rs = list(rs);
    expectRows = [
      (1000, '2020-12-25', 'province1', 'city1', 1, 1590738990000),
      (1001, '2020-12-26', 'province1', 'city2', 2, 1590738991000),
      (1002, '2020-12-27', 'province1', 'city3', 3, 1590738992000),
      ]
    self.check_result(rs, expectRows)
    
    # test parameterized query in batch mode case 2
    logging.info("[Execute]: select * from tsql1010 where col3 = ?; ('province2')")
    rs = connection.execute("select * from tsql1010 where col3 = ?;", ('province2'))
    rs = list(rs);
    expectRows = [
      (1003, '2020-12-28', 'province2', 'city4', 4, 1590738993000),
      (1004, '2020-12-29', 'province2', 'city5', 5, 1590738994000),
      (1005, '2020-12-30', 'province2', 'city6', 6, 1590738995000),
      ]
    self.check_result(rs, expectRows)

    # test parameterized query in batch mode case 3
    logging.info("[Execute]: select * from tsql1010 where col3 = ?; ('province3')")
    rs = connection.execute("select * from tsql1010 where col3 = ?;", ('province3'))
    rs = list(rs)
    expectRows = [
      (1006, '2020-12-31', 'province3', 'city7', 7, 1590738996000),
      (1007, '2021-01-01', 'province3', 'city8', 8, 1590738997000),
      (1008, '2021-01-02', 'province3', 'city9', 9, 1590738998000),
      (1009, '2021-01-03', 'province3', 'city10', 10, 1590738999000),
      ]
    self.check_result(rs, expectRows)

    # test parameterized query in batch mode case 3
    logging.info("[Execute]: select * from tsql1010 where col3 = ? and col1 < ?; ('province3', 1008)")
    rs = connection.execute("select * from tsql1010 where col3 = ? and col1 < ?;", ('province3', 1008))
    rs = list(rs);
    expectRows = [
      (1006, '2020-12-31', 'province3', 'city7', 7, 1590738996000),
      (1007, '2021-01-01', 'province3', 'city8', 8, 1590738997000),
      ]
    self.check_result(rs, expectRows)

    # test parameterized query in batch mode case 4
    logging.info("[Execute]: select * from tsql1010 where col3 = ? and col1 < ? and col2 < ?; ('province3', 1008, date.fromisoformat('2021-01-01'))")
    rs = connection.execute("select * from tsql1010 where col3 = ? and col1 < ? and col2 < ?;", ('province3', 1008, date.fromisoformat('2021-01-01')))
    rs = list(rs);
    expectRows = [
      (1006, '2020-12-31', 'province3', 'city7', 7, 1590738996000),
      ]
    self.check_result(rs, expectRows)

    # test parameterized query in batch mode case 5
    logging.info("[Execute]: select * from tsql1010 where col3 = ? and col6 < ?; ('province3', datetime.fromtimestamp(1590739000000))")
    rs = connection.execute("select * from tsql1010 where col3 = ? and col6 < ?;", ('province3', datetime.fromtimestamp(1590738999.000)))
    rs = list(rs);
    expectRows = [
      (1006, '2020-12-31', 'province3', 'city7', 7, 1590738996000),
      (1007, '2021-01-01', 'province3', 'city8', 8, 1590738997000),
      (1008, '2021-01-02', 'province3', 'city9', 9, 1590738998000),
      ]
    self.check_result(rs, expectRows)
 



if __name__ == '__main__':
    unittest.main()

