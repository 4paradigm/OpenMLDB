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

import sqlalchemy as db

ddl = "create table tsql1010 ( col1 bigint, col2 date, col3 string, col4 string, col5 int, index(key=col3, ts=col1));"
logging.basicConfig(level=logging.INFO)
class TestRtidbClient(unittest.TestCase):
  
  def show(self, rs):
    logging.info("Dataset size: %d", rs.rowcount)
    for i in rs:
      logging.info(i);

  def test_basic(self):
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
    self.assertTrue(rs.rowcount == 5)
    self.show(rs);
    for i in rs:
      j = 0
      line = data[i[0]]
      for d in i:
        self.assertTrue(d == line[j])
        j+=1
    # test condition select
    rs = connection.execute("select * from tsql1010 where col3 = 'hefei';");
    logging.info("[Execute]: select * from tsql1010 where col3 = 'hefei';")
    self.show(rs);
    for i in rs:
      j = 0
      line = data[i[0]]
      for d in i:
        self.assertTrue(d == line[j])
        j+=1
    # test request mode
    rs = connection.execute("select * from tsql1010;", ({"col1":9999, "col2":'2020-12-27', "col3":'zhejiang', "col4":'hangzhou', "col5":100}))
    logging.info("[Request Execute]: select * from tsql1010;")
    self.show(rs);
    self.assertTrue(rs.rowcount == 1) 
    for row in rs:
      self.assertEqual(row, (9999, "2020-12-27", "zhejiang", "hangzhou", 100))
      break

    # test parameterized query in batch mode
    logging.info("[Execute]: select * from tsql1010 where col3 = ?; ('hefei')")
    rs = connection.execute("select * from tsql1010 where col3 = ?;", ('hefei'))
    self.show(rs);
    self.assertTrue(rs.rowcount == 1) 
    for row in rs:
      self.assertEqual(row, Tuple(1001, '2020-12-26', 'hefei', 'anhui', 2))
      break

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

  def check_result(self, rs, expectRows):
    self.assertEqual(rs.rowcount, len(expectRows))
    i = 0;
    for row in rs:
      self.assertEqual(row, expectRows[i])
      i+=1
    
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

    connection.execute(ddl)
    insert_sqls = [
      "insert into tsql1010 values(1000, '2020-12-25', 'province1', 'city1', 1);",
      "insert into tsql1010 values(1001, '2020-12-26', 'province1', 'city2', 2);",
      "insert into tsql1010 values(1002, '2020-12-27', 'province1', 'city3', 3);",
      "insert into tsql1010 values(1003, '2020-12-28', 'province2', 'city4', 4);",
      "insert into tsql1010 values(1004, '2020-12-29', 'province2', 'city5', 5);",
      "insert into tsql1010 values(1005, '2020-12-30', 'province2', 'city6', 6);",
      "insert into tsql1010 values(1006, '2020-12-31', 'province3', 'city7', 7);",
      "insert into tsql1010 values(1007, '2021-01-01', 'province3', 'city8', 8);",
      "insert into tsql1010 values(1008, '2021-01-02', 'province3', 'city9', 9);",
      "insert into tsql1010 values(1009, '2021-01-03', 'province3', 'city10', 10);"
    ]
    self.execute_insert_sqls(connection, insert_sqls)

    # test parameterized query in batch mode case 1
    logging.info("[Execute]: select * from tsql1010 where col3 = ?; ('province1')")
    rs = connection.execute("select * from tsql1010 where col3 = ?;", ('province1'))
    self.show(rs);
    expectRows = [
      (1000, '2020-12-25', 'province1', 'city1', 1),
      (1001, '2020-12-26', 'province1', 'city2', 2),
      (1002, '2020-12-27', 'province1', 'city3', 3),
      ]
    self.check_result(rs, expectRows)
    
    # test parameterized query in batch mode case 2
    logging.info("[Execute]: select * from tsql1010 where col3 = ?; ('province2')")
    rs = connection.execute("select * from tsql1010 where col3 = ?;", ('province2'))
    self.show(rs);
    expectRows = [
      (1003, '2020-12-28', 'province2', 'city4', 4),
      (1004, '2020-12-29', 'province2', 'city5', 5),
      (1005, '2020-12-30', 'province2', 'city6', 6)
      ]
    self.check_result(rs, expectRows)

    # test parameterized query in batch mode case 3
    logging.info("[Execute]: select * from tsql1010 where col3 = ?; ('province3')")
    rs = connection.execute("select * from tsql1010 where col3 = ?;", ('province3'))
    self.show(rs);
    expectRows = [
      (1006, '2020-12-31', 'province3', 'city7', 7),
      (1007, '2021-01-01', 'province3', 'city8', 8),
      (1008, '2021-01-02', 'province3', 'city9', 9),
      (1009, '2021-01-03', 'province3', 'city10', 10)
      ]
    self.check_result(rs, expectRows)

    # test parameterized query in batch mode case 3
    logging.info("[Execute]: select * from tsql1010 where col3 = ? and col1 < ?; ('province3', 1008)")
    rs = connection.execute("select * from tsql1010 where col3 = ? and col1 < ?;", ('province3', 1008))
    self.show(rs);
    expectRows = [
      (1006, '2020-12-31', 'province3', 'city7', 7),
      (1007, '2021-01-01', 'province3', 'city8', 8),
      ]
    self.check_result(rs, expectRows)



if __name__ == '__main__':
    unittest.main()

