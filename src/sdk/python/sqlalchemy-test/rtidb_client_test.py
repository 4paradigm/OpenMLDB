#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
#
import unittest
import logging
import time

import sqlalchemy as db

ddl = "create table tsql1010 ( col1 bigint, col2 date, col3 string, col4 string, col5 int, index(key=col3, ts=col1));"

class TestRtidbClient(unittest.TestCase):
  
  def test_basic(self):
    engine = db.create_engine('fedb:///db_test?zk=127.0.0.1:6181&zkPath=/onebox')
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
    self.assertTrue(rs.rowcount == 5)
    for i in rs:
      j = 0
      line = data[i[0]]
      for d in i:
        self.assertTrue(d == line[j])
        j+=1
    # test condition select
    rs = connection.execute("select * from tsql1010 where col3 = 'hefei';");
    for i in rs:
      j = 0
      line = data[i[0]]
      for d in i:
        self.assertTrue(d == line[j])
        j+=1
    # test request mode
    rs = connection.execute("select * from tsql1010;", ({"col1":1002, "col2":'2020-12-27', "col3":'fujian', "col4":'fuzhou', "col5":3}))
    for i in rs:
      j = 0
      line = data[i[0]]
      for d in i:
        self.assertTrue(d == line[j])
        j+=1
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
    self.assertTrue(rs.rowcount == 1)
    for i in range(rs.rowcount):
      i = rs.fetchone()
      if i == None: break
      j = 0
      line = data[i[0]]
      for d in i:
        self.assertTrue(d == line[j])
        j+=1
     # test batch request mode
    mouse2 = raw_connection.cursor()
    rs = mouse2.batch_row_request("select * from tsql1010;", (), ({"col1":1002, "col2":'2020-12-27', "col3":'fujian', "col4":'fuzhou', "col5":3}))
    for i in range(rs.rowcount):
      i = rs.fetchone()
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
      if i == None: break
      j = 0
      line = data[i[0]]
      for d in i:
        self.assertTrue(d == line[j])
        j+=1


if __name__ == '__main__':
    unittest.main()

