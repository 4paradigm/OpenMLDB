#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# rtidb_client_test.py
# Copyright (C) 2017 4paradigm.com
# Author vagrant
# Date 2017-04-25
#
import unittest

import rtidb_client

endpoint = "127.0.0.1:9501"

class TestRtidbClient(unittest.TestCase):
  
  def test_create_table(self):
    db = rtidb_client.RtidbClient(endpoint)
    ok = db.create_table("py_test1", 101, 1, 0)
    self.assertTrue(ok)
    ok = db.create_table("py_test1", 101, 1, 0)
    self.assertFalse(ok)
    ok = db.create_table("", 119, 1, 0)
    self.assertFalse(ok)


  def test_put(self):
    db = rtidb_client.RtidbClient(endpoint)
    ok = db.create_table("py_test2", 102, 1, 0)
    self.assertTrue(ok)
    ok = db.put(102, 1, "pk", 9527, "pk")
    self.assertTrue(ok)
    ok = db.put(102, 1, "pk", 9527, "pk")
    self.assertTrue(ok)
    ok = db.put(102, 1, "pk", 9527, 9527)
    self.assertTrue(ok)
    ok = db.put(201, 1, "pk", 9527, "pk")
    self.assertFalse(ok)

  def test_scan(self):
    db = rtidb_client.RtidbClient(endpoint)
    ok = db.create_table("py_test3", 103, 1, 0)
    self.assertTrue(ok)
    ok = db.put(103, 1, "pk", 9527, "pk1")
    self.assertTrue(ok)
    ok = db.put(103, 1, "pk", 9528, "pk2")
    self.assertTrue(ok)

    it = db.scan(103, 1, "pk", 9528, 9527)
    self.assertTrue(it.valid())
    self.assertEqual(9528, it.get_key())
    self.assertEqual("pk2", it.get_value())
    it.next()
    self.assertFalse(it.valid())
  def test_scan_no_table(self):
    db = rtidb_client.RtidbClient(endpoint)
    it = db.scan(303, 1, "pk", 9528, 9527)
    self.assertFalse(it.valid())

  def test_drop_table(self):
    db = rtidb_client.RtidbClient(endpoint)
    ok = db.drop_table(104)
    self.assertFalse(ok)
    ok = db.create_table("py_test3", 105, 1, 0)
    self.assertTrue(ok)
    ok = db.drop_table(105)
    self.assertTrue(ok)


if __name__ == '__main__':
    unittest.main()




