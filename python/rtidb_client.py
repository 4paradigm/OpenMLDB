#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# rtidb_client.py
# Copyright (C) 2017 4paradigm.com
# Author vagrant
# Date 2017-04-25
#


from ctypes import CDLL, byref
from ctypes import c_char_p,c_int, c_uint32, c_ulong,  string_at

rtidb_so = CDLL("./librtidb_py.so")

class RtidbClient(object):
  def __init__(self, endpoint):
    self.db_ = rtidb_so.NewClient(endpoint)

  def create_table(self, name, tid, pid, ttl, data_ha = False):
    return rtidb_so.CreateTable(self.db_, name, tid, pid, ttl, data_ha)

  def put(self, tid, pid, pk, time, value):
    return rtidb_so.Put(self.db_, tid, pid, pk, time, value)

  def scan(self, tid, pid, pk, stime, etime):
    return KvIterator(rtidb_so.Scan(self.db_, tid, pid, pk, stime, etime))

  def drop_table(self, tid):
    return rtidb_so.DropTable(self.db_, tid)

  def __del__(self):
    rtidb_so.FreeClient(self.db_)


class KvIterator(object):
  """
  the usage 
  it = rtidb_client.scan()
  while (it.valid()):
      it.next()
      it.get_key()
      it.get_value()

  """
  def __init__(self, it):
    self.it_ = it

  def valid(self):
    return rtidb_so.IteratorValid(self.it_)

  def next(self):
    rtidb_so.IteratorNext(self.it_)

  def get_key(self):
    key = c_ulong()
    rtidb_so.IteratorGetKey(self.it_, byref(key))
    return key.value

  def get_value(self):
    size = c_int()
    buf = c_char_p()
    rtidb_so.IteratorGetValue(self.it_, byref(size), byref(buf))
    value =  string_at(buf, size)
    rtidb_so.FreeString(buf)
    return value

  def __del__(self):
    rtidb_so.IteratorFree(self.it_)
