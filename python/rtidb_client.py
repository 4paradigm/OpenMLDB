#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# rtidb_client.py
# Copyright (C) 2017 4paradigm.com
# Author vagrant
# Date 2017-04-25
#

import os
from ctypes import CDLL, byref
from ctypes import c_char_p,c_int, c_uint32, c_ulong,  string_at

dir_path = os.path.dirname(os.path.realpath(__file__))
rtidb_so = CDLL(os.path.join(dir_path ,"librtidb_py.so"))

class RtidbClient(object):
  def __init__(self, endpoint):
    self.db_ = rtidb_so.NewClient(endpoint)

  def create_table(self, name, tid, pid, ttl, data_ha = False):
    if rtidb_so.CreateTable(self.db_, name, tid, pid, ttl, data_ha):
      return True
    return False

  def put(self, tid, pid, pk, time, value):
    if type(value) is not str:
      if rtidb_so.Put(self.db_, tid, pid, pk, time, str(value)):
        return True
    else:
      if rtidb_so.Put(self.db_, tid, pid, pk, time, value):
        return True
    return False

  def scan(self, tid, pid, pk, stime, etime):
    return KvIterator(rtidb_so.Scan(self.db_, tid, pid, pk, stime, etime))

  def drop_table(self, tid):
    if rtidb_so.DropTable(self.db_, tid):
      return True
    return False

  def __del__(self):
    rtidb_so.FreeClient(self.db_)


class KvIterator(object):
  """
  the usage 
  it = rtidb_client.scan()
  while (it.valid()):
      it.get_key()
      it.get_value()
      it.next()

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
