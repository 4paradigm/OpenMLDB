#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
#

"""

"""

from test_base import *
import time
import fesql

def run_db_check(dbms, ns, result):
    assert dbms
    status = fesql.Status()
    dbms.CreateDatabase(ns, status)
    assert status.code == result

def test_dbms_create_db():
    dbms = fesql.CreateDBMSSdk(dbms_endpoint)
    cases= [("ns1" + str(time.time()) , 0), ('ns2' + str(time.time()), 0)]
    for case in cases:
        yield run_db_check, dbms, case[0], case[1]

def test_dbms_valid():
    dbms = fesql.CreateDBMSSdk("xxxxx")
    assert not dbms


