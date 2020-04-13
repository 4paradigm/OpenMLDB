#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
#

"""

"""

from test_base import *
import fesql_interface


def test_dbms_create_db():
    dbms = fesql_interface.CreateDBMSSdk(dbms_endpoint)
    cases= [("ns1", 0), ('ns2', 0)]
    for case in cases:
        yield run_db_check, dbms, case[0], case[1]

def run_db_check(dbms, ns, result):
    assert dbms
    status = fesql_interface.Status()
    dbms.CreateDatabase(ns, status)
    assert status.code == result

def test_dbms_valid():
    dbms = fesql_interface.CreateDBMSSdk("xxxxx")
    assert not dbms
