#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
"""

import fesql

dbms_endpoint="127.0.0.1:9211"

def exec_query(dbms, db, sql):
    if not dbms:
        return False, None, None
    status = fesql.Status();
    rs = dbms.ExecuteQuery(db, sql, status)
    if status.code == 0:
        return True, None, rs
    return False, status.msg, None

