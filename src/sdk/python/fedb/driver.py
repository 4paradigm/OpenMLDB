#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
#

import logging
from . import sql_router_sdk
logger = logging.getLogger("fedb_driver")
class DriverOptions(object):
    def __init__(self, zk_cluster, zk_path, session_timeout = 3000):
        self.zk_cluster = zk_cluster
        self.zk_path = zk_path
        self.session_timeout = session_timeout

class Driver(object):
    def __init__(self, options):
        self.options = options
        self.sdk = None

    def init(self):
        options = sql_router_sdk.SQLRouterOptions()
        options.zk_cluster = self.options.zk_cluster
        options.zk_path = self.options.zk_path
        self.sdk = sql_router_sdk.NewClusterSQLRouter(options)
        if not self.sdk:
            logger.error("fail to init fedb driver with zk cluster %s and zk path %s"%(options.zk_cluster, options.zk_path))
            return False
        logger.info("init fedb driver done with zk cluster %s and zk path %s"%(options.zk_cluster, options.zk_path))
        return True

    def createDB(self, db):
        if not self.sdk:
            return False, "please init driver first"
        status = sql_router_sdk.Status()
        if self.sdk.CreateDB(db, status):
            return True, "ok"
        else:
            return False, status.msg

    def dropDB(self, db):
        if not self.sdk:
            return False, "please init driver first"
        status = sql_router_sdk.Status()
        if self.sdk.DropDB(db, status):
            return True, "ok"
        else:
            return False, status.msg

    def executeDDL(self, db, ddl):
        if not self.sdk:
            return False, "please init driver first"
        status = sql_router_sdk.Status()
        if not self.sdk.ExecuteDDL(db, ddl, status):
            return False, status.msg
        else:
            self.sdk.RefreshCatalog()
            return True, "ok"

    def getInsertBuilder(self, db, sql):
        if not self.sdk:
            return False, "please init driver first"
        status = sql_router_sdk.Status()
        row_builder = self.sdk.GetInsertRow(db, sql, status)
        if not row_builder:
            return False, status.msg
        return True, row_builder
    
    def getInsertBatchBuilder(self, db, sql):
        if not self.sdk:
            return False, "please init driver first"
        status = sql_router_sdk.Status()
        rows_builder = self.sdk.GetInsertRows(db, sql, status)
        if not rows_builder:
            return False, status.msg
        return True, rows_builder

    def executeInsert(self, db, sql, row_builder = None):
        if not self.sdk:
            return False, "please init driver first"
        status = sql_router_sdk.Status()
        if row_builder:
            if self.sdk.ExecuteInsert(db, sql, row_builder, status):
                return True, "ok"
            else:
                return False, status.msg
        else:
            if self.sdk.ExecuteInsert(db, sql, status):
                return True, "ok"
            else:
                return False, status.msg

    def getRequestBuilder(self, db, sql):
        if not self.sdk:
            return False, "please init driver first"
        status = sql_router_sdk.Status()
        row_builder = self.sdk.GetRequestRow(db, sql, status)
        if not row_builder:
            return False, status.msg
        return True, row_builder

    def executeQuery(self, db, sql, row_builder = None):
        if not self.sdk:
            return False, "please init driver first"

        status = sql_router_sdk.Status()
        if row_builder:
            rs = self.sdk.ExecuteSQL(db, sql, row_builder, status)
            if not rs:
                return False, status.msg
            else:
                return True, rs
        else:
            rs = self.sdk.ExecuteSQL(db, sql, status)
            if not rs:
                return False, status.msg
            else:
                return True, rs

    def getRowBySp(self, db, sp):
        status = sql_router_sdk.Status()
        row_builder = self.sdk.GetRequestRowByProcedure(db, sp, status)
        if not row_builder:
            return False, status.msg
        return True, row_builder
   
    def callProc(self, db, sp, rq):
        status = sql_router_sdk.Status()
        rs = self.sdk.CallProcedure(db, sp, rq, status)
        if not rs:
            return False, status.msg
        return True, rs

