package com._4paradigm.sql.sdk;

import com._4paradigm.sql.ResultSet;
import com._4paradigm.sql.SQLRequestRow;
import com._4paradigm.sql.SQLInsertRow;
import com._4paradigm.sql.SQLInsertRows;

import java.sql.PreparedStatement;

public interface SqlExecutor {
    boolean createDB(String db);
    boolean dropDB(String db);
    boolean executeDDL(String db, String sql);
    boolean executeInsert(String db, String sql);
    boolean executeInsert(String db, String sql, SQLInsertRow row);
    boolean executeInsert(String db, String sql, SQLInsertRows rows);
    ResultSet executeSQL(String db, String sql);
    SQLRequestRow getRequestRow(String db, String sql);
    SQLInsertRow getInsertRow(String db, String sql);
    PreparedStatement getInsertPrepareStmt(String db, String sql);
    PreparedStatement getRequestPrepareStmt(String db, String sql);
    SQLInsertRows getInsertRows(String db, String sql);
    ResultSet executeSQL(String db, String sql, SQLRequestRow row);
}
