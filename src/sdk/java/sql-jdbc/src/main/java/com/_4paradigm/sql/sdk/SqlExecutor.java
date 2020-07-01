package com._4paradigm.sql.sdk;

import com._4paradigm.sql.ResultSet;
import com._4paradigm.sql.SQLRequestRow;

public interface SqlExecutor {
    boolean createDB(String db);
    boolean executeDDL(String db, String sql);
    boolean executeInsert(String db, String sql);
    boolean executeInsert(String db, String sql, Object[] data);
    ResultSet executeSQL(String db, String sql);
    SQLRequestRow getRequestRow(String db, String sql);
    ResultSet executeSQL(String db, String sql, SQLRequestRow row);
}
