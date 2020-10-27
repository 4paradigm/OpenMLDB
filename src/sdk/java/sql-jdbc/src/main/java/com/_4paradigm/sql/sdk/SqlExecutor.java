package com._4paradigm.sql.sdk;

import com._4paradigm.sql.ResultSet;
import com._4paradigm.sql.SQLRequestRow;
import com._4paradigm.sql.SQLInsertRow;
import com._4paradigm.sql.SQLInsertRows;
import com._4paradigm.sql.jdbc.SQLResultSet;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public interface SqlExecutor {
    boolean createDB(String db);
    boolean dropDB(String db);
    boolean executeDDL(String db, String sql);
    boolean executeInsert(String db, String sql);
    boolean executeInsert(String db, String sql, SQLInsertRow row);
    boolean executeInsert(String db, String sql, SQLInsertRows rows);
    ResultSet executeSQL(String db, String sql);
//    SQLRequestRow getRequestRow(String db, String sql);
    SQLInsertRow getInsertRow(String db, String sql);
    PreparedStatement getInsertPreparedStmt(String db, String sql);
    PreparedStatement getRequestPreparedStmt(String db, String sql) throws SQLException;
    SQLInsertRows getInsertRows(String db, String sql);
    ResultSet executeSQL(String db, String sql, SQLRequestRow row);

    Schema getInputSchema(String dbName, String sql) throws SQLException;
    SQLResultSet callProcedure(String dbName, String proName, Object[][] requestRows) throws SQLException;
    SQLResultSet callProcedure(String dbName, String proName, Object[] requestRow) throws SQLException;
    boolean dropProcedure(String dbName, String proName) throws SQLException;
    ProcedureInfo showProcedure(String dbName, String proName) throws SQLException;
}
