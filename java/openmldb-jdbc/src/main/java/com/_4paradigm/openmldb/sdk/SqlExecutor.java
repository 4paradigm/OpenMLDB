/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com._4paradigm.openmldb.sdk;

import com._4paradigm.openmldb.*;
import com._4paradigm.openmldb.jdbc.CallablePreparedStatement;
import com._4paradigm.openmldb.proto.NS;

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface SqlExecutor {
    @Deprecated
    boolean createDB(String db);

    @Deprecated
    boolean dropDB(String db);

    @Deprecated
    boolean executeDDL(String db, String sql);

    @Deprecated
    boolean executeInsert(String db, String sql);

    boolean executeInsert(String db, String sql, SQLInsertRow row);

    boolean executeInsert(String db, String sql, SQLInsertRows rows);

    TableReader getTableReader();

    @Deprecated
    java.sql.ResultSet executeSQL(String db, String sql);

    @Deprecated
    SQLInsertRow getInsertRow(String db, String sql);

    @Deprecated
    SQLInsertRows getInsertRows(String db, String sql);

    @Deprecated
    ResultSet executeSQLRequest(String db, String sql, SQLRequestRow row);

    Statement getStatement();

    PreparedStatement getInsertPreparedStmt(String db, String sql) throws SQLException;

    PreparedStatement getDeletePreparedStmt(String db, String sql) throws SQLException;

    PreparedStatement getRequestPreparedStmt(String db, String sql) throws SQLException;

    PreparedStatement getPreparedStatement(String db, String sql) throws SQLException;

    PreparedStatement getBatchRequestPreparedStmt(String db, String sql,
                                                  List<Integer> commonColumnIndices) throws SQLException;

    CallablePreparedStatement getCallablePreparedStmt(String db, String deploymentName) throws SQLException;

    CallablePreparedStatement getCallablePreparedStmtBatch(String db, String deploymentName) throws SQLException;

    Schema getInputSchema(String dbName, String sql) throws SQLException;

    Schema getTableSchema(String dbName, String tableName) throws SQLException;

    ProcedureInfo showProcedure(String dbName, String proName) throws SQLException;

    NS.TableInfo getTableInfo(String db, String table);

    List<String> getTableNames(String db);

    void close();
}
