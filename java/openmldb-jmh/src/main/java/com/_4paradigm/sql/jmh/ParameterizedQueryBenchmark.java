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

package com._4paradigm.sql.jmh;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * provide a set of rules of one benchmark case
 */
public interface ParameterizedQueryBenchmark extends QueryBenchmark {
    String param1 = "pk-2-55";

    default int getRecordSize() {
        return 10000;
    }

    default String getInsertStmt() {
        String insertValues = "(col1, col2, col3, col4, col5) values ('%s', %d, 100.0, 200.0, 'hello world');";
        return String.format("insert into %s %s", getTableName(), insertValues);
    }

    /**
     * ddl string to create the new table, useful in {@link DatabaseSetup#setup}
     *  remember to String.format with a table name
     * @return ddl
     */
    default String getDDL() {
        String ddl = "create table %s (col1 varchar(128), col2 timestamp, " +
                "col3 float," +
                "col4 float," +
                "col5 varchar(128)," +
                "primary key (col1));";
        return String.format(ddl, getTableName());
    }

    default String getQuery() {
        return String.format( "SELECT col1, col2, col3, col4, col5 FROM %s WHERE col1=?", getTableName());
    }

    default void prepareData() throws SQLException {
        try (Statement createStmt = getConnection().createStatement()) {
            createStmt.execute(getDDL());
        }

        try (Statement insertStmt = getConnection().createStatement()) {
            for (int i = 0; i < getRecordSize() / 1000; i++) {
                for (int j = 0; j < 1000; j++) {
                    String sql = String.format(getInsertStmt(), String.format("pk-%d-%d", i, j), System.currentTimeMillis());
                    insertStmt.execute(sql);
                }
            }
        }
    }

    /**
     * delete prepared data and inserted tables
     * @throws SQLException
     */
    default void cleanup() throws SQLException {
        try (PreparedStatement stmt = getConnection().prepareStatement(getQuery())) {
            stmt.execute(getCleanDDL());
        }
    }

    default String getCleanDDL() {
        return String.format("drop table %s", getTableName());
    }

    default ResultSet query() throws SQLException {
        try (PreparedStatement stmt = getConnection().prepareStatement(getQuery())) {
            stmt.setString(1, param1);
            return stmt.executeQuery();
        }
    }
}
