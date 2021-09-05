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
 * parameterized query benchmark where have two filter conditions
 */
public interface ParameterizedQuery2Benchmark extends QueryBenchmark {
    String ddl = "create table %s (col1 int, col2 int, " +
            "col3 timestamp," +
            "col4 float," +
            "col5 varchar(128)," +
            "primary key (col1));";
    String insertValues = " values (%d, %d, %d, 200.0, 'hello world');";

    int param1 = 500;
    int param2 = 40;

    default String getDDL() {
        return String.format(ddl, getTableName());
    }

    /**
     * number of records pre-inserted in the table
     * @return
     */
    default int getRecordSize() {
        return 10000;
    }

    default String getInsertStmt() {
        return String.format("insert into %s %s", getTableName(), insertValues);
    }

    default String getQuery() {
        return String.format("select col1, col2, col3, col4, col5 from %s where col1 > ? and col2 = ?;", getTableName());
    }

    @Override
    default void prepareData() throws SQLException {
        try (Statement createStmt = getConnection().createStatement()) {
            createStmt.execute(getDDL());
            // create a non-unique index for col3
            createStmt.execute(String.format("create index col3_index on %s (%s)",  getTableName(), " col2"));
        }

        try (Statement insertStmt = getConnection().createStatement()) {
            for (int i = 0; i < getRecordSize(); i++) {
                int val = i % 100;
                String sql = String.format(getInsertStmt(), i, val, System.currentTimeMillis());
                insertStmt.execute(sql);
            }
        }
    }

    @Override
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
            stmt.setInt(1, param1);
            stmt.setInt(2, param2);
            return stmt.executeQuery();
        }
    }
}
