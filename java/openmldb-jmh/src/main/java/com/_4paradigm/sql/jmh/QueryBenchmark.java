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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * general case for query benchmark
 *  require a set of tables & data, and query string
 */
public interface QueryBenchmark {
    void prepareData() throws SQLException;

    ResultSet query() throws SQLException;

    void cleanup() throws SQLException;

    default String getTableName() {
        return "table_" + this.getClass().getSimpleName();
    }

    Connection getConnection();
}
