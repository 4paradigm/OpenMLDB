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

package com._4paradigm.sql.jmh.voltdb;

import com._4paradigm.sql.BenchmarkConfig;
import com._4paradigm.sql.jmh.DatabaseSetup;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

@Slf4j
public class VoltdbSetup implements DatabaseSetup {
    public Connection connection;

    @Override
    public void teardown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Override
    public void setup() throws SQLException {
        try {
            Class.forName("org.voltdb.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            log.error("no voltdb driver found", e);
            throw new SQLException(e);
        }
        connection = DriverManager.getConnection(BenchmarkConfig.VOLTDB_URL);
    }
}
