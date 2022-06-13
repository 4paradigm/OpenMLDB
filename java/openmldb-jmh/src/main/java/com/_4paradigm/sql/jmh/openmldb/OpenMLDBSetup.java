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

package com._4paradigm.sql.jmh.openmldb;

import com._4paradigm.openmldb.sdk.SdkOption;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;
import com._4paradigm.sql.BenchmarkConfig;
import com._4paradigm.sql.jmh.DatabaseSetup;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.SQLException;

@Slf4j
public class OpenMLDBSetup implements DatabaseSetup {
    public SqlExecutor executor;

    @Override
    public void setup() throws SQLException {
        SdkOption sdkOption = new SdkOption();
        sdkOption.setZkSessionTimeout(30000);
        sdkOption.setZkCluster(BenchmarkConfig.ZK_CLUSTER);
        sdkOption.setZkPath(BenchmarkConfig.ZK_PATH);
        try {
            executor = new SqlClusterExecutor(sdkOption);
        } catch (com._4paradigm.openmldb.sdk.SqlException e) {
            log.error("failed to create new SqlClusterExecutor", e);
            throw new SQLException(e.getLocalizedMessage());
        }

        if (!executor.createDB(getDb())) {
            throw new SQLException("failed to create db");
        }
        log.info("created db {}", getDb());
    }

    @Override
    public void teardown() throws SQLException {
        if (executor != null) {
            executor.dropDB(getDb());
            executor.close();
        }
    }

    @Override
    public Connection getConnection() {
        return null;
    }
}
