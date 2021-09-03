package com._4paradigm.sql.jmh.openmldb;

import com._4paradigm.openmldb.sdk.SdkOption;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;
import com._4paradigm.sql.BenchmarkConfig;
import com._4paradigm.sql.jmh.DatabaseSetup;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;

@Slf4j
public class OpenMLDBSetup implements DatabaseSetup {
    public SqlExecutor executor;

    public String getDb() {
        return "db_" + this.getClass().getSimpleName();
    }

    @Override
    public void setup() throws SQLException {
        SdkOption sdkOption = new SdkOption();
        sdkOption.setSessionTimeout(30000);
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
}
