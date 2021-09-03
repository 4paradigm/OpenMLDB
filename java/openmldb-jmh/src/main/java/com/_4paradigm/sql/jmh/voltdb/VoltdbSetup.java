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
