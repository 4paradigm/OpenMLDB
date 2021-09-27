package com._4paradigm.sql.jmh.memsql;

import com._4paradigm.sql.BenchmarkConfig;
import com._4paradigm.sql.jmh.DatabaseSetup;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

@Slf4j
public class MemsqlSetup implements DatabaseSetup {
    private Connection connection;

    // Note: can't auto create database from jdbc url using option 'createDatabaseIfNotExist=true'
    //  override `getDb` method as workaround, you may create database manually
    @Override
    public String getDb() {
        return "benchmark";
    }

    @Override
    public void setup() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", "root");
        props.setProperty("password", "bS;=i=|gnrgbXd%<mgMw");
        connection = DriverManager.getConnection(String.format("%s/%s", BenchmarkConfig.MEMSQL_URL, getDb()), props);
    }

    @Override
    public void teardown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    public Connection getConnection() {
        return connection;
    }
}
