package com._4paradigm.openmldb.memoryusagecompare;

import com._4paradigm.openmldb.sdk.SdkOption;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

public class OpenMLDBExecutor {
    private static final Logger logger = LoggerFactory.getLogger(OpenMLDBExecutor.class);
    static String tableName = "test_db";
    static final String dbName = "mem";
    static SqlExecutor executor;

    public void initializeOpenMLDB(Properties config, InputStream configStream) throws IOException {
        config.load(configStream);
        tableName = config.getProperty("OPENMLDB_TABLE_NAME");
        SdkOption sdkOption = new SdkOption();
        sdkOption.setSessionTimeout(30000);
        sdkOption.setZkCluster(config.getProperty("ZK_CLUSTER"));
        sdkOption.setZkPath(config.getProperty("ZK_PATH"));
        sdkOption.setEnableDebug(true);
        try {
            executor = new SqlClusterExecutor(sdkOption);
            initOpenMLDBEnv();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void initOpenMLDBEnv() throws SQLException {
        Statement statement = executor.getStatement();
        statement.execute("SET @@execute_mode='online';");
        statement.execute("CREATE DATABASE IF NOT EXISTS " + dbName + ";");
        statement.execute("USE " + dbName + ";");
        statement.execute("CREATE TABLE IF NOT EXISTS `" + tableName + "`( \n`key` string,\n`value` string\n) OPTIONS (replicanum=1); ");

        statement.close();
        logger.info("create db and test table.");
    }


    void insert(String key, ArrayList<String> values) throws SQLException {
        for (String value : values) {
            String sql = "INSERT INTO `" + tableName + "` values ('" + key + "'" + "," + "'" + value + "');";
            PreparedStatement statement = executor.getInsertPreparedStmt(dbName, sql);
            statement.execute();
            statement.close();
        }
    }

    void clear() throws SQLException {
        Statement statement = executor.getStatement();
        statement.execute("TRUNCATE TABLE `" + tableName + "`;");
        statement.close();

        while (true) {
            HashMap<String, String> tableStatus = this.getTableStatus();
            if (tableStatus == null) {
                return;
            }

            try {
                int rowCount = Integer.parseInt(tableStatus.get(OpenMLDBTableStatusField.ROWS.name()));
                if (rowCount > 0) {
                    Thread.sleep(1000);
                } else {
                    return;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    void close() {
        if (executor != null) executor.close();
    }

    HashMap<String, String> getTableStatus() {
        logger.info("show openmldb table status...");
        Statement stmt = null;
        ResultSet res = null;
        HashMap<String, String> infoMap = new HashMap<>();
        try {
            stmt = executor.getStatement();
            stmt.executeQuery("SHOW TABLE STATUS;");
            res = stmt.getResultSet();
            while (res.next()) {
                String tName = res.getString(2);
                String dName = res.getString(3);

                if (dName.equals(dbName) && tName.equals(tableName)) {
                    for (OpenMLDBTableStatusField f : OpenMLDBTableStatusField.values()) {
                        infoMap.put(f.name(), res.getString(f.getIndex()));
                    }
                    logger.info("OpenMLDB table status: \n" +
                            "\tused memory: " + infoMap.get(OpenMLDBTableStatusField.MEMORY_DATA_SiZE.name()) + "\n" +
                            "\tdata count: " + infoMap.get(OpenMLDBTableStatusField.ROWS.name())
                    );
                    return infoMap;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null) stmt.close();
                if (res != null) res.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return infoMap;
    }
}
