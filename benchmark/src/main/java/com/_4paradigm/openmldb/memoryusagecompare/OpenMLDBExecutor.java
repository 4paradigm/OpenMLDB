package com._4paradigm.openmldb.memoryusagecompare;

import com._4paradigm.openmldb.sdk.SdkOption;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;

public class OpenMLDBExecutor {
    private static final Logger logger = LoggerFactory.getLogger(OpenMLDBExecutor.class);
    public String tableName = "test_db";
    public String dbName = "mem";
    public SqlExecutor executor;

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
        } catch (Exception e) {
            logger.error("Exception: ", e);
        }
    }

    void initOpenMLDBEnv() throws SQLException {
        Statement statement = executor.getStatement();
        statement.execute("SET @@execute_mode='online';");
        statement.execute("CREATE DATABASE IF NOT EXISTS " + dbName + ";");
        statement.execute("USE " + dbName + ";");
        statement.close();
        logger.info("create db.");
    }

    void initOpenMLDBEnvWithDDL(String sql) throws SQLException {
        Statement statement = executor.getStatement();
        statement.execute("SET @@execute_mode='online';");
        statement.execute("CREATE DATABASE IF NOT EXISTS " + dbName + ";");
        statement.execute("USE " + dbName + ";");
        try {
            statement.execute("DROP TABLE " + tableName + ";");
        } catch (SQLException e) {
            if (e.getMessage().contains("table does not exist")) {
                logger.warn("drop table error, table dose not exist.");
            } else {
                throw e;
            }
        }

        statement.execute(sql);

        statement.close();
        logger.info("create db and test table.");
    }

    void insert(HashMap<String, ArrayList<String>> keyValues) {
        String sqlWithPlaceHolder = "INSERT INTO `" + tableName + "` values (?,?);";
        java.sql.PreparedStatement statement = null;
        try {
            statement = executor.getInsertPreparedStmt(dbName, sqlWithPlaceHolder);
            for (String key : keyValues.keySet()) {
                for (String value : keyValues.get(key)) {
                    statement.setString(1, key);
                    statement.setString(2, value);
                    statement.addBatch();
                }
            }
            statement.executeBatch();
        } catch (SQLException e) {
            logger.error("Exception: ", e);
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    logger.error("Exception: ", e);
                }
            }
        }
    }

    void insertTalkingData(HashMap<String, ArrayList<TalkingData>> keyValues) {
        String sqlWithPlaceHolder = "INSERT INTO `" + tableName + "` values (?,?,?,?,?,?,?);";
        java.sql.PreparedStatement statement = null;
        try {
            statement = executor.getInsertPreparedStmt(dbName, sqlWithPlaceHolder);
            for (String key : keyValues.keySet()) {
                for (TalkingData td : keyValues.get(key)) {
                    statement.setString(1, key);
                    statement.setInt(2, td.app);
                    statement.setInt(3, td.device);
                    statement.setInt(4, td.os);
                    statement.setInt(5, td.channel);
                    statement.setTimestamp(6, Timestamp.valueOf(td.clickTime));
                    statement.setInt(7, td.isAttribute);
                    statement.addBatch();
                }
            }
            statement.executeBatch();
        } catch (SQLException e) {
            logger.error("Exception: ", e);
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    logger.error("Exception: ", e);
                }
            }
        }
    }

    ArrayList<HashMap<String, Object>> queryRowsWithSql(String sql) {
        Statement statement = null;
        ResultSet res;
        try {
            statement = executor.getStatement();
            statement.execute(sql);
            res = statement.getResultSet();

            ResultSetMetaData metaData = res.getMetaData();
            int columnCount = metaData.getColumnCount();
            ArrayList<HashMap<String, Object>> rows = new ArrayList<>();

            while (res.next()) {
                HashMap<String, Object> row = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    int columnType = metaData.getColumnType(i);
                    Object value = getValue(columnType, i, res);
                    row.put(columnName, value);
                }
                rows.add(row);
            }
            return rows;
        } catch (SQLException e) {
            logger.error(e.getMessage());
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    logger.error("Exception: ", e);
                }
            }
        }
        return null;
    }

    int queryRowSizeWithSql(String sql) {
        Statement statement = null;
        ResultSet res;
        try {
            statement = executor.getStatement();
            statement.execute(sql);
            res = statement.getResultSet();
            return res.getFetchSize();
        } catch (SQLException e) {
            logger.error(e.getMessage());
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    logger.error("Exception: ", e);
                }
            }
        }
        return 0;
    }

    private Object getValue(int tp, int idx, ResultSet rs) throws SQLException {
        Object val;
        switch (tp){
            case Types.VARCHAR:
                return rs.getString(idx);
            case Types.INTEGER:
                return rs.getInt(idx);
            case Types.FLOAT:
                return rs.getFloat(idx);
            case Types.DOUBLE:
                return rs.getDouble(idx);
            case Types.DATE:
                return rs.getDate(idx);
            case Types.TIMESTAMP:
                return rs.getTimestamp(idx);
            case Types.BOOLEAN:
                return rs.getBoolean(idx);
            case Types.TIME:
                return rs.getTime(idx);
            default:
                return null;
        }
    }

    public void clear() {
        Statement statement = null;
        try {
            statement = executor.getStatement();
            statement.execute("TRUNCATE TABLE `" + tableName + "`;");
        } catch (SQLException e) {
            logger.warn(e.getMessage());
            if (e.getMessage().contains("table does not exist")) {
                return;
            }
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    logger.error("Exception: ", e);
                }
            }
        }

        while (true) {
            try {
                HashMap<String, String> tableStatus = this.getTableStatus();
                if (tableStatus == null) {
                    return;
                }
                int rowCount = Integer.parseInt(tableStatus.get(OpenMLDBTableStatusField.ROWS.name()));
                if (rowCount > 0) {
                    Thread.sleep(1000);
                } else {
                    return;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                logger.error("Exception: ", e);
            }
        }
    }

    public void close() {
        if (executor != null) executor.close();
    }

    public HashMap<String, String> getTableStatus() throws Exception {
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
            throw new Exception("get openmldb table memory usage error, table does not exist.");
        } catch (SQLException e) {
            logger.error("Exception: ", e);
        } finally {
            try {
                if (stmt != null) stmt.close();
                if (res != null) res.close();
            } catch (SQLException e) {
                logger.error("Exception: ", e);
            }
        }
        return infoMap;
    }

    public void createIndex(String indexName, String indexColName) throws SQLException {
        Statement statement = null;
        String sql = "CREATE INDEX " + indexName + " ON " + tableName + " (" + indexColName + ");";
        try {
            statement = executor.getStatement();
            statement.executeQuery(sql);
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    logger.error("Exception: ", e);
                }
            }
        }
    }

    public boolean waitForIndexJobFinish() throws SQLException {
        String sql = "SHOW JOBS FROM NAMESERVER;";
        Statement statement = null;
        ResultSet res;
        try {
            while (true) {
                statement = executor.getStatement();
                statement.execute(sql);
                res = statement.getResultSet();
                while (res.next()) {
                    // use empty while to move cursor to last row, because res.last() method is not supported now.
                }
                String state = res.getString(3);
                if (state.equals("FINISHED")) {
                    return true;
                } else if (state.equals("LOST")) {
                    logger.error("Task state: LOST. Please check the job log manually.");
                    return false;
                }
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    logger.error("Exception: ", e);
                }
            }
        }
    }
}
