package com._4paradigm.openmldb.indexmemoryusage;

import com._4paradigm.openmldb.memoryusagecompare.OpenMLDBExecutor;
import com._4paradigm.openmldb.memoryusagecompare.OpenMLDBTableStatusField;
import com._4paradigm.openmldb.memoryusagecompare.Summary;
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
import java.util.Random;

/**
 * 1. parse config from memory.properties.
 * a. zk_cluster: ZK_CLUSTER;
 * b. zk_path: ZK_PATH;
 * c. test db:OPENMLDB_TABLE_NAME;
 * 2. insert data;
 * 3. get memory usage;
 * 4. create index;
 * 5. get memory usage;
 */

public class BenchmarkIndexMemoryUsage {
    private static final Logger logger = LoggerFactory.getLogger(BenchmarkIndexMemoryUsage.class);
    private static int keyLength;
    private static int valueLength;
    private static int valuePerKey;
    private final String tableName = "test_index";
    private final String dbName = "mem";
    private final OpenMLDBExecutor opdb = new OpenMLDBExecutor();
    private static final InputStream configStream = BenchmarkIndexMemoryUsage.class.getClassLoader().getResourceAsStream("memory.properties");
    private static final Properties config = new Properties();
    private static final ArrayList<ResourceUsage> summary = new ArrayList<>();
    private static final int keyNum = 100000;

    private final String[] colNames = new String[]{"col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col9", "col10"};

    public static void main(String[] args) {
        logger.info("start index memory usage test ... ");
        try {
            parseConfig();
            BenchmarkIndexMemoryUsage m = new BenchmarkIndexMemoryUsage();
            m.createTable();
            m.clearData();
            m.insertData();
            m.getMemUsage("origin(include index on col1)");

            m.testIndexMemoryUsage();

            m.printSummary();
            m.closeConn();
            logger.info("Done.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void parseConfig() throws IOException {
        logger.info("start parse test configs ... ");
        config.load(configStream);
        keyLength = Integer.parseInt(config.getProperty("KEY_LENGTH"));
        valueLength = Integer.parseInt(config.getProperty("VALUE_LENGTH"));
        valuePerKey = Integer.parseInt(config.getProperty("VALUE_PER_KEY"));

        logger.info("test config: \n" +
                "\tKEY_LENGTH: " + keyLength + "\n" +
                "\tVALUE_LENGTH: " + valueLength + "\n" +
                "\tVALUE_PER_KEY: " + valuePerKey + "\n"
        );
    }

    public BenchmarkIndexMemoryUsage() throws IOException {
        opdb.initializeOpenMLDB(config, configStream);
        opdb.tableName = tableName;
        opdb.dbName = dbName;
    }

    private void createTable() throws SQLException {
        Statement statement = opdb.executor.getStatement();
        statement.execute("SET @@execute_mode='online';");
        statement.execute("CREATE DATABASE IF NOT EXISTS " + dbName + ";");
        statement.execute("USE " + dbName + ";");
        try {
            statement.execute("DROP TABLE " + tableName + ";");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE IF NOT EXISTS `" + tableName + "`( ");
        for (String col : colNames) {
            sb.append("`").append(col).append("` string,").append("\n");
        }
        sb.append("\n) OPTIONS (replicanum=1); ");
        statement.execute(sb.toString());

        statement.close();
        logger.info("create db and test table.");
    }

    private void clearData() throws SQLException, InterruptedException {
        logger.info("delete all data in redis and openmldb, and wait for the asynchronous operation to complete ... ");
        opdb.clear();
        Thread.sleep(10 * 1000);
        logger.info("Done. All test data deleted.");
    }

    private void closeConn() {
        opdb.close();
    }

    private void insertData() {
        logger.info("start test: key size: " + keyNum + ", values per key: " + valuePerKey);
        int count = 0;
        HashMap<String, ArrayList<String>> keyValues = new HashMap<>();
        for (int keyIdx = 0; keyIdx <= keyNum; keyIdx++) {
            int batchKeys = 100;
            if (count >= batchKeys) {
                insert(keyValues);
                count = 0;
                keyValues.clear();
            }
            String key = generateRandomString(keyLength);
            ArrayList<String> values = new ArrayList<>();
            for (int valIdx = 0; valIdx < valuePerKey; valIdx++) {
                values.add(generateRandomString(valueLength));
            }
            keyValues.put(key, values);
            count++;
        }
    }

    void insert(HashMap<String, ArrayList<String>> keyValues) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO `" + tableName + "` values (");
        for (int i = 0; i < colNames.length; i++) {
            String col = colNames[i];
            sb.append("?");
            if (i != colNames.length - 1) {
                sb.append(",");
            }
        }
        sb.append(");");

        PreparedStatement statement = null;
        try {
            statement = opdb.executor.getInsertPreparedStmt(dbName, sb.toString());
            for (String key : keyValues.keySet()) {
                for (String value : keyValues.get(key)) {
                    for (int i = 0; i < colNames.length; i++) {
                        statement.setString(i + 1, colNames[i] + "_" + value);
                    }
                    statement.addBatch();
                }
            }
            statement.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    // add index and monitor memory usage change
    void testIndexMemoryUsage() throws Exception {
        for (int i = 1; i < colNames.length; i++) {
            String col = colNames[i];
            String indexName = "idx_" + col;
            this.createIndex(indexName, col);
            if (!waitAsyncJobFinish()) {
                logger.error("create index error, stop running.");
                throw new Exception("create index error");
            }
            this.getMemUsage("add index on " + col);
            this.printSummary();
            Thread.sleep(2000);
        }
    }

    public void createIndex(String indexName, String indexColName) throws SQLException {
        Statement statement = null;
        String sql = "CREATE INDEX " + indexName + " ON " + tableName + " (" + indexColName + ");";
        try {
            statement = opdb.executor.getStatement();
            statement.executeQuery(sql);
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    // wait for async job finished.
    boolean waitAsyncJobFinish() throws SQLException {
        String sql = "SHOW JOBS FROM NAMESERVER;";
        Statement statement = null;
        ResultSet res;
        try {
            while (true) {
                statement = opdb.executor.getStatement();
                statement.execute(sql);
                res = statement.getResultSet();
                while (res.next()) {
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
                    e.printStackTrace();
                }
            }
        }
    }

    private String generateRandomString(int length) {
        String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder result = new StringBuilder();
        Random random = new Random();
        while (length-- > 0) {
            result.append(characters.charAt(random.nextInt(characters.length())));
        }
        return result.toString();
    }

    private void getMemUsage(String label) throws InterruptedException {
        Thread.sleep(10 * 1000);
        HashMap<String, String> openMLDBMem = opdb.getTableStatus();
        ResourceUsage usage = new ResourceUsage(label, Long.parseLong(openMLDBMem.get(OpenMLDBTableStatusField.MEMORY_DATA_SiZE.name())));
        summary.add(usage);
    }

    public void printSummary() {
        int colWidth = 20;
        StringBuilder report = new StringBuilder();
        report.append(Summary.getRow(Summary.formatValue("label", colWidth, "center"), Summary.formatValue("OpenMLDBMem", colWidth, "center")));

        String border = "-";
        report.append(Summary.getRow(Summary.repeatString(border, colWidth), Summary.repeatString(border, colWidth)));

        long preMemUsage = 0L;
        for (ResourceUsage usage : summary) {
            String label = usage.label;
            Long openmldbMem = usage.memoryUsage;

            report.append(Summary.getRow(Summary.formatValue(label, colWidth), Summary.formatValue(openmldbMem, colWidth)));
            if (preMemUsage == 0L) {
                preMemUsage = openmldbMem;
            }
        }
        logger.info("\n====================\n" +
                "Summary report" +
                "\n====================\n" +
                report
        );
    }

    private static class ResourceUsage {
        private String label;
        private long memoryUsage;

        public ResourceUsage(String label, long memoryUsage) {
            this.label = label;
            this.memoryUsage = memoryUsage;
        }
    }

}
