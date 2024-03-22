package com._4paradigm.openmldb.memoryusagecompare;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

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
    private static final Summary summary = new Summary();
    private static final int keyNum = 100000;
    private final String[] colNames = new String[]{"key", "int", "int32", "int64", "float", "double", "str", "date", "timestamp"};

    public static void main(String[] args) {
        logger.info("start index memory usage test ... ");
        try {
            parseConfig();
            BenchmarkIndexMemoryUsage m = new BenchmarkIndexMemoryUsage();
            m.clearData();
            m.insertData();
            m.getMemUsage("origin(include index on col1)");

            m.testIndexMemoryUsage();

            summary.printIndexMemUsageSummary();
            m.closeConn();
            logger.info("Done.");
        } catch (Exception e) {
            logger.error("Exception: ", e);
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

    public BenchmarkIndexMemoryUsage() throws IOException, SQLException {
        opdb.initializeOpenMLDB(config, configStream);
        opdb.tableName = tableName;
        opdb.dbName = dbName;
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE IF NOT EXISTS `" + tableName + "`( ");
        for (String col : colNames) {
            switch (col) {
                case "key":
                case "str":
                    sb.append("`").append(col).append("` string,").append("\n");
                    break;
                case "int":
                case "int32":
                    sb.append("`").append(col).append("` int,").append("\n");
                    break;
                case "long":
                case "int64":
                    sb.append("`").append(col).append("` int64,").append("\n");
                    break;
                case "float":
                case "float32":
                    sb.append("`").append(col).append("` float,").append("\n");
                    break;
                case "double":
                case "float64":
                    sb.append("`").append(col).append("` double,").append("\n");
                    break;
                case "date":
                    sb.append("`").append(col).append("` date,").append("\n");
                    break;
                case "timestamp":
                    sb.append("`").append(col).append("` timestamp,").append("\n");
                    break;
            }
        }
        sb.append("\n) OPTIONS (replicanum=1); ");
        opdb.initOpenMLDBEnvWithDDL(sb.toString());
    }

    private void clearData() throws InterruptedException {
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
            String key = Utils.generateRandomString(keyLength);
            ArrayList<String> values = new ArrayList<>();
            for (int valIdx = 0; valIdx < valuePerKey; valIdx++) {
                values.add(Utils.generateRandomString(valueLength));
            }
            keyValues.put(key, values);
            count++;
        }
    }

    void insert(HashMap<String, ArrayList<String>> keyValues) {
        PreparedStatement statement = null;
        try {
            String insertSql = "INSERT INTO `" + tableName + "` values (?, ?, ?, ?, ?, ?, ?, ?, ?);";
            statement = opdb.executor.getInsertPreparedStmt(dbName, insertSql);
            for (String key : keyValues.keySet()) {
                for (String value : keyValues.get(key)) {
                    statement.setString(1, key);
                    for (int i = 1; i < colNames.length; i++) {
                        switch (colNames[i]) {
                            case "str":
                                statement.setString(i + 1, value);
                                break;
                            case "int":
                            case "int32":
                                statement.setInt(i + 1, RandomDataGenerator.generateRandomInt32());
                                break;
                            case "int64":
                            case "long":
                                statement.setLong(i + 1, RandomDataGenerator.generateRandomInt64());
                                break;
                            case "float":
                            case "float32":
                                statement.setFloat(i + 1, RandomDataGenerator.generateRandomFloat());
                                break;
                            case "double":
                            case "float64":
                                statement.setDouble(i + 1, RandomDataGenerator.generateRandomDouble());
                                break;
                            case "date":
                                statement.setDate(i + 1, RandomDataGenerator.generateRandomDate());
                                break;
                            case "timestamp":
                                statement.setTimestamp(i + 1, new Timestamp(RandomDataGenerator.generateRandomTimestamp()));
                                break;
                        }
                    }
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

    // add index and monitor memory usage change
    void testIndexMemoryUsage() throws Exception {
        for (int i = 1; i < colNames.length; i++) {
            String col = colNames[i];
            if (col.contains("float") || col.contains("double")) {
                logger.warn("unsupported to add index on column: {}", col);
            } else {
                String indexName = "idx_" + col;
                opdb.createIndex(indexName, col);
                if (!opdb.waitForIndexJobFinish()) {
                    logger.error("create index error, stop running.");
                    throw new Exception("create index error");
                }
                this.getMemUsage("add index on " + col);
                summary.printIndexMemUsageSummary();
                Thread.sleep(2000);
            }
        }
    }

    private void getMemUsage(String label) throws Exception {
        Thread.sleep(10 * 1000);
        HashMap<String, String> openMLDBMem = opdb.getTableStatus();
        ResourceUsage usage = new ResourceUsage(label, Long.parseLong(openMLDBMem.get(OpenMLDBTableStatusField.MEMORY_DATA_SiZE.name())));
        summary.indexMemSummary.add(usage);
    }
}
