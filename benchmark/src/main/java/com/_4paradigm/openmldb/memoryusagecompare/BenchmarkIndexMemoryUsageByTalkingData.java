package com._4paradigm.openmldb.memoryusagecompare;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
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
 * 6. repeat 4-5 ...
 */

public class BenchmarkIndexMemoryUsageByTalkingData {
    private static final Logger logger = LoggerFactory.getLogger(BenchmarkIndexMemoryUsageByTalkingData.class);
    private final String tableName = "test_talking_index";
    private final OpenMLDBExecutor opdb = new OpenMLDBExecutor();
    private static final InputStream configStream = BenchmarkIndexMemoryUsageByTalkingData.class.getClassLoader().getResourceAsStream("memory.properties");
    private static final Properties config = new Properties();
    private static final Summary summary = new Summary();
    private final HashMap<String, ArrayList<TalkingData>> testData;
    private final String[] colNames = new String[]{"ip", "app", "device", "os", "channel", "click_time", "is_attributed"};

    public static void main(String[] args) {
        logger.info("start index memory usage test ... ");
        try {
            parseConfig();
            BenchmarkIndexMemoryUsageByTalkingData m = new BenchmarkIndexMemoryUsageByTalkingData();
            m.clearData();
            m.insertData();
            m.getMemUsage("origin(index on ip)");

            m.testIndexMemoryUsage();

            summary.printIndexMemUsageSummary();
            m.closeConn();
            logger.info("Done.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void parseConfig() throws IOException {
        logger.info("start parse test configs ... ");
        config.load(configStream);
    }

    public BenchmarkIndexMemoryUsageByTalkingData() throws IOException, SQLException {
        opdb.initializeOpenMLDB(config, configStream);
        opdb.tableName = tableName;
        opdb.dbName = "mem";

        String sb = "CREATE TABLE IF NOT EXISTS `" + tableName + "`(\n" +
                "`ip` string, \n" +
                "`app` int, \n" +
                "`device` int, \n" +
                "`os` int, \n" +
                "`channel` int, \n" +
                "`click_time` Timestamp, \n" +
                "`is_attributed` int \n" +
                ")\n" + "OPTIONS (replicanum=1); ";
        opdb.initOpenMLDBEnvWithDDL(sb);
        testData = Utils.readTalkingDataFromCsv("data/talkingdata.csv");
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
        logger.info("start test using dataset train_sample.csv from here: https://github.com/4paradigm/OpenMLDB/tree/main/demo/talkingdata-adtracking-fraud-detection.");
        opdb.insertTalkingData(testData);
    }

    // add index and monitor memory usage change
    void testIndexMemoryUsage() throws Exception {
        for (int i = 1; i < colNames.length; i++) {
            String col = colNames[i];
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

    private void getMemUsage(String label) throws Exception {
        Thread.sleep(10 * 1000);
        HashMap<String, String> openMLDBMem = opdb.getTableStatus();
        ResourceUsage usage = new ResourceUsage(label, Long.parseLong(openMLDBMem.get(OpenMLDBTableStatusField.MEMORY_DATA_SiZE.name())));
        summary.indexMemSummary.add(usage);
    }
}
