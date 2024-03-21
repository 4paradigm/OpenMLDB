package com._4paradigm.openmldb.memoryusagecompare;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;


public class BenchmarkMemoryUsageByTalkingData {
    private static final Logger logger = LoggerFactory.getLogger(BenchmarkMemoryUsageByTalkingData.class);
    private static final RedisExecutor redis = new RedisExecutor();
    private static final OpenMLDBExecutor opdb = new OpenMLDBExecutor();
    private static final InputStream configStream = BenchmarkMemoryUsageByTalkingData.class.getClassLoader().getResourceAsStream("memory.properties");
    private static final Properties config = new Properties();
    private static final Summary summary = new Summary();
    private final HashMap<String, ArrayList<TalkingData>> testData;

    public static void main(String[] args) {
        logger.info("Start benchmark test: Compare memory usage with Redis.");
        try {
            parseConfig();
            BenchmarkMemoryUsageByTalkingData m = new BenchmarkMemoryUsageByTalkingData();
            m.clearData();
            m.insertData();
            m.getMemUsage();
            summary.printMemUsageSummary();

            m.closeConn();
            summary.printMemUsageSummary();
            logger.info("Done.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void parseConfig() throws IOException {
        logger.info("start parse test configs ... ");
        config.load(configStream);
    }

    public BenchmarkMemoryUsageByTalkingData() throws IOException, SQLException {
        redis.initializeJedis(config, configStream);
        opdb.initializeOpenMLDB(config, configStream);
        String tableName = "test_talking_mem";
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
        opdb.tableName = tableName;
        opdb.dbName = "mem";
        testData = Utils.readTalkingDataFromCsv("data/talkingdata.csv");
    }

    private void clearData() throws InterruptedException {
        logger.info("delete all data in redis and openmldb, and wait for the asynchronous operation to complete ... ");
        redis.clear();
        opdb.clear();
        Thread.sleep(10 * 1000);
        logger.info("Done. All test data deleted.");
    }

    private void closeConn() {
        redis.close();
        opdb.close();
    }

    private void insertData() {
        logger.info("start test using dataset train_sample.csv from here: https://github.com/4paradigm/OpenMLDB/tree/main/demo/talkingdata-adtracking-fraud-detection.");

        redis.insertTalkingData(testData);
        opdb.insertTalkingData(testData);
    }

    private void getMemUsage() throws Exception {
        Thread.sleep(10 * 1000);
        HashMap<String, Long> knRes = new HashMap<>();
        HashMap<String, String> redisInfoMap = redis.getRedisInfo();
        HashMap<String, String> openMLDBMem = opdb.getTableStatus();

        knRes.put("redis", Long.parseLong(redisInfoMap.get("used_memory")));
        knRes.put("openmldb", Long.parseLong(openMLDBMem.get(OpenMLDBTableStatusField.MEMORY_DATA_SiZE.name())));
        summary.memSummary.put("talking-data-sample", knRes);
    }

}
