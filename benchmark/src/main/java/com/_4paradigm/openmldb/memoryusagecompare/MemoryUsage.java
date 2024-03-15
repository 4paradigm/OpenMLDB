package com._4paradigm.openmldb.memoryusagecompare;

import com._4paradigm.openmldb.sdk.SdkOption;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;


public class MemoryUsage {
    private static Logger logger = LoggerFactory.getLogger(MemoryUsage.class);
    private static int keyLength;
    private static int valueLength;
    private static int valuePerKey;
    private static Jedis jedis;
    private static SqlExecutor opdbExecutor;
    private static String opdbTable;

    private static final String dbName = "mem";
    private static final InputStream configStream = MemoryUsage.class.getClassLoader().getResourceAsStream("memory.properties");
    private static final Properties config = new Properties();
    private static HashMap<String, HashMap<String, Integer>> summary;

    public static void main(String[] args) {
        logger.info("Start benchmark test: Compare memory usage with Redis.");
        try {
            // parse config
            logger.info("start parse test configs ... ");
            config.load(configStream);
            keyLength = Integer.parseInt(config.getProperty("KEY_LENGTH"));
            valueLength = Integer.parseInt(config.getProperty("VALUE_LENGTH"));
            valuePerKey = Integer.parseInt(config.getProperty("VALUE_PER_KEY"));
            String[] totalKeyNums = config.getProperty("TOTAL_KEY_NUM").split(",");

            logger.info("test config: \n" +
                    "\tKEY_LENGTH: " + keyLength + "\n" +
                    "\tVALUE_LENGTH: " + valueLength + "\n" +
                    "\tVALUE_PER_KEY: " + valuePerKey + "\n" +
                    "\tTOTAL_KEY_NUM: " + Arrays.toString(totalKeyNums) + "\n"
            );
            MemoryUsage m = new MemoryUsage();
            for (String keyNum : totalKeyNums) {
                logger.info("start test: key size: " + keyNum + ", values per key: " + valuePerKey);
                int kn = Integer.parseInt(keyNum);
                m.insertData(kn);

                Thread.sleep(10 * 1000);

                m.getMemUsage(kn);

                logger.info("this round finished, clear all date in redis ... ");
                jedis.flushAll();
                logger.info("all data cleared.");
            }
            m.clearConn();
            m.printSummary();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public MemoryUsage() throws IOException, SQLException {
        // init redis and openmldb connections
        // initializeJedis();
        initializeOpenMLDB();
    }

    private void initializeJedis() throws IOException {
        config.load(configStream);
        String[] hp = config.getProperty("REDIS_HOST_PORT").split(":");
        String host = hp[0];
        int port = Integer.parseInt(hp[1]);
        jedis = new Jedis(host, port);
    }

    private void initializeOpenMLDB() throws IOException {
        config.load(configStream);
        opdbTable = config.getProperty("OPENMLDB_TABLE_NAME");
        SdkOption sdkOption = new SdkOption();
        sdkOption.setSessionTimeout(30000);
        sdkOption.setZkCluster(config.getProperty("ZK_CLUSTER"));
        sdkOption.setZkPath(config.getProperty("ZK_PATH"));
        sdkOption.setEnableDebug(true);
        sdkOption.setHost("172.27.234.11");
        try {
            opdbExecutor = new SqlClusterExecutor(sdkOption);
            initOpenMLDBEnv();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void initOpenMLDBEnv() throws SQLException {
        Statement statement = opdbExecutor.getStatement();
        statement.execute("SET @@execute_mode='online';");
        statement.execute("CREATE DATABASE IF NOT EXISTS " + dbName + ";");
        statement.execute("USE " + dbName + ";");
        statement.execute("CREATE TABLE IF NOT EXISTS `" + opdbTable + "`( \n`key` string,\n`value` string\n) ; "
        );
        statement.execute("TRUNCATE TABLE `" + opdbTable + "`;");
        statement.close();
        logger.info("create db and test table.");
    }

    private void clearConn() {
        if (jedis != null) {
            jedis.close();
        }

        if (opdbExecutor != null) {
            opdbExecutor.close();
        }
    }

    private void insertData(int keyNum) throws SQLException {
        // gen data
        String key;
        ArrayList<String> values = new ArrayList<>();
        for (int keyIdx = 0; keyIdx < keyNum; keyIdx++) {
            values.clear();
            key = generateRandomString(keyLength);
            for (int valIdx = 0; valIdx < valuePerKey; valIdx++) {
                values.add(generateRandomString(valueLength));
            }
//            insertToRedis(key, values);
            insertToOpenMLDB(key, values);
        }
    }

    private void insertToRedis(String key, ArrayList<String> values) {
        HashMap<String, Double> valScores = new HashMap<>();
        for (int i = 0; i < values.size(); i++) {
            valScores.put(values.get(i), (double) i);
        }
        jedis.zadd(key, valScores);
    }

    private void insertToOpenMLDB(String key, ArrayList<String> values) throws SQLException {
        for (String value : values) {
            String sql = "INSERT INTO `" + opdbTable + "` values ('" + key + "'" + "," + "'" + value + "');";
            logger.info("\n" + sql);
            PreparedStatement statement = opdbExecutor.getInsertPreparedStmt(dbName, sql);
            statement.execute();
            statement.close();
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

    private Map<String, String> parseRedisInfo() {
        String res = jedis.info();
        Map<String, String> infoMap = new HashMap<>();
        String[] lines = res.split("\n");
        for (String line : lines) {
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }
            String[] parts = line.split(":");
            if (parts.length >= 2) {
                infoMap.put(parts[0], parts[1].trim());
            }
        }
        return infoMap;
    }

    private void getMemUsage(int kn) {
        // 输出解析后的信息
        Map<String, String> redisInfoMap = this.parseRedisInfo();
        logger.info("Redis info: \n" +
                "\t\t\tused_memory: " + redisInfoMap.get("used_memory") + "\n" +
                "\t\t\tused_memory_human: " + redisInfoMap.get("used_memory_human") + "\n" +
                "\t\t\tKeyspace.keys: " + redisInfoMap.get("db0")
        );
        HashMap<String, Integer> knRes = new HashMap<>();
        knRes.put("redis", Integer.parseInt(redisInfoMap.get("used_memory")));


        summary.put(kn + "", knRes);
    }

    private void printSummary() {
        StringBuilder report = new StringBuilder();
        report.append(getRow("num", "redisMem", "OpenMLDBMem"));
        int colWidth = 20;
        String border = "-";
        report.append(getRow(repeatString(border, colWidth), repeatString(border, colWidth), repeatString(border, colWidth)));
        for (Map.Entry<String, HashMap<String, Integer>> entry : summary.entrySet()) {
            String num = entry.getKey();
            HashMap<String, Integer> memValues = entry.getValue();
            Integer redisMem = memValues.get("redis");
            Integer openmldbMem = memValues.get("openmldb");
            report.append(getRow(formatValue(num, colWidth), formatValue(redisMem, colWidth), formatValue(openmldbMem, colWidth)));
        }
        logger.info("Summary report:\n" + report);
    }

    private static String getRow(String... values) {
        StringBuilder row = new StringBuilder();
        row.append("|");
        for (String value : values) {
            row.append(" ").append(value).append(" |");
        }
        row.append("\n");
        return row.toString();
    }

    private static String repeatString(String str, int count) {
        StringBuilder repeatedStr = new StringBuilder();
        for (int i = 0; i < count; i++) {
            repeatedStr.append(str);
        }
        return repeatedStr.toString();
    }

    private static String formatValue(Object value, int maxLength) {
        return String.format("%" + (-maxLength) + "s", value.toString());
    }
}
