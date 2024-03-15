package com._4paradigm.openmldb.memoryusagecompare;

import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;


public class DataInsert {
    private static int keyLength;
    private static int valueLength;
    private static int valuePerKey;
    private static Jedis jedis;
    private static Connection opdbConn;
    private static final InputStream configStream = DataInsert.class.getClassLoader().getResourceAsStream("memory.properties");
    private static final Properties config = new Properties();
    private static HashMap<String, Integer> summary;

    public static void main(String[] args) {
        System.out.println("Start benchmark test: Compare memory usage with Redis.");
        try {
            // parse config
            System.out.println("start parse test configs ... ");
            config.load(configStream);
            keyLength = Integer.parseInt(config.getProperty("KEY_LENGTH"));
            valueLength = Integer.parseInt(config.getProperty("VALUE_LENGTH"));
            valuePerKey = Integer.parseInt(config.getProperty("VALUE_PER_KEY"));
            String[] totalKeyNums = config.getProperty("TOTAL_KEY_NUM").split(",");

            System.out.println("test config: \n" +
                    "\tKEY_LENGTH: " + keyLength + "\n" +
                    "\tVALUE_LENGTH: " + valueLength + "\n" +
                    "\tVALUE_PER_KEY: " + valuePerKey + "\n" +
                    "\tTOTAL_KEY_NUM: " + Arrays.toString(totalKeyNums) + "\n"
            );
            DataInsert d = new DataInsert();
            for (String keyNum : totalKeyNums) {
                System.out.println("start test: key size: " + keyNum + ", values per key: " + valuePerKey);
                int kn = Integer.parseInt(keyNum);
                d.insertData(kn);

                Thread.sleep(10 * 1000);

                // 输出解析后的信息
                Map<String, String> infoMap = d.parseRedisInfo();
                System.out.println("Redis info: \n" +
                        "\t\t\tused_memory: " + infoMap.get("used_memory") + "\n" +
                        "\t\t\tused_memory_human: " + infoMap.get("used_memory_human") + "\n" +
                        "\t\t\tKeyspace.keys: " + infoMap.get("db0")
                );

                System.out.println("this round finished, clear all date in redis ... ");
                jedis.flushAll();
                System.out.println("all data cleared.");
            }
            d.clearConn();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public DataInsert() throws IOException, SQLException {
        // init redis and openmldb connections
        initializeJedis();
//        initializeOpenMLDB();
    }

    private void initializeJedis() throws IOException {
        config.load(configStream);
        String[] hp = config.getProperty("REDIS_HOST_PORT").split(":");
        String host = hp[0];
        int port = Integer.parseInt(hp[1]);
        jedis = new Jedis(host, port);
    }

    private void initializeOpenMLDB() throws IOException, SQLException {
        config.load(configStream);
        String connStrWithDb = config.getProperty("OPENMLDB_CONN_STR_WITH_DB");
        opdbConn = DriverManager.getConnection(connStrWithDb);
    }

    private void clearConn() throws SQLException {
        if (jedis != null) {
            jedis.close();
        }

        if (opdbConn != null) {
            opdbConn.close();
        }
    }

    private void insertData(int keyNum) throws InterruptedException {
        // gen data
        String key;
        ArrayList<String> values = new ArrayList<>();

        for (int keyIdx = 0; keyIdx < keyNum; keyIdx++) {
            values.clear();
            key = generateRandomString(keyLength);
            for (int valIdx = 0; valIdx < valuePerKey; valIdx++) {
                values.add(generateRandomString(valueLength));
            }
            insertToRedis(key, values);
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

    private void insertToOpenMLDB(String key, ArrayList<String> values) {
        HashMap<String, Double> valScores = new HashMap<>();
        for (int i = 0; i < values.size(); i++) {
            valScores.put(values.get(i), (double) i);
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

    private Map<String, String> parseRedisInfo1() {
        String res = jedis.info();
        Map<String, String> infoMap = new HashMap<>();
        String[] lines = res.split("\n");
        for (String line : lines) {
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }
            String[] parts = line.split(":");
            if (parts.length >= 2) {
                infoMap.put(parts[0], parts[1]);
            }
        }
        return infoMap;
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

    private void summary() {

    }
}
