package com._4paradigm.openmldb.memoryusagecompare;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.resps.Tuple;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.time.Duration;
import java.util.*;


public class RedisExecutor {
    private static final Logger logger = LoggerFactory.getLogger(RedisExecutor.class);
    static Jedis jedis;
    static JedisPool pool;
    private static final int MAX_RETRIES = 20;
    private static final int RETRY_INTERVAL_MS = 1000;
    public static final HashMap<String, Integer> cacheDecimal = new HashMap<>();
    private static final int step = 1;


    public void initializeJedis(Properties config, InputStream configStream) throws IOException {
        config.load(configStream);
        String[] hp = config.getProperty("REDIS_HOST_PORT").split(":");
        String host = hp[0];
        int port = Integer.parseInt(hp[1]);
        logger.info("init redis config, host: {}, port:{}", host, port);
        jedis = new Jedis(host, port, 10000);
    }

    public void initJedisPool(Properties config, InputStream configStream) throws IOException {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(128); // 最大连接数
        poolConfig.setMaxIdle(64); // 最大空闲连接数
        poolConfig.setMinIdle(16); // 最小空闲连接数
        poolConfig.setMaxWait(Duration.ofMillis(5000)); // 最大等待时间

        config.load(configStream);
        String[] hp = config.getProperty("REDIS_HOST_PORT").split(":");
        String host = hp[0];
        int port = Integer.parseInt(hp[1]);
        int timeout = 5000;
        logger.info("init redis pool config, host: {}, port:{}, timeout:{}", host, port, timeout);

        pool = new JedisPool(poolConfig, host, port, timeout);
    }

    void insert(HashMap<String, ArrayList<String>> keyValues) {
        Pipeline pipeline = jedis.pipelined();
        for (String key : keyValues.keySet()) {
            HashMap<String, Double> valScores = new HashMap<>();
            for (int i = 0; i < keyValues.get(key).size(); i++) {
                valScores.put(keyValues.get(key).get(i), (double) i);
            }
            pipeline.zadd(key, valScores);
        }
        pipeline.sync();
    }

    void insertTalkingData(HashMap<String, ArrayList<TalkingData>> keyValues) throws ParseException {
        Pipeline pipeline = jedis.pipelined();
        Gson gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();

        for (String key : keyValues.keySet()) {
            ArrayList<TalkingData> tds = keyValues.get(key);
            int currScore = 0;
            if (cacheDecimal.containsKey(key)) {
                currScore = cacheDecimal.get(key);
            } else {
                cacheDecimal.put(key, currScore);
            }
            for (TalkingData td : tds) {
                currScore += step;
                String jsonStr = gson.toJson(td);
                pipeline.zadd(key, Utils.getTimestamp(td.clickTime), jsonStr + "#" + currScore);
            }
            cacheDecimal.put(key, currScore);
        }
        pipeline.sync();
    }

    List<String> queryAllData(String key) {
        try (Jedis j = pool.getResource()) {
            return j.zrange(key, 0, -1);
        }
    }

    List<String> queryDataWithScore(String key, double score) {
        try (Jedis j = pool.getResource()) {
            return j.zrangeByScore(key, score, score);
        }
    }

    List<String> queryDataRangeByScores(String key, double minScore, double maxScore) {
        try (Jedis j = pool.getResource()) {
            return j.zrangeByScore(key, minScore, maxScore);
        }
    }


    void clear() {
        int flushRetries = 0;
        while (true) {
            try {
                jedis.flushAll();
                break;
            } catch (Exception e) {
                if (flushRetries < MAX_RETRIES) {
                    logger.warn("exception: " + e.getCause() + ", sleep and retry, timeout retry times: " + (flushRetries + 1));
                    flushRetries++;
                    try {
                        Thread.sleep(RETRY_INTERVAL_MS); // 休眠一段时间后重试
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                } else {
                    logger.error("exception: " + e.getCause() + ", timeout retry times: " + (flushRetries + 1));
                    throw e;
                }
            }
        }

        while (jedis.dbSize() > 0) {
            try {
                Thread.sleep(1000); // Wait for 1 second
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        logger.info("clear redis done.");
    }

    void close() {
        if (jedis != null) jedis.close();
        if (pool != null) pool.close();
    }

    HashMap<String, String> getRedisInfo() {
        String res = jedis.info();
        HashMap<String, String> infoMap = new HashMap<>();
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
        logger.info("Redis info: \n\tused memory: {}\n\tkeys number: {}", infoMap.get("used_memory"), infoMap.get("db0"));
        return infoMap;
    }
}
