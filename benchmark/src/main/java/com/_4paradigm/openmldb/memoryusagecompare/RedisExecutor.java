package com._4paradigm.openmldb.memoryusagecompare;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

public class RedisExecutor {
    private static final Logger logger = LoggerFactory.getLogger(RedisExecutor.class);
    static Jedis jedis;
    private static final int MAX_RETRIES = 5;
    private static final int RETRY_INTERVAL_MS = 1000;


    public void initializeJedis(Properties config, InputStream configStream) throws IOException {
        config.load(configStream);
        String[] hp = config.getProperty("REDIS_HOST_PORT").split(":");
        String host = hp[0];
        int port = Integer.parseInt(hp[1]);
        jedis = new Jedis(host, port);
    }

    void insert(String key, ArrayList<String> values) {
        HashMap<String, Double> valScores = new HashMap<>();
        for (int i = 0; i < values.size(); i++) {
            valScores.put(values.get(i), (double) i);
        }
        jedis.zadd(key, valScores);
    }

    void clear() {
        int flushRetries = 0;
        while (true) {
            try {
                jedis.flushAll();
                break;
            } catch (JedisConnectionException e) {
                if (e.getCause() instanceof java.net.SocketTimeoutException && flushRetries < MAX_RETRIES) {
                    logger.warn("SocketTimeoutException, sleep and retry, timeout retry times: " + (flushRetries + 1));
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
    }

    void close() {
        if (jedis != null) jedis.close();
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
        logger.info("Redis info: \n" +
                "\tused memory: " + infoMap.get("used_memory") + "\n" +
                "\tkeys number: " + infoMap.get("db0")
        );
        return infoMap;
    }
}
