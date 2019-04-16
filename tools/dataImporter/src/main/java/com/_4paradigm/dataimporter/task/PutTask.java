package com._4paradigm.dataimporter.task;

import com._4paradigm.dataimporter.initialization.Constant;
import com._4paradigm.rtidb.client.TableSyncClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

public class PutTask implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(PutTask.class);
    public static AtomicLong successfulCount = new AtomicLong(0);
    public static AtomicLong failedCount = new AtomicLong(0);
    private String id;
    private long timestamp;
    private TableSyncClient tableSyncClient;
    private String tableName;
    private HashMap map;
    private final int LOG_INTERVAL = Constant.LOG_INTERVAL;

    public PutTask(String id, long timestamp, TableSyncClient tableSyncClient, String tableName, HashMap map) {
        this.id = id;
        this.timestamp = timestamp;
        this.tableSyncClient = tableSyncClient;
        this.tableName = tableName;
        this.map = map;
    }

    @Override
    public void run() {
        if (map == null) {
            logger.warn("the map is null");
            return;
        }
        int limit = 10;//retry times
        while (limit-- > 0) {
            try {
                if (tableSyncClient.put(tableName, timestamp, map)) {
                    break;
                }
            } catch (Exception e) {
                e.printStackTrace();
                logger.error(e.getMessage());
            }
        }
        if (limit < 0) {
            logger.warn("the failed row inserted is : " + map);
            failedCount.getAndIncrement();
        } else {
            long temp = successfulCount.incrementAndGet();
            if (temp % LOG_INTERVAL == 0) {
                logger.info("put successfully successfulCount is ：" + temp);
            }
        }
    }

    @Override
    public String toString() {
        return "Task Num=" + id;
    }
}

