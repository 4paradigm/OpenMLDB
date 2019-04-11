package com._4paradigm.dataimporter.initialization;

import java.util.concurrent.TimeUnit;

public class Constant {
    public static final String FILEPATH = InitProperties.getProperties().getProperty("filePath");
    public static final String TABLENAME = InitProperties.getProperties().getProperty("tableName");
    public static final String TABLE_EXIST = InitProperties.getProperties().getProperty("tableExist");
    public static final String INDEX = InitProperties.getProperties().getProperty("index");
    public static final String TIMESTAMP = InitProperties.getProperties().getProperty("timeStamp");
    public static final String CSV_SEPARATOR = InitProperties.getProperties().getProperty("csv.separator");
    public static final String CSV_ENCODINGFORMAT = InitProperties.getProperties().getProperty("csv.encodingFormat");

    public static final String ZKENDPOINTS = InitProperties.getProperties().getProperty("zkEndpoints");
    public static final String ZKROOTPATH = InitProperties.getProperties().getProperty("zkRootPath");
    public static final int REPLICA_NUM = Integer.parseInt(InitProperties.getProperties().getProperty("replicaNum"));
    public static final int PARTITION_NUM = Integer.parseInt(InitProperties.getProperties().getProperty("partitionNum"));
    public static final long TTL = Long.parseLong(InitProperties.getProperties().getProperty("ttl"));

    public static final int COREPOOLSIZE = Integer.parseInt(InitProperties.getProperties().getProperty("corePoolSize"));
    public static final int MAXIMUMPOOLSIZE = Integer.parseInt(InitProperties.getProperties().getProperty("maximumPoolSize"));
    public static final int KEEPALIVETIME = Integer.parseInt(InitProperties.getProperties().getProperty("keepAliveTime"));
    public static final int BLOCKINGQUEUESIZE = Integer.parseInt(InitProperties.getProperties().getProperty("blockingQueueSize"));
    public static final TimeUnit TIMEUNIT = TimeUnit.valueOf(InitProperties.getProperties().getProperty("timeUnit"));
    public static final int MAX_THREAD_NUM = Integer.parseInt(InitProperties.getProperties().getProperty("maxThreadNum"));
    public static final int LOG_INTERVAL = Integer.parseInt(InitProperties.getProperties().getProperty("log.interval"));


}
