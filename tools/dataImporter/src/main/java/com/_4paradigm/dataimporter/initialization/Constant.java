package com._4paradigm.dataimporter.initialization;

import java.util.concurrent.TimeUnit;

public class Constant {
    public static final String PARQUET_FILEPATH = InitProperties.getProperties().getProperty("parquet.filePath");
    public static final String CSV_FILEPATH = InitProperties.getProperties().getProperty("csv.filePath");
    public static final String PARQUET_TABLENAME = InitProperties.getProperties().getProperty("parquet.tableName");
    public static final String CSV_TABLENAME = InitProperties.getProperties().getProperty("csv.tableName");
    public static final String CSV_SEPARATOR = InitProperties.getProperties().getProperty("csv.separator");
    public static final String CSV_ENCODINGFORMAT = InitProperties.getProperties().getProperty("csv.encodingFormat");
    public static final String ZKENDPOINTS = InitProperties.getProperties().getProperty("zkEndpoints");
    public static final String ZKROOTPATH = InitProperties.getProperties().getProperty("zkRootPath");
    public static final int COREPOOLSIZE = Integer.parseInt(InitProperties.getProperties().getProperty("corePoolSize"));
    public static final int MAXIMUMPOOLSIZE = Integer.parseInt(InitProperties.getProperties().getProperty("maximumPoolSize"));
    public static final int KEEPALIVETIME = Integer.parseInt(InitProperties.getProperties().getProperty("keepAliveTime"));
    public static final int BLOCKINGQUEUESIZE = Integer.parseInt(InitProperties.getProperties().getProperty("blockingQueueSize"));
    public static final TimeUnit TIMEUNIT = TimeUnit.valueOf(InitProperties.getProperties().getProperty("timeUnit"));
    public static final int CLIENTCOUNT = Integer.parseInt(InitProperties.getProperties().getProperty("clientCount"));
    public static final int INTERVAL = Integer.parseInt(InitProperties.getProperties().getProperty("interval"));

    public static final String PARQUET_INDEX = InitProperties.getProperties().getProperty("parquet.index");
    public static final String CSV_INDEX = InitProperties.getProperties().getProperty("csv.index");

}
