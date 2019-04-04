package com._4paradigm.dataimporter.initialization;

import java.util.concurrent.TimeUnit;

public class Constant {
    public static final String FILEPATH = InitProperties.getProperties().getProperty("filePath");
    //    public static final String TABLENAME;
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

}
