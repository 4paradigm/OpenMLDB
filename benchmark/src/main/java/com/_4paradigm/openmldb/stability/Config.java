package com._4paradigm.openmldb.stability;


import com._4paradigm.openmldb.sdk.SdkOption;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;

import java.util.Properties;

public class Config {
    public static String ZK_CLUSTER;
    public static String ZK_PATH;
    public static int PUT_THREAD_NUM;
    public static int QUERY_THREAD_NUM;
    public static boolean NEED_CREATE;
    public static boolean NEED_DEPLOY;
    public static int REPLICA_NUM;
    public static int PARTITION_NUM;
    public static String CASE_PATH;
    public static boolean ENABLE_PUT;
    public static boolean ENABLE_QUERY;
    public static int PK_NUM;
    public static float INSERT_RATIO;
    private static SqlExecutor executor = null;
    private static SdkOption option = null;

    static {
        try {
            Properties prop = new Properties();
            prop.load(Config.class.getClassLoader().getResourceAsStream("stability.properties"));
            ZK_CLUSTER = prop.getProperty("ZK_CLUSTER");
            ZK_PATH = prop.getProperty("ZK_PATH");
            PUT_THREAD_NUM = Integer.parseInt(prop.getProperty("PUT_THREAD_NUM"));
            QUERY_THREAD_NUM = Integer.parseInt(prop.getProperty("QUERY_THREAD_NUM"));
            NEED_CREATE = Boolean.valueOf(prop.getProperty("CREATE_TABLE"));
            NEED_DEPLOY = Boolean.valueOf(prop.getProperty("DEPLOY_SQL"));
            REPLICA_NUM = Integer.parseInt(prop.getProperty("REPLICA_NUM"));
            PARTITION_NUM = Integer.parseInt(prop.getProperty("PARTITION_NUM"));
            CASE_PATH = prop.getProperty("CASE_PATH");
            ENABLE_PUT = Boolean.valueOf(prop.getProperty("ENABLE_PUT"));
            ENABLE_QUERY = Boolean.valueOf(prop.getProperty("ENABLE_QUERY"));
            PK_NUM = Integer.parseInt(prop.getProperty("PK_NUM"));
            INSERT_RATIO = Float.parseFloat(prop.getProperty("INSERT_RATIO"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static SqlExecutor GetSqlExecutor(boolean enableDebug) {
        if (executor != null) {
            return executor;
        }
        SdkOption sdkOption = new SdkOption();
        sdkOption.setSessionTimeout(30000);
        sdkOption.setZkCluster(Config.ZK_CLUSTER);
        sdkOption.setZkPath(Config.ZK_PATH);
        sdkOption.setEnableDebug(enableDebug);
        sdkOption.setRequestTimeout(10000000);
        option = sdkOption;
        try {
            executor = new SqlClusterExecutor(option);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return executor;
    }
}
