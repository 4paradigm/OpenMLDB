package com._4paradigm.sql.jmh;

import com._4paradigm.sql.sdk.SdkOption;
import com._4paradigm.sql.sdk.SqlExecutor;
import com._4paradigm.sql.sdk.impl.SqlClusterExecutor;

import java.util.Properties;

public class BenchmarkConfig {
    public static String ZK_CLUSTER="172.27.128.32:12200";
    public static String ZK_PATH="/standalone";
    public static String ZK_NS = "featuredb";
    public static String MEMSQL_URL="jdbc:mysql://172.27.128.37:3306/benchmark?user=benchmark&password=benchmark";
    public static String PARTITION_NUM = "4";

    public static String ddlUrl;
    public static String scriptUrl;
    public static String relationUrl;
    public static String jsonUrl;


    private static SqlExecutor executor = null;
    private static SdkOption option = null;
    private static boolean needProxy = false;

    static {
        try {
            Properties prop = new Properties();
            prop.load(BenchmarkConfig.class.getClassLoader().getResourceAsStream("conf.properties"));
            ZK_CLUSTER = prop.getProperty("ZK_CLUSTER");
            ZK_PATH = prop.getProperty("ZK_PATH");
            ZK_NS = prop.getProperty("ZK_NS");
            PARTITION_NUM = prop.getProperty("PARTITION_NUM");
            ddlUrl = prop.getProperty("ddlUrl");
            scriptUrl = prop.getProperty("scriptUrl");
            relationUrl = prop.getProperty("relationUrl");
            jsonUrl = prop.getProperty("jsonUrl");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static boolean NeedProxy() {
        return needProxy;
    }

    public static SqlExecutor GetSqlExecutor(boolean enableDebug) {
        if (executor != null) {
            return executor;
        }
        SdkOption sdkOption = new SdkOption();
        sdkOption.setSessionTimeout(30000);
        sdkOption.setZkCluster(BenchmarkConfig.ZK_CLUSTER);
        sdkOption.setZkPath(BenchmarkConfig.ZK_PATH);
        sdkOption.setEnableDebug(enableDebug);
        option = sdkOption;
        try {
            executor = new SqlClusterExecutor(option);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return executor;
    }


}
