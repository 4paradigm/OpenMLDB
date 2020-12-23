package com._4paradigm.sql.jmh;

import com._4paradigm.sql.sdk.SdkOption;
import com._4paradigm.sql.sdk.SqlExecutor;
import com._4paradigm.sql.sdk.impl.SqlClusterExecutor;
import com._4paradigm.featuredb.driver.DBMSClient;
import com._4paradigm.featuredb.driver.impl.DBMSDriverClientImpl;

public class BenchmarkConfig {
//    public static String ZK_CLUSTER = "127.0.0.1:6181";
    public static String ZK_CLUSTER="172.27.128.32:12200";
    public static String ZK_PATH="/standalone";
//    public static String ZK_PATH="/cluster";
//    public static String ZK_PATH="/onebox";
//    public static String ZK_CLUSTER="172.27.128.81:16181";
//    public static String ZK_PATH="/rtidb_fe";
    public static String ZK_NS = "featuredb";
    public static String MEMSQL_URL="jdbc:mysql://172.27.128.37:3306/benchmark?user=benchmark&password=benchmark";
    public static String PARTITION_NUM = "4";

    private static SqlExecutor executor = null;
    private static SdkOption option = null;
    private static boolean needProxy = false;

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
