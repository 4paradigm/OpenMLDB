package com._4paradigm.sql.jmh;

public class BenchmarkConfig {
    public static String ZK_CLUSTER="127.0.0.1:6181";
//    public static String ZK_CLUSTER="172.27.128.32:12200";
//    public static String ZK_PATH="/standalone";
    public static String ZK_PATH="/cluster";
//    public static String ZK_PATH="/onebox";
//    public static String ZK_CLUSTER="172.27.128.81:16181";
//    public static String ZK_PATH="/fedb_cluster";
    public static String MEMSQL_URL="jdbc:mysql://172.27.128.37:3306/benchmark?user=benchmark&password=benchmark";
}
