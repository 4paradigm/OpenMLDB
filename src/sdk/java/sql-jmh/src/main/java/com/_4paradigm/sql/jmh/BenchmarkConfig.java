package com._4paradigm.sql.jmh;

public class BenchmarkConfig {
    public static String ZK_CLUSTER="172.27.128.81:16181";
    public static String ZK_PATH="/fedb_cluster2";
    public static String MEMSQL_URL="jdbc:mysql://172.27.128.37:3306/benchmark?user=benchmark&password=benchmark";
    public static String REDIS_IP = "172.27.128.81";
    public static int REDIS_PORT = 6379;
}
