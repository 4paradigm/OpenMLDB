package com._4paradigm.fesql_auto_test.performance;

public class BaseExample {
    public static ThreadLocal<Integer> threadLocalProcessCnt = new ThreadLocal<Integer>();
    public static ThreadLocal<Integer> threadLocalProcessErrorCnt = new ThreadLocal<Integer>();
    protected String zkCluster = "127.0.0.1:6181";
    protected String zkPath = "/onebox";
}
