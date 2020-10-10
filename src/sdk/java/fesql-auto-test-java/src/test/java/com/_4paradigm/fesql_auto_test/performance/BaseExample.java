package com._4paradigm.fesql_auto_test.performance;

public class BaseExample {
    public static ThreadLocal<Integer> threadLocalProcessCnt = new ThreadLocal<Integer>();
    public static ThreadLocal<Integer> threadLocalProcessErrorCnt = new ThreadLocal<Integer>();
    protected String zkCluster = "172.27.128.37:7181";
    protected String zkPath = "/rtidb_wb";
}
