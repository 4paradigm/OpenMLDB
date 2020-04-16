package com._4paradigm.rtidb.client.base;

public class Config {
    public static final String ZK_ENDPOINTS = "172.27.128.37:7181,172.27.128.37:7182,172.27.128.37:7183";
    public static final String ZK_ROOT_PATH = "/rtidb_cluster";
    public static final String ENDPOINT = "172.27.128.37:9533";
    public static final String[] NODES = new String[]{"172.27.128.37:9533", "172.27.128.37:9532", "172.27.128.37:9531"};
    public static final int READ_TIMEOUT = 1000000;
    public static final int WRITE_TIMEOUT = 1000000;
}
