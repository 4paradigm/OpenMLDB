package com._4paradigm.rtidb.client.base;

public class Config {
    public static final String ZK_ENDPOINTS = "127.0.0.1:6181";
    public static final String ZK_ROOT_PATH = "/onebox";
    public static final String ENDPOINT = "127.0.0.1:9522";
    public static final String[] NODES = new String[]{"127.0.0.1:9522", "127.0.0.1:9521", "127.0.0.1:9520"};
    public static final int READ_TIMEOUT = 1000000;
    public static final int WRITE_TIMEOUT = 1000000;
}
