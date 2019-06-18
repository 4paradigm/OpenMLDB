package com._4paradigm.rtidb.client.base;

public class Config {
    public static final String ZK_ENDPOINTS = "172.27.128.37:6181";
    public static final String ZK_ROOT_PATH = "/onebox";
    public static final String ENDPOINT = "172.27.128.37:9522";
    public static final String[] NODES = new String[]{"172.27.128.37:9522", "172.27.128.37:9521", "172.27.128.37:9520"};
    public static final int READ_TIMEOUT = 100000;
    public static final int WRITE_TIMEOUT = 1000000;
}
