package com._4paradigm.openmldb.conf;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Properties;

public class NLTabletConfig {
    public static String HOST = "127.0.0.1";
    public static int PORT = 9901;
    public static int WORKER_THREAD = 4;
    public static int IO_THREAD = 4;
    public static String ZK_CLUSTER;
    public static String ZK_ROOTPATH;
    public static int ZK_SESSION_TIMEOUT = 5000;
    static {
        try {
            Properties prop = new Properties();
            prop.load(NLTabletConfig.class.getClassLoader().getResourceAsStream("openmldb-tablet.properties"));
            HOST = prop.getProperty("server.host", "127.0.0.1");
            PORT = Integer.parseInt(prop.getProperty("server.port", "9901"));
            WORKER_THREAD = Integer.parseInt(prop.getProperty("server.worker_threads", "4"));
            IO_THREAD = Integer.parseInt(prop.getProperty("server.io_threads", "4"));
            ZK_SESSION_TIMEOUT = Integer.parseInt(prop.getProperty("zookeeper.session_timeout", "5000"));
            ZK_CLUSTER = prop.getProperty("zookeeper.cluster");
            ZK_ROOTPATH = prop.getProperty("zookeeper.root_path");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
