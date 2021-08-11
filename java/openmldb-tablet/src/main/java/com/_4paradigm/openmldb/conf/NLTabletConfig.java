package com._4paradigm.openmldb.conf;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Properties;

public class NLTabletConfig {
    public static int PORT = 9901;
    public static int WORKER_THREAD = 4;
    public static int IO_THREAD = 4;
    static {
        try {
            Properties prop = new Properties();
            prop.load(NLTabletConfig.class.getClassLoader().getResourceAsStream("openmldb-tablet.properties"));
            PORT = Integer.parseInt(prop.getProperty("server.port", "9901"));
            WORKER_THREAD = Integer.parseInt(prop.getProperty("server.worker_threads", "4"));
            IO_THREAD = Integer.parseInt(prop.getProperty("server.io_threads", "4"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
