package com._4paradigm.openmldb.mysql.server;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ServerConfig {
    private static final String CONFIG_FILE_PATH = "server.properties";

    private static final Properties properties;

    static {
        properties = new Properties();

        // Load local properties file
        /*
        try {
            properties.load(ServerConfig.class.getClassLoader().getResourceAsStream(CONFIG_FILE_PATH));
        } catch (IOException e) {
            e.printStackTrace();
        }
         */

        try (FileInputStream input = new FileInputStream(CONFIG_FILE_PATH)) {
            properties.load(input);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static int getPort() {
        return Integer.parseInt(properties.getProperty("server.port", "3307"));
    }

    public static String getZkCluster() {
        return properties.getProperty("zookeeper.cluster", "0.0.0.0:2181");
    }

    public static String getZkRootPath() {
        return properties.getProperty("zookeeper.root_path", "/openmldb");
    }

    public static String getOpenmldbUser() {
        return properties.getProperty("openmldb.user", "root");
    }

    public static String getOpenmldbPassword() {
        return properties.getProperty("openmldb.password", "root");
    }

}
