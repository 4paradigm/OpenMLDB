package com._4paradigm.dataimporter.initialization;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class InitProperties {
    private static Logger logger = LoggerFactory.getLogger(InitProperties.class);
    private static Properties properties = new Properties();

    public static void initProperties() {
        try {
            // 1.加载config.properties配置文件
            String path = new File(System.getProperty("user.dir")) + "/config.properties";
            properties.load(new BufferedInputStream(new FileInputStream(new File(path))));
            logger.info("loading config succeeded");
        } catch (IOException e) {
            logger.error("loading config failed");
            e.printStackTrace();
        }
    }

    public static Properties getProperties() {
        return properties;
    }

}

