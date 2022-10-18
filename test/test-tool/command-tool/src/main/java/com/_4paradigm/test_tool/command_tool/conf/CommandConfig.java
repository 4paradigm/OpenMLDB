package com._4paradigm.test_tool.command_tool.conf;
import com._4paradigm.test_tool.command_tool.util.PropertiesUtils;

import java.util.Properties;

public class CommandConfig {
    public static String env = "qa";
    public static final boolean IS_REMOTE;
    public static final String REMOTE_IP;
    public static final String REMOTE_USER;
    public static final String REMOTE_PASSWORD;
    public static final String REMOTE_PRIVATE_KEY_PATH;

    public static final Properties CONFIG = PropertiesUtils.getProperties("command.properties");


    static{
        IS_REMOTE = Boolean.parseBoolean(CONFIG.getProperty("is_remote","false"));
        String remote_ip = CONFIG.getProperty(env+"_remote_ip");
        String remote_user = CONFIG.getProperty(env+"_remote_user");
        String remote_password = CONFIG.getProperty(env+"_remote_password");
        String remote_private_key_path = CONFIG.getProperty(env+"_remote_private_key_path");
        REMOTE_IP = remote_ip!=null?remote_ip: CONFIG.getProperty("remote_ip");
        REMOTE_USER = remote_user!=null?remote_user: CONFIG.getProperty("remote_user");
        REMOTE_PASSWORD = remote_password!=null?remote_password: CONFIG.getProperty("remote_password");
        REMOTE_PRIVATE_KEY_PATH = remote_private_key_path!=null?remote_private_key_path: CONFIG.getProperty("remote_private_key_path");;
    }
}
