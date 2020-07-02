package com._4paradigm.fesql_auto_test.common;

import com._4paradigm.fesql_auto_test.util.Tool;

import java.util.Properties;

/**
 * @author zhaowei
 * @date 2020/6/11 11:34 AM
 */
public class FesqlConfig {
    public static final String ZK_CLUSTER;
    public static final String ZK_ROOT_PATH;

    public static final Properties CONFIG = Tool.getProperties("fesql.properties");


    static{
        ZK_CLUSTER = CONFIG.getProperty(FesqlGlobalVar.env+"_zk_cluster");
        ZK_ROOT_PATH = CONFIG.getProperty(FesqlGlobalVar.env+"_zk_root_path");
    }
}
