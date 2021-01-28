package com._4paradigm.fesql_auto_test.common;

import com._4paradigm.fesql_auto_test.util.Tool;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * @author zhaowei
 * @date 2020/6/11 11:34 AM
 */
public class FesqlConfig {
    public static final String ZK_CLUSTER;
    public static final String ZK_ROOT_PATH;
    public static final List<Integer> levels;

    public static final Properties CONFIG = Tool.getProperties("fesql.properties");


    static{
        ZK_CLUSTER = CONFIG.getProperty(FesqlGlobalVar.env+"_zk_cluster");
        ZK_ROOT_PATH = CONFIG.getProperty(FesqlGlobalVar.env+"_zk_root_path");
        String levelStr = CONFIG.getProperty(FesqlGlobalVar.env+"_test.case.level");
        levels = Arrays.stream(levelStr.split(",")).map(Integer::parseInt).collect(Collectors.toList());
    }
    public static boolean isCluster() {
        return FesqlGlobalVar.env.equals("cluster");
    }
}
