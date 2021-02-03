package com._4paradigm.fesql_auto_test.common;

import com._4paradigm.fesql_auto_test.util.Tool;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * @author zhaowei
 * @date 2020/6/11 11:34 AM
 */
@Slf4j
public class FesqlConfig {
    public static final String ZK_CLUSTER;
    public static final String ZK_ROOT_PATH;
    public static final List<Integer> levels;
    public static final String TB_ENDPOINT_0;
    public static final String TB_ENDPOINT_1;
    public static final String TB_ENDPOINT_2;

    public static final Properties CONFIG = Tool.getProperties("fesql.properties");


    static{
        ZK_CLUSTER = CONFIG.getProperty(FesqlGlobalVar.env+"_zk_cluster");
        ZK_ROOT_PATH = CONFIG.getProperty(FesqlGlobalVar.env+"_zk_root_path");
        String levelStr = System.getProperty("caseLevel");
        levelStr = StringUtils.isEmpty(levelStr) ? "0" : levelStr;
        levels = Arrays.stream(levelStr.split(",")).map(Integer::parseInt).collect(Collectors.toList());
        log.info("FesqlConfig: levels: {}", levels);
        TB_ENDPOINT_0 = CONFIG.getProperty(FesqlGlobalVar.env+"_tb_endpoint_0");
        TB_ENDPOINT_1 = CONFIG.getProperty(FesqlGlobalVar.env+"_tb_endpoint_1");
        TB_ENDPOINT_2 = CONFIG.getProperty(FesqlGlobalVar.env+"_tb_endpoint_2");
    }
    public static boolean isCluster() {
        return FesqlGlobalVar.env.equals("cluster");
    }
}
