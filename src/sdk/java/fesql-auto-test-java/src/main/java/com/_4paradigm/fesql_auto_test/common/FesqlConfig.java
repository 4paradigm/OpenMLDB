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
    public static final List<Integer> FESQL_CASE_LEVELS;
    public static final String FESQL_CASE_PATH;
    public static final String FESQL_CASE_NAME;
    public static final String FESQL_CASE_ID;
    public static final String FESQL_CASE_DESC;
    public static final String TB_ENDPOINT_0;
    public static final String TB_ENDPOINT_1;
    public static final String TB_ENDPOINT_2;

    public static final Properties CONFIG = Tool.getProperties("fesql.properties");


    static {
        ZK_CLUSTER = CONFIG.getProperty(FesqlGlobalVar.env + "_zk_cluster");
        ZK_ROOT_PATH = CONFIG.getProperty(FesqlGlobalVar.env + "_zk_root_path");
        String levelStr = System.getProperty("caseLevel");
        levelStr = StringUtils.isEmpty(levelStr) ? "0,1,2,3,4,5" : levelStr;
        FESQL_CASE_LEVELS = Arrays.stream(levelStr.split(",")).map(Integer::parseInt).collect(Collectors.toList());
        FESQL_CASE_NAME = System.getProperty("caseName");
        FESQL_CASE_ID = System.getProperty("caseId");
        FESQL_CASE_DESC = System.getProperty("caseDesc");
        FESQL_CASE_PATH = System.getProperty("casePath");
        log.info("FESQL_CASE_LEVELS {}", FESQL_CASE_LEVELS);
        if (!StringUtils.isEmpty(FESQL_CASE_NAME)) {
            log.info("FESQL_CASE_NAME {}", FESQL_CASE_NAME);
        }
        if (!StringUtils.isEmpty(FESQL_CASE_ID)) {
            log.info("FESQL_CASE_ID {}", FESQL_CASE_ID);
        }
        if (!StringUtils.isEmpty(FESQL_CASE_PATH)) {
            log.info("FESQL_CASE_PATH {}", FESQL_CASE_PATH);
        }
        if (!StringUtils.isEmpty(FESQL_CASE_DESC)) {
            log.info("FESQL_CASE_DESC {}", FESQL_CASE_DESC);
        }
        TB_ENDPOINT_0 = CONFIG.getProperty(FesqlGlobalVar.env + "_tb_endpoint_0");
        TB_ENDPOINT_1 = CONFIG.getProperty(FesqlGlobalVar.env + "_tb_endpoint_1");
        TB_ENDPOINT_2 = CONFIG.getProperty(FesqlGlobalVar.env + "_tb_endpoint_2");
    }

    public static boolean isCluster() {
        return FesqlGlobalVar.env.equals("cluster");
    }
}
