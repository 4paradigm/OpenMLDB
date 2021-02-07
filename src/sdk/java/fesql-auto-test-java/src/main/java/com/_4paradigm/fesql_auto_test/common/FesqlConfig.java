package com._4paradigm.fesql_auto_test.common;

import com._4paradigm.fesql_auto_test.entity.FEDBInfo;
import com._4paradigm.fesql_auto_test.util.DeployUtil;
import com._4paradigm.fesql_auto_test.util.Tool;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.testng.collections.Lists;

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
    // public static final String TB_ENDPOINT_0;
    // public static final String TB_ENDPOINT_1;
    // public static final String TB_ENDPOINT_2;
    public static final List<String> VERSIONS;
    public static final FEDBInfo mainInfo;
    public static final String BASE_PATH;
    public static boolean INIT_VERSION_ENV = true;

    public static final Properties CONFIG = Tool.getProperties("fesql.properties");

    static{
        ZK_CLUSTER = CONFIG.getProperty(FesqlGlobalVar.env+"_zk_cluster");
        ZK_ROOT_PATH = CONFIG.getProperty(FesqlGlobalVar.env+"_zk_root_path");
        String levelStr = System.getProperty("caseLevel");
        levelStr = StringUtils.isEmpty(levelStr) ? "0" : levelStr;
        levels = Arrays.stream(levelStr.split(",")).map(Integer::parseInt).collect(Collectors.toList());
        log.info("FesqlConfig: levels: {}", levels);
        BASE_PATH = CONFIG.getProperty(FesqlGlobalVar.env+"_base_path");
        String tb_endpoint_0 = CONFIG.getProperty(FesqlGlobalVar.env+"_tb_endpoint_0");
        String tb_endpoint_1 = CONFIG.getProperty(FesqlGlobalVar.env+"_tb_endpoint_1");
        String tb_endpoint_2 = CONFIG.getProperty(FesqlGlobalVar.env+"_tb_endpoint_2");
        String versionStr = System.getProperty("fedbVersion");
        if(StringUtils.isEmpty(versionStr)){
            versionStr = CONFIG.getProperty(FesqlGlobalVar.env+"_versions");
        }
        VERSIONS = Arrays.stream(versionStr.split(",")).collect(Collectors.toList());
        log.info("FesqlConfig: versions: {}", VERSIONS);
        mainInfo = FEDBInfo.builder()
                .nsNum(2)
                .tabletNum(3)
                .zk_cluster(ZK_CLUSTER)
                .tabletEndpoints(Lists.newArrayList(tb_endpoint_0,tb_endpoint_1,tb_endpoint_2))
                .zk_root_path(ZK_ROOT_PATH)
                .build();
        String init_env = CONFIG.getProperty(FesqlGlobalVar.env+"_init_version_env");
        if(StringUtils.isNotEmpty(init_env)){
            INIT_VERSION_ENV = Boolean.parseBoolean(init_env);
        }
    }
    public static boolean isCluster() {
        return FesqlGlobalVar.env.equals("cluster");
    }
}
