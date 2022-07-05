/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com._4paradigm.openmldb.java_sdk_test.common;

import com._4paradigm.openmldb.test_common.openmldb.OpenMLDBGlobalVar;
import com._4paradigm.openmldb.test_common.util.Tool;
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
public class FedbConfig {
    // public static final String ZK_CLUSTER;
    // public static final String ZK_ROOT_PATH;
    public static final List<String> VERSIONS;
    // public static final FEDBInfo mainInfo;
    public static final String BASE_PATH;
    public static boolean INIT_VERSION_ENV = true;
    public static final List<Integer> FESQL_CASE_LEVELS;
    public static final String FESQL_CASE_PATH;
    public static final String FESQL_CASE_NAME;
    public static final String FESQL_CASE_ID;
    public static final String FESQL_CASE_DESC;
    public static final String YAML_CASE_BASE_DIR;
    public static final boolean ADD_REPORT_LOG;
    public static final String ZK_URL;

    public static final Properties CONFIG = Tool.getProperties("fesql.properties");

    static {
        // ZK_CLUSTER = CONFIG.getProperty(FedbGlobalVar.env + "_zk_cluster");
        // ZK_ROOT_PATH = CONFIG.getProperty(FedbGlobalVar.env + "_zk_root_path");
        String levelStr = System.getProperty("caseLevel");
        levelStr = StringUtils.isEmpty(levelStr) ? "0" : levelStr;
        FESQL_CASE_LEVELS = Arrays.stream(levelStr.split(",")).map(Integer::parseInt).collect(Collectors.toList());
        FESQL_CASE_NAME = System.getProperty("caseName");
        FESQL_CASE_ID = System.getProperty("caseId");
        FESQL_CASE_DESC = System.getProperty("caseDesc");
        FESQL_CASE_PATH = System.getProperty("casePath");
        YAML_CASE_BASE_DIR = System.getProperty("yamlCaseBaseDir");
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
        if (!StringUtils.isEmpty(YAML_CASE_BASE_DIR)) {
            log.info("YAML_CASE_BASE_DIR {}", YAML_CASE_BASE_DIR);
        }

        BASE_PATH = CONFIG.getProperty(OpenMLDBGlobalVar.env + "_base_path");
        // String tb_endpoint_0 = CONFIG.getProperty(FedbGlobalVar.env + "_tb_endpoint_0");
        // String tb_endpoint_1 = CONFIG.getProperty(FedbGlobalVar.env + "_tb_endpoint_1");
        // String tb_endpoint_2 = CONFIG.getProperty(FedbGlobalVar.env + "_tb_endpoint_2");
        String versionStr = System.getProperty("fedbVersion");
        if (StringUtils.isEmpty(versionStr)) {
            versionStr = CONFIG.getProperty(OpenMLDBGlobalVar.env + "_versions");
        }
        if (StringUtils.isNotEmpty(versionStr)) {
            VERSIONS = Arrays.stream(versionStr.split(",")).collect(Collectors.toList());
        } else {
            VERSIONS = Lists.newArrayList();
        }
        log.info("HybridSEConfig: versions: {}", VERSIONS);
        ZK_URL = CONFIG.getProperty("zk_url");
        String reportLogStr = System.getProperty("reportLog");
        if(StringUtils.isNotEmpty(reportLogStr)){
            ADD_REPORT_LOG = Boolean.parseBoolean(reportLogStr);
        }else{
            ADD_REPORT_LOG = true;
        }
        String init_env = CONFIG.getProperty(OpenMLDBGlobalVar.env + "_init_version_env");
        if (StringUtils.isNotEmpty(init_env)) {
            INIT_VERSION_ENV = Boolean.parseBoolean(init_env);
        }
    }

    public static boolean isCluster() {
        return OpenMLDBGlobalVar.env.equals("cluster");
    }

}
