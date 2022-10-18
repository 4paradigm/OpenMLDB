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

package com._4paradigm.openmldb.test_common.openmldb;


import com._4paradigm.openmldb.test_common.util.Tool;
import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.testng.collections.Lists;
import org.testng.collections.Sets;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author zhaowei
 * @date 2020/6/11 11:45 AM
 */
@Slf4j
public class OpenMLDBGlobalVar {
    public static String env;
    public static String level;
    public static String version;
    public static String openMLDBPath;
    public static OpenMLDBInfo mainInfo;
    public static String dbName = "test_zw";
    public static String tableStorageMode = "memory";
    public static final List<Integer> CASE_LEVELS;
    public static final String CASE_NAME;
    public static final String CASE_ID;
    public static final String CASE_DESC;
    public static final String CASE_PATH;
    public static final String YAML_CASE_BASE_DIR;
    public static final Set<String> CREATE_DB_NAMES = Sets.newHashSet();

    public static String EXECUTE_MODE = "offline";

    public static final Properties CONFIG = Tool.getProperties("run_case.properties");

    static {
        String levelStr = System.getProperty("caseLevel");
        levelStr = StringUtils.isEmpty(levelStr) ? "0" : levelStr;
        CASE_LEVELS = Arrays.stream(levelStr.split(",")).map(Integer::parseInt).collect(Collectors.toList());
        CASE_NAME = System.getProperty("caseName");
        CASE_ID = System.getProperty("caseId");
        CASE_DESC = System.getProperty("caseDesc");
        CASE_PATH = System.getProperty("casePath");
        YAML_CASE_BASE_DIR = System.getProperty("yamlCaseBaseDir");
        log.info("CASE_LEVELS {}", CASE_LEVELS);
        if (!StringUtils.isEmpty(CASE_NAME)) {
            log.info("CASE_NAME {}", CASE_NAME);
        }
        if (!StringUtils.isEmpty(CASE_ID)) {
            log.info("CASE_ID {}", CASE_ID);
        }
        if (!StringUtils.isEmpty(CASE_PATH)) {
            log.info("CASE_PATH {}", CASE_PATH);
        }
        if (!StringUtils.isEmpty(CASE_DESC)) {
            log.info("CASE_DESC {}", CASE_DESC);
        }
        if (!StringUtils.isEmpty(YAML_CASE_BASE_DIR)) {
            log.info("YAML_CASE_BASE_DIR {}", YAML_CASE_BASE_DIR);
        }
        String tableStorageMode = CONFIG.getProperty("table_storage_mode");
        if(StringUtils.isNotEmpty(tableStorageMode)){
            OpenMLDBGlobalVar.tableStorageMode = tableStorageMode;
        }
        log.info("test tableStorageMode: {}", OpenMLDBGlobalVar.tableStorageMode);
    }
}
