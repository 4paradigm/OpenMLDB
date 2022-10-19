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
public class OpenMLDBConfig {
    public static final List<String> VERSIONS;
    public static boolean INIT_VERSION_ENV = true;
    public static final boolean ADD_REPORT_LOG;
    public static final String TEST_DB;

    public static final Properties CONFIG = Tool.getProperties("run_case.properties");

    static {
        String versionStr = System.getProperty("diffVersion");
        if (StringUtils.isEmpty(versionStr)) {
            versionStr = CONFIG.getProperty(OpenMLDBGlobalVar.env + "_versions");
        }
        if (StringUtils.isNotEmpty(versionStr)) {
            VERSIONS = Arrays.stream(versionStr.split(",")).collect(Collectors.toList());
        } else {
            VERSIONS = Lists.newArrayList();
        }
        log.info("HybridSEConfig: versions: {}", VERSIONS);
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

        String version = CONFIG.getProperty("version");
        if(StringUtils.isNotEmpty(version)){
            OpenMLDBGlobalVar.version = version;
        }
        log.info("test version: {}", OpenMLDBGlobalVar.version);
        TEST_DB = CONFIG.getProperty("test_db","test_sdk");
        log.info("test test_db: {}", TEST_DB);
    }

    public static boolean isCluster() {
        return OpenMLDBGlobalVar.env.equals("cluster");
    }

}
