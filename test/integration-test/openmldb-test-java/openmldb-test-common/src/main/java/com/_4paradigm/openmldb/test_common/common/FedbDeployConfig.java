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

package com._4paradigm.openmldb.test_common.common;


import com._4paradigm.openmldb.test_common.util.DeployUtil;
import com._4paradigm.openmldb.test_common.util.FedbTool;
import lombok.extern.slf4j.Slf4j;

import java.util.Properties;

/**
 * @author zhaowei
 * @date 2020/6/11 11:34 AM
 */
@Slf4j
public class FedbDeployConfig {

    public static final String ZK_URL;
    public static final String SPARK_URL;
    public static final Properties CONFIG;

    static {
        CONFIG = FedbTool.getProperties("fedb_deploy.properties");
        ZK_URL = CONFIG.getProperty("zk_url");
        SPARK_URL = CONFIG.getProperty("spark_url");
    }

    public static String getUrl(String version){
        return CONFIG.getProperty(version, DeployUtil.getOpenMLDBUrl(version));
    }
    public static String getZKUrl(String version){
        return CONFIG.getProperty(version+"_zk_url", ZK_URL);
    }
    public static String getSparkUrl(String version){
        return CONFIG.getProperty(version+"_spark_url", SPARK_URL);
    }
}
