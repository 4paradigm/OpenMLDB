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


import com._4paradigm.openmldb.test_common.util.Tool;
import lombok.extern.slf4j.Slf4j;

import java.util.Properties;

/**
 * @author zhaowei
 * @date 2020/6/11 11:34 AM
 */
@Slf4j
public class OpenMLDBVersionConfig {

    public static final Properties CONFIG = Tool.getProperties("fesql_version.properties");

    public static String getUrl(String version){
        return CONFIG.getProperty(version);
    }
}
