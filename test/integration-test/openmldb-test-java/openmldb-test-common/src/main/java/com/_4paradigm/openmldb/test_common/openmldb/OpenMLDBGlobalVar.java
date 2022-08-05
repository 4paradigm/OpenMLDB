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


import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBInfo;

import java.util.List;

/**
 * @author zhaowei
 * @date 2020/6/11 11:45 AM
 */
public class OpenMLDBGlobalVar {
    public static String env;
    public static String level;
    public static String version;
    public static String openMLDBPath;
    public static OpenMLDBInfo mainInfo;
    public static String dbName = "test_zw";
    public static String tableStorageMode = "memory";
    public static List<Integer> CASE_LEVELS;
    public static String CASE_NAME;
    public static String CASE_ID;
    public static String CASE_DESC;
    public static String CASE_PATH;
    public static String YAML_CASE_BASE_DIR;
}
