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

package com._4paradigm.openmldb.benchmark;

import com._4paradigm.openmldb.sdk.SdkOption;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;

import java.util.Properties;

public class BenchmarkConfig {
    public static String ZK_CLUSTER = "127.0.0.1:6181";
    public static String ZK_PATH="/onebox";

    public static String DATABASE = "";
    public static int WINDOW_NUM = 2;
    public static int WINDOW_SIZE = 1000;
    public static int JOIN_NUM = 2;
    public static int PK_BASE = 1000000;
    public static long TS_BASE = 1652232079000l;
    public static String DEPLOY_NAME;
    public static String CSV_PATH;

    private static SqlExecutor executor = null;
    private static SdkOption option = null;

    public static int PK_NUM = 1;
    public static int PK_MAX = 0;
    static {
        try {
            Properties prop = new Properties();
            prop.load(BenchmarkConfig.class.getClassLoader().getResourceAsStream("conf.properties"));
            ZK_CLUSTER = prop.getProperty("ZK_CLUSTER");
            ZK_PATH = prop.getProperty("ZK_PATH");
            TS_BASE = Long.parseLong(prop.getProperty("TS_BASE"));
            PK_BASE = Integer.parseInt(prop.getProperty("PK_BASE"));
            DATABASE = prop.getProperty("DATABASE");
            DEPLOY_NAME = prop.getProperty("DEPLOY_NAME");
            WINDOW_NUM = Integer.valueOf(prop.getProperty("WINDOW_NUM"));
            WINDOW_SIZE = Integer.valueOf(prop.getProperty("WINDOW_SIZE"));
            JOIN_NUM = Integer.valueOf(prop.getProperty("JOIN_NUM"));
            PK_NUM = Integer.valueOf(prop.getProperty("PK_NUM", "100000"));
            PK_MAX = Integer.valueOf(prop.getProperty("PK_MAX", "0"));
            CSV_PATH = prop.getProperty("CSV_PATH");
//            if(!CSV_PATH.startsWith("/")){
//                CSV_PATH=Util.getRootPath()+CSV_PATH;
//            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static SqlExecutor GetSqlExecutor(boolean enableDebug) {
        if (executor != null) {
            return executor;
        }
        SdkOption sdkOption = new SdkOption();
        sdkOption.setSessionTimeout(30000);
        sdkOption.setZkCluster(BenchmarkConfig.ZK_CLUSTER);
        sdkOption.setZkPath(BenchmarkConfig.ZK_PATH);
        sdkOption.setEnableDebug(enableDebug);
        sdkOption.setRequestTimeout(1000000);
        option = sdkOption;
        try {
            executor = new SqlClusterExecutor(option);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return executor;
    }
}
