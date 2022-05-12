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

package com._4paradigm.sql;

import com._4paradigm.openmldb.sdk.SdkOption;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;

import java.util.Properties;

public class BenchmarkConfig {
    public static String ZK_CLUSTER = "127.0.0.1:6181";
    public static String ZK_PATH="/onebox";
    public static String REDIS_IP = "172.27.128.81";
    public static int REDIS_PORT = 6379;

    public static String VOLTDB_URL = "jdbc:voltdb://localhost:21212";

    // memsql can connect via mysql/maraiadb jdbc
    public static String MEMSQL_URL="jdbc:mariadb://localhost:3306";

    public static String DATABASE = "";
    public static String TABLE = "";
    public static int WINDOW_NUM = 2000;
    public static int PK_BASE = 1000000;
    public static long TS_BASE = 1652232079000l;
    public static String DEPLOY_NAME;
    public static String PARTITION_NUM = "4";
    public static int BATCH_SIZE = 1;
    public static Mode mode = Mode.REQUEST;
    public static int TIME_DIFF = 0;

    public static String ddlUrl;
    public static String scriptUrl;
    public static String relationUrl;
    public static String commonCol;

    private static SqlExecutor executor = null;
    private static SdkOption option = null;
    private static boolean NEED_PROXY= false;

    public static int PK_NUM = 1;
    public static int PUT_THREAD_NUM = 1;
    public static int QUERY_THREAD_NUM = 1;
    public static boolean NEED_CREATE = true;
    public static float REQUEST_RATIO = 1.0f;
    public static float PROCEDURE_RATIO = 1.0f;
    public static String METHOD;
    public static long RUNTIME = 1 * 60 * 1000;
    public static float INSERT_RATIO = 1.0f;
    static {
        try {
            Properties prop = new Properties();
            prop.load(BenchmarkConfig.class.getClassLoader().getResourceAsStream("conf.properties"));
            ZK_CLUSTER = prop.getProperty("ZK_CLUSTER");
            ZK_PATH = prop.getProperty("ZK_PATH");
            PARTITION_NUM = prop.getProperty("PARTITION_NUM");
            TS_BASE = Long.parseLong(prop.getProperty("TS_BASE"));
            PK_BASE = Integer.parseInt(prop.getProperty("PK_BASE"));
            ddlUrl = prop.getProperty("ddlUrl");
            NEED_PROXY = Boolean.valueOf(prop.getProperty("HTTP_PROXY"));
            scriptUrl = prop.getProperty("scriptUrl");
            relationUrl = prop.getProperty("relationUrl");
            DATABASE = prop.getProperty("DATABASE");
            TABLE = prop.getProperty("TABLE");
            DEPLOY_NAME = prop.getProperty("DEPLOY_NAME");
            WINDOW_NUM = Integer.valueOf(prop.getProperty("WINDOW_NUM"));
            BATCH_SIZE = Integer.valueOf((String)prop.get("BATCH_SIZE"));
            TIME_DIFF = Integer.valueOf((String)prop.getProperty("TIME_DIFF", "0"));
            String mode_str = prop.getProperty("MODE");
            if (mode_str.equals("batch")) {
                System.out.println("mode is batch");
                mode = Mode.BATCH;
            } else if (mode_str.equals("batchrequest")) {
                System.out.println("mode is batch request");
                mode = Mode.BATCH_REQUEST;
            } else {
                System.out.println("mode is request");
                mode = Mode.REQUEST;
            }
            commonCol = prop.getProperty("commonCol", "");
            PK_NUM = Integer.valueOf((String)prop.getProperty("PK_NUM", "100000"));
            PUT_THREAD_NUM = Integer.valueOf((String)prop.getProperty("PUT_THREAD_NUM", "1"));
            QUERY_THREAD_NUM = Integer.valueOf((String)prop.getProperty("QUERY_THREAD_NUM", "1"));

            if (prop.getProperty("NEED_CREATE", "").toLowerCase().equals("false")) {
                NEED_CREATE = false;
            }
            REQUEST_RATIO = Float.valueOf((String)prop.getProperty("REQUEST_RATIO", "1.0"));
            PROCEDURE_RATIO = Float.valueOf((String)prop.getProperty("PROCEDURE_RATIO", "1.0"));
            METHOD = prop.getProperty("METHOD", "insert");
            RUNTIME = Long.valueOf((String)prop.getProperty("RUNTIME", "1000"));
            INSERT_RATIO = Float.valueOf((String)prop.getProperty("INSERT_RATIO", "1.0"));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public enum Mode {
        REQUEST,
        BATCH_REQUEST,
        BATCH
    }

    public static boolean NeedProxy() {
        return NEED_PROXY;
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
        sdkOption.setRequestTimeout(10000000);
        option = sdkOption;
        try {
            executor = new SqlClusterExecutor(option);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return executor;
    }
}
