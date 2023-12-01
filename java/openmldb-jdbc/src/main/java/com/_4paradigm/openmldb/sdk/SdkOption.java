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

package com._4paradigm.openmldb.sdk;

import lombok.Data;

import com._4paradigm.openmldb.BasicRouterOptions;
import com._4paradigm.openmldb.SQLRouterOptions;
import com._4paradigm.openmldb.StandaloneOptions;

@Data
public class SdkOption {
    // TODO(hw): set isClusterMode automatically
    private boolean isClusterMode = true;
    // options for cluster mode
    private String zkCluster = "";
    private String zkPath = "";
    private long sessionTimeout = 10000;
    private String sparkConfPath = "";
    private int zkLogLevel = 3;
    private String zkLogFile = "";
    private String zkCert = "";

    // options for standalone mode
    private String host = "";
    private long port = -1;

    // base options
    private Boolean enableDebug = false;
    private long requestTimeout = 60000;
    private int glogLevel = 0;
    private String glogDir = "";
    private int maxSqlCacheSize = 50;

    private void buildBaseOptions(BasicRouterOptions opt) {
        opt.setEnable_debug(getEnableDebug());
        opt.setRequest_timeout(getRequestTimeout());
        opt.setGlog_level(getGlogLevel());
        opt.setGlog_dir(getGlogDir());
        opt.setMax_sql_cache_size(getMaxSqlCacheSize());
    }

    public SQLRouterOptions buildSQLRouterOptions() throws SqlException {
        if (!isClusterMode()) {
            return null;
        }
        SQLRouterOptions copt = new SQLRouterOptions();
        // required
        if (getZkCluster().isEmpty() || getZkPath().isEmpty()) {
            throw new SqlException("empty zk cluster or path");
        }
        copt.setZk_cluster(getZkCluster());
        copt.setZk_path(getZkPath());

        // optional
        copt.setZk_session_timeout(getSessionTimeout());
        copt.setSpark_conf_path(getSparkConfPath());
        copt.setZk_log_level(getZkLogLevel());
        copt.setZk_log_file(getZkLogFile());
        copt.setZk_cert(getZkCert());

        // base
        buildBaseOptions(copt);
        return copt;
    }

    public StandaloneOptions buildStandaloneOptions() throws SqlException {
        if (isClusterMode()) {
            return null;
        }
        StandaloneOptions sopt = new StandaloneOptions();
        // required
        if (getHost().isEmpty() || getPort() == -1) {
            throw new SqlException("empty host or unset port");
        }
        sopt.setHost(getHost());
        sopt.setPort(getPort());

        buildBaseOptions(sopt);
        return sopt;
    } 
}
