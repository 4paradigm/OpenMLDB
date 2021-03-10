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

package com._4paradigm.fesql_auto_test.common;

import com._4paradigm.fesql_auto_test.entity.FEDBInfo;
import com._4paradigm.sql.sdk.SdkOption;
import com._4paradigm.sql.sdk.SqlException;
import com._4paradigm.sql.sdk.SqlExecutor;
import com._4paradigm.sql.sdk.impl.SqlClusterExecutor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 * @author zhaowei
 * @date 2020/6/11 11:28 AM
 */
@Data
@Slf4j
public class FesqlClient {

    private SqlExecutor executor;

    public FesqlClient(String zkCluster,String zkPath){
        SdkOption option = new SdkOption();
        option.setZkCluster(zkCluster);
        option.setZkPath(zkPath);
        option.setEnableDebug(true);
        option.setSessionTimeout(1000000);
        option.setRequestTimeout(1000000);
        log.info("zkCluster {}, zkPath {}", option.getZkCluster(), option.getZkPath());
        try {
            executor = new SqlClusterExecutor(option);
        } catch (SqlException e) {
            e.printStackTrace();
        }
    }
    public FesqlClient(FEDBInfo fedbInfo){
        this(fedbInfo.getZk_cluster(),fedbInfo.getZk_root_path());
    }
}
