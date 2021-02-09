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
