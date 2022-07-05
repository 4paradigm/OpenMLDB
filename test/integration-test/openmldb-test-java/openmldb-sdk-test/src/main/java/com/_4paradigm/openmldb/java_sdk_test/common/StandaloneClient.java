package com._4paradigm.openmldb.java_sdk_test.common;

import com._4paradigm.openmldb.sdk.SdkOption;
import com._4paradigm.openmldb.sdk.SqlException;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;
import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBInfo;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
public class StandaloneClient {

    private SqlExecutor executor;

    public StandaloneClient(String host, Integer port){
        SdkOption option = new SdkOption();
        option.setHost(host);
        option.setPort(port);
        option.setClusterMode(false);
        option.setSessionTimeout(10000);
        option.setRequestTimeout(60000);
        log.info("host {}, port {}", option.getHost(), option.getPort());
        try {
            executor = new SqlClusterExecutor(option);
        } catch (SqlException e) {
            e.printStackTrace();
        }
    }
    public StandaloneClient(OpenMLDBInfo fedbInfo){
        this(fedbInfo.getHost(),fedbInfo.getPort());
    }
}
