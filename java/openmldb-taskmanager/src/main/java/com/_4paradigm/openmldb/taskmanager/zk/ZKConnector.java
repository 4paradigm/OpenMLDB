package com._4paradigm.openmldb.taskmanager.zk;

import com._4paradigm.openmldb.common.zk.ZKClient;
import com._4paradigm.openmldb.common.zk.ZKConfig;
import com._4paradigm.openmldb.taskmanager.config.TaskManagerConfig;

public class ZKConnector {
    private ZKConfig zkConfig;
    private ZKClient zkClient;

    public ZKConnector() {
        // TODO: add config for zkConfig builder
        this.zkConfig = ZKConfig.builder()
                .cluster(TaskManagerConfig.ZK_CLUSTER)
                .namespace(TaskManagerConfig.ZK_ROOTPATH)
                .sessionTimeout(TaskManagerConfig.ZK_SESSION_TIMEOUT)
                .baseSleepTime(TaskManagerConfig.ZK_BASE_SLEEP_TIME)
                .connectionTimeout(TaskManagerConfig.ZK_CONNECTION_TIMEOUT)
                .maxConnectWaitTime(TaskManagerConfig.ZK_MAX_CONNECT_WAIT_TIME)
                .maxRetries(TaskManagerConfig.ZK_MAX_RETRIES)
                .build();
        zkClient = new ZKClient(this.zkConfig);
    }
    public ZKConnector(ZKConfig config) {
        this.zkConfig = config;
        zkClient = new ZKClient(this.zkConfig);
    }
    public ZKClient getZkClient() {
        return zkClient;
    }
}
