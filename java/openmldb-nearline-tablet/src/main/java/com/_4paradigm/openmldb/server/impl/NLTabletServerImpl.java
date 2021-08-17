package com._4paradigm.openmldb.server.impl;

import com._4paradigm.openmldb.conf.NLTabletConfig;
import com._4paradigm.openmldb.server.NLTablet;
import com._4paradigm.openmldb.server.NLTabletServer;
import com._4paradigm.openmldb.zk.ZKClient;
import com._4paradigm.openmldb.zk.ZKConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

@Slf4j
public class NLTabletServerImpl implements NLTabletServer {
    private ZKClient zkClient;
    private static final String NLTabletPrefix = "NLTABLET_";

    public NLTabletServerImpl() throws Exception {
        try {
            connectZookeeper();
        } catch (Exception e) {
            log.error("init zk error");
            throw e;
        }
    }

    public void connectZookeeper() throws Exception {
        ZKConfig config = ZKConfig.builder()
                .cluster(NLTabletConfig.ZK_CLUSTER)
                .namespace(NLTabletConfig.ZK_ROOTPATH)
                .sessionTimeout(NLTabletConfig.ZK_SESSION_TIMEOUT)
                .build();
        zkClient = new ZKClient(config);
        zkClient.connect();
        String endpoint = NLTabletConfig.HOST + ":" + NLTabletConfig.PORT;
        String value = NLTabletPrefix + endpoint;
        zkClient.createEphemeralNode("nodes/" + value, value.getBytes());
    }

    @Override
    public NLTablet.CreateTableResponse createTable(NLTablet.CreateTableRequest request) {
        NLTablet.CreateTableResponse.Builder builder = NLTablet.CreateTableResponse.newBuilder();
        builder.setCode(0).setMsg("ok");
        NLTablet.CreateTableResponse response = builder.build();
        return response;
    }
}
