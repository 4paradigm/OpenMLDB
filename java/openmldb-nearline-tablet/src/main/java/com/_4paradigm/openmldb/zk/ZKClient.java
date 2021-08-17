package com._4paradigm.openmldb.zk;

import com.google.protobuf.InvalidProtocolBufferException;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;

@Slf4j
public class ZKClient {
    private ZKConfig config;
    private CuratorFramework client;

    public ZKClient(ZKConfig config) {
        this.config = config;
    }

    public void connect() throws Exception {
        log.info("ZKClient connect with config: {}", config);
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(config.getBaseSleepTime(), config.getMaxRetries());
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(config.getCluster())
                .sessionTimeoutMs(config.getSessionTimeout())
                .connectionTimeoutMs(config.getConnectionTimeout())
                .retryPolicy(retryPolicy)
                .build();
        client.start();
        client.blockUntilConnected();
        client.getConnectionStateListenable();
        this.client = client;
    }

    public void createEphemeralNode(String path, byte[] data) throws Exception {
        String realPath = config.getNamespace() + "/" + path;
        if (client.checkExists().forPath(realPath) == null) {
            client.create().withMode(CreateMode.EPHEMERAL).forPath(realPath, data);
            log.info("create ephemeral node " + path);
        } else {
            log.error("create ephemeral node failed. node {} is exist", realPath);
        }
    }

    public void createNode(String path, byte[] data) throws Exception{
        if (client.checkExists().forPath(path) == null) {
            client.create().forPath(path, data);
        }
    }

    public void setNodeValue(String path, byte[] data) throws Exception{
        if (client.checkExists().forPath(path) == null) {
            client.create().forPath(path, data);
        } else {
            client.setData().forPath(path, data);
        }
    }
}
