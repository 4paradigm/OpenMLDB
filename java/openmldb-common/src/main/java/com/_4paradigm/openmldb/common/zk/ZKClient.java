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

package com._4paradigm.openmldb.common.zk;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import java.util.concurrent.TimeUnit;

@Slf4j
public class ZKClient {
    private ZKConfig config;
    private CuratorFramework client;

    public ZKClient(ZKConfig config) {
        this.config = config;
    }

    public boolean connect() throws Exception {
        log.info("ZKClient connect with config: {}", config);
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(config.getBaseSleepTime(), config.getMaxRetries());
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(config.getCluster())
                .sessionTimeoutMs(config.getSessionTimeout())
                .connectionTimeoutMs(config.getConnectionTimeout())
                .retryPolicy(retryPolicy)
                .build();
        client.start();
        if (!client.blockUntilConnected(config.getMaxConnectWaitTime(), TimeUnit.MILLISECONDS)) {
            return false;
        }
        client.getConnectionStateListenable();
        this.client = client;
        return true;
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
