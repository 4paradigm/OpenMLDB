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

package com._4paradigm.openmldb.taskmanager.dao;

import com._4paradigm.openmldb.common.zk.ZKClient;
import com._4paradigm.openmldb.common.zk.ZKConfig;
import com._4paradigm.openmldb.taskmanager.config.TaskManagerConfig;

public class JobIdGenerator {
    private volatile static int maxJobId;
    private volatile static int currentJobId;
    private volatile static ZKClient zkClient;

    static {
        try {
            zkClient = new ZKClient(ZKConfig.builder()
                    .cluster(TaskManagerConfig.getZkCluster())
                    .namespace(TaskManagerConfig.getZkRootPath())
                    .sessionTimeout(TaskManagerConfig.getZkSessionTimeout())
                    .baseSleepTime(TaskManagerConfig.getZkBaseSleepTime())
                    .connectionTimeout(TaskManagerConfig.getZkConnectionTimeout())
                    .maxConnectWaitTime(TaskManagerConfig.getZkMaxConnectWaitTime())
                    .maxRetries(TaskManagerConfig.getZkMaxRetries())
                    .build());
            zkClient.connect();

            // Initialize zk nodes
            zkClient.createNode(TaskManagerConfig.getZkRootPath(), "".getBytes());
            zkClient.createNode(TaskManagerConfig.getZkTaskmanagerPath(), "".getBytes());

            int lastMaxJobId = 0;
            if (zkClient.checkExists(TaskManagerConfig.getZkMaxJobIdPath())) {
                // Get last max job id from zk
                lastMaxJobId = Integer.parseInt(zkClient.getNodeValue(TaskManagerConfig.getZkMaxJobIdPath()));
            }
            currentJobId = lastMaxJobId;
            maxJobId = lastMaxJobId + TaskManagerConfig.getPrefetchJobidNum();
            // set max job id in zk
            zkClient.setNodeValue(TaskManagerConfig.getZkMaxJobIdPath(), String.valueOf(maxJobId).getBytes());

        } catch (Exception e) {
            zkClient = null;
            e.printStackTrace();


        }
    }

    private JobIdGenerator() {
    }

    public static int getUniqueId() throws Exception {
        synchronized (JobIdGenerator.class) {
            currentJobId += 1;
            if (currentJobId > maxJobId) {
                // Update zk before returning job id
                maxJobId += TaskManagerConfig.getPrefetchJobidNum();
                zkClient.setNodeValue(TaskManagerConfig.getZkMaxJobIdPath(), String.valueOf(maxJobId).getBytes());
            }
            return currentJobId;
        }
    }
}
