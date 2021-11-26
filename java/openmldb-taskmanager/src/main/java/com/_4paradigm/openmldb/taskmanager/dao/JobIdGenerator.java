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

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

public class JobIdGenerator {
    private static final AtomicInteger jobId = new AtomicInteger();

    private volatile static ZKClient zkClient;

    private JobIdGenerator() {
    }

    public static int getUniqueJobID() throws Exception {
        synchronized (JobIdGenerator.class) {
            if (zkClient == null) {
                zkClient = new ZKClient(ZKConfig.builder()
                        .cluster(TaskManagerConfig.ZK_CLUSTER)
                        .namespace(TaskManagerConfig.ZK_ROOT_PATH)
                        .sessionTimeout(TaskManagerConfig.ZK_SESSION_TIMEOUT)
                        .baseSleepTime(TaskManagerConfig.ZK_BASE_SLEEP_TIME)
                        .connectionTimeout(TaskManagerConfig.ZK_CONNECTION_TIMEOUT)
                        .maxConnectWaitTime(TaskManagerConfig.ZK_MAX_CONNECT_WAIT_TIME)
                        .maxRetries(TaskManagerConfig.ZK_MAX_RETRIES)
                        .build());
                zkClient.connect();
                zkClient.createNode(
                        TaskManagerConfig.ZK_ROOT_PATH + "/TaskManager/max_oneBatch_job_id",
                       "0".getBytes(StandardCharsets.UTF_8));
                //fix: jobId.set(zkClient.getNodeValue);
                jobId.set(0);
            }
            if ((jobId.get()+1) % TaskManagerConfig.MAX_ONEBATCH_JOB_ID == 0) {
                zkClient.setNodeValue(TaskManagerConfig.ZK_ROOT_PATH + "/TaskManager/max_oneBatch_job_id",
                        String.valueOf(jobId.get()+1).getBytes(StandardCharsets.UTF_8));
                //fix: jobId.set(zkClient.getNodeValue);
                jobId.set(0);
            }
            return jobId.getAndIncrement();
        }
    }
}
