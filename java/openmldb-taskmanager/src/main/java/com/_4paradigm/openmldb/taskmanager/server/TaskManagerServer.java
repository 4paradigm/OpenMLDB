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

package com._4paradigm.openmldb.taskmanager.server;

import com._4paradigm.openmldb.taskmanager.zk.FailoverWatcher;
import lombok.extern.slf4j.Slf4j;
import com._4paradigm.openmldb.taskmanager.config.TaskManagerConfig;
import com._4paradigm.openmldb.taskmanager.server.impl.TaskManagerImpl;
import com.baidu.brpc.server.RpcServer;
import com.baidu.brpc.server.RpcServerOptions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

@Slf4j
public class TaskManagerServer {
    private static final Log logger = LogFactory.getLog(TaskManagerServer.class);

    private RpcServer rpcServer;

    public void start() {
        try {
            FailoverWatcher failoverWatcher = new FailoverWatcher();

            logger.info("The server runs and prepares for leader election");
            if (failoverWatcher.blockUntilActive()) {
                logger.info("The server becomes active master and prepare to do business logic");
                startBrpcServer();
            }
            failoverWatcher.close();
            logger.info("The server exits after running business logic");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void startBrpcServer() {
        try {
            RpcServerOptions options = new RpcServerOptions();
            options.setReceiveBufferSize(64 * 1024 * 1024);
            options.setSendBufferSize(64 * 1024 * 1024);
            options.setIoThreadNum(TaskManagerConfig.WORKER_THREAD);
            options.setWorkThreadNum(TaskManagerConfig.IO_THREAD);
            rpcServer = new RpcServer(TaskManagerConfig.PORT, options);
            rpcServer.registerService(new TaskManagerImpl());
            rpcServer.start();
            log.info("Start TaskManager on {} with worker thread number {}", TaskManagerConfig.PORT, TaskManagerConfig.WORKER_THREAD);

            // make server keep running
            synchronized (TaskManagerServer.class) {
                try {
                    TaskManagerServer.class.wait();
                } catch (Throwable e) {
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Fail to start TaskManager, " + e.getMessage());
        }
    }

    public void shutdown() {
        rpcServer.shutdown();
        rpcServer = null;
    }

    public static void main(String[] args) {
        TaskManagerServer server = new TaskManagerServer();
        server.start();
    }

}
