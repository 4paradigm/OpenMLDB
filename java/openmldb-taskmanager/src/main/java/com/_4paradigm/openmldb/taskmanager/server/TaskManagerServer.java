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

import com._4paradigm.openmldb.taskmanager.config.ConfigException;
import com._4paradigm.openmldb.taskmanager.tracker.JobTrackerService;
import com._4paradigm.openmldb.taskmanager.util.VersionUtil;
import com._4paradigm.openmldb.taskmanager.zk.FailoverWatcher;
import lombok.extern.slf4j.Slf4j;
import com._4paradigm.openmldb.taskmanager.config.TaskManagerConfig;
import com._4paradigm.openmldb.taskmanager.server.impl.TaskManagerImpl;
import com.baidu.brpc.server.RpcServer;
import com.baidu.brpc.server.RpcServerOptions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.io.IOException;

/**
 * The RPC server implementation of TaskManager.
 */
@Slf4j
public class TaskManagerServer {
    private static final Log logger = LogFactory.getLog(TaskManagerServer.class);

    // The RPC server object
    private RpcServer rpcServer;

    /**
     * Read and parse the config file from classpath.
     *
     * @throws ConfigException if config file does not exist or some configs are incorrect.
     */
    public TaskManagerServer() throws ConfigException {
        TaskManagerConfig.parse();
    }

    /**
     * Register leader in ZooKeeper and start the RPC server.
     *
     * @throws IOException if it fails to start.
     */
    public void start() throws ConfigException, IOException, InterruptedException {
        this.start(true);
    }

    /**
     * Register leader in ZooKeeper and start the RPC server.
     *
     * @param blocking if it will block the current thread.
     * @throws IOException if it fails to start.
     */
    public void start(Boolean blocking) throws ConfigException, IOException, InterruptedException {
        FailoverWatcher failoverWatcher = new FailoverWatcher();

        logger.info("The server runs and prepares for leader election");
        if (failoverWatcher.blockUntilActive()) {
            logger.info("The server becomes active master and prepare to do business logic");
            if (TaskManagerConfig.TRACK_UNFINISHED_JOBS) {
                // Start threads to track unfinished jobs
                JobTrackerService.startTrackerThreads();
            }

            // Start brpc server
            startRpcServer(blocking);
        }
        failoverWatcher.close();
        logger.info("The server exits after running business logic");
    }

    /**
     * Start the underlying bRPC server.
     */
    public void startRpcServer() throws ConfigException, InterruptedException {
        this.startRpcServer(true);
    }

    /**
     * Start the underlying bRPC server.
     *
     * @param blocking if it will block the current thread.
     */
    public void startRpcServer(Boolean blocking) throws ConfigException, InterruptedException {
        RpcServerOptions options = new RpcServerOptions();
        options.setReceiveBufferSize(64 * 1024 * 1024);
        options.setSendBufferSize(64 * 1024 * 1024);
        options.setIoThreadNum(TaskManagerConfig.WORKER_THREAD);
        options.setWorkThreadNum(TaskManagerConfig.IO_THREAD);
        options.setKeepAliveTime(TaskManagerConfig.CHANNEL_KEEP_ALIVE_TIME);
        rpcServer = new RpcServer(TaskManagerConfig.PORT, options);
        rpcServer.registerService(new TaskManagerImpl());
        rpcServer.start();
        log.info("Start TaskManager on {} with worker thread number {}", TaskManagerConfig.PORT,
                TaskManagerConfig.WORKER_THREAD);

        if (blocking) {
            // make server keep running
            synchronized (TaskManagerServer.class) {
                try {
                    TaskManagerServer.class.wait();
                } catch (Throwable e) {
                    logger.warn("Get exception when waiting, message: " + e.getMessage());
                }
            }
        }
    }

    /**
     * Close and shutdown the RPC server.
     */
    public void close() {
        if (rpcServer != null) {
            rpcServer.shutdown();
            rpcServer = null;
        }
    }

    /**
     * The main function of TaskManager server.
     *
     * @param args
     */
    public static void main(String[] args) {
        try {
            TaskManagerServer server = new TaskManagerServer();
            server.start();
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(String.format("Fail to start TaskManager, message: ", e.getMessage()));
        }
    }

}
