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

package com._4paradigm.openmldb.synctool;

import com._4paradigm.openmldb.sdk.SqlException;
import com.baidu.brpc.server.RpcServer;
import com.baidu.brpc.server.RpcServerOptions;
import lombok.extern.slf4j.Slf4j;
import lombok.Getter;

@Slf4j
public class SyncTool {
    private RpcServer rpcServer;
    @Getter
    private SyncToolImpl syncToolService;

    public void start() throws SqlException, InterruptedException {
        start(true);
    }

    public void start(boolean blocking) throws SqlException, InterruptedException {
        RpcServerOptions options = new RpcServerOptions();
        options.setReceiveBufferSize(64 * 1024 * 1024);
        options.setSendBufferSize(64 * 1024 * 1024);
        options.setIoThreadNum(SyncToolConfig.IO_THREAD);
        options.setWorkThreadNum(SyncToolConfig.WORKER_THREAD);
        rpcServer = new RpcServer(SyncToolConfig.HOST, SyncToolConfig.PORT, options);
        syncToolService = new SyncToolImpl(String.format("%s:%s",
                SyncToolConfig.HOST, SyncToolConfig.PORT));
        // recover before register service
        syncToolService.init();
        rpcServer.registerService(syncToolService);
        rpcServer.start();
        // with worker thread number?
        log.info("Start SyncTool on {}:{}", SyncToolConfig.HOST, SyncToolConfig.PORT);
        if (blocking) {
            // make server keep running
            synchronized (SyncTool.class) {
                try {
                    SyncTool.class.wait();
                } catch (Throwable e) {
                    log.error("Get exception when waiting, message: " + e.getMessage());
                    throw new RuntimeException(e);
                }
            }
        }

    }

    public void stop() {
        if (rpcServer != null) {
            rpcServer.shutdown();
        }
    }

    public static void main(String[] args) {
        SyncToolConfig.parse();
        SyncTool syncTool = new SyncTool();
        try {
            log.info("Start");
            System.out.println("Start SyncTool");
            syncTool.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
