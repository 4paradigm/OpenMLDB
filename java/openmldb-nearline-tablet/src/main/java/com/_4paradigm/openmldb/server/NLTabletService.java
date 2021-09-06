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

package com._4paradigm.openmldb.server;

import lombok.extern.slf4j.Slf4j;
import com._4paradigm.openmldb.conf.NLTabletConfig;
import com._4paradigm.openmldb.server.impl.NLTabletServerImpl;
import com.baidu.brpc.server.RpcServer;
import com.baidu.brpc.server.RpcServerOptions;

@Slf4j
public class NLTabletService {

    public static void main(String[] args) {
        try {

            RpcServerOptions options = new RpcServerOptions();
            options.setReceiveBufferSize(64 * 1024 * 1024);
            options.setSendBufferSize(64 * 1024 * 1024);
            options.setIoThreadNum(NLTabletConfig.WORKER_THREAD);
            options.setWorkThreadNum(NLTabletConfig.IO_THREAD);
            final RpcServer rpcServer = new RpcServer(NLTabletConfig.PORT, options);
            rpcServer.registerService(new NLTabletServerImpl());
            rpcServer.start();

            log.info("start nearLine tablet on {} with worker thread number {}", NLTabletConfig.PORT, NLTabletConfig.WORKER_THREAD);

            // make server keep running
            synchronized (NLTabletService.class) {
                try {
                    NLTabletService.class.wait();
                } catch (Throwable e) {
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("fail to start nearline tablet server");
        }
    }
}
