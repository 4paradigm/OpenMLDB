package com._4paradigm.openmldb.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com._4paradigm.openmldb.conf.NLTabletConfig;
import com._4paradigm.openmldb.server.impl.NLTabletServerImpl;
import com.baidu.brpc.server.RpcServer;
import com.baidu.brpc.server.RpcServerOptions;

public class NLTabletService {
    private final static Logger logger = LoggerFactory.getLogger(NLTabletService.class);

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

            logger.info("start NearLine Tablet on {} with worker thread number {}", NLTabletConfig.PORT, NLTabletConfig.WORKER_THREAD);

            // make server keep running
            synchronized (NLTabletService.class) {
                try {
                    NLTabletService.class.wait();
                } catch (Throwable e) {
                }
            }
        } catch (Exception e) {
            logger.error("fail to start dbms server", e);
        }
    }
}
