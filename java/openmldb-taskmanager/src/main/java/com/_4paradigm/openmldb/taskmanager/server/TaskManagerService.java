package com._4paradigm.openmldb.taskmanager.server;

import lombok.extern.slf4j.Slf4j;
import com._4paradigm.openmldb.conf.NLTabletConfig;
import com._4paradigm.openmldb.taskmanager.server.impl.TaskManagerServerImpl;
import com.baidu.brpc.server.RpcServer;
import com.baidu.brpc.server.RpcServerOptions;

@Slf4j
public class TaskManagerService {

    public static void main(String[] args) {
        try {

            RpcServerOptions options = new RpcServerOptions();
            options.setReceiveBufferSize(64 * 1024 * 1024);
            options.setSendBufferSize(64 * 1024 * 1024);
            options.setIoThreadNum(NLTabletConfig.WORKER_THREAD);
            options.setWorkThreadNum(NLTabletConfig.IO_THREAD);
            final RpcServer rpcServer = new RpcServer(NLTabletConfig.PORT, options);
            rpcServer.registerService(new TaskManagerServerImpl());
            rpcServer.start();

            //log.info("start nearLine tablet on {} with worker thread number {}", NLTabletConfig.PORT, NLTabletConfig.WORKER_THREAD);

            // make server keep running
            synchronized (TaskManagerService.class) {
                try {
                    TaskManagerService.class.wait();
                } catch (Throwable e) {
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            //log.error("fail to start nearline tablet server");
        }
    }
}
