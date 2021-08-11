package com._4paradigm.openmldb.server;

import com.baidu.brpc.protocol.BrpcMeta;

public interface NLTabletServer {
    @BrpcMeta(serviceName = "openmldb.NLTabletServer", methodName = "CreateTable")
    NLTablet.CreateTableResponse createTable(NLTablet.CreateTableRequest request);
}
