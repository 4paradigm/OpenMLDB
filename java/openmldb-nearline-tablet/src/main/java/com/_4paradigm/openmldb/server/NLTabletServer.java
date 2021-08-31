package com._4paradigm.openmldb.server;

import com._4paradigm.openmldb.proto.NLTablet;
import com.baidu.brpc.protocol.BrpcMeta;

public interface NLTabletServer {
    @BrpcMeta(serviceName = "openmldb.nltablet.NLTabletServer", methodName = "CreateTable")
    NLTablet.CreateTableResponse createTable(NLTablet.CreateTableRequest request);
}
