package com._4paradigm.openmldb.taskmanager.server;

import com._4paradigm.openmldb.proto.TaskManager;
import com.baidu.brpc.protocol.BrpcMeta;

public interface TaskManagerServer {
    @BrpcMeta(serviceName = "openmldb.taskmanager.TaskManagerServer", methodName = "RunBatchSql")
    TaskManager.RunBatchSqlResponse runBatchSql(TaskManager.RunBatchSqlRequest request);
}
