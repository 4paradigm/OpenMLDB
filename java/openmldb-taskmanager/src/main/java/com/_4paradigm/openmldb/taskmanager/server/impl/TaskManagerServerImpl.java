package com._4paradigm.openmldb.taskmanager.server.impl;

import com._4paradigm.openmldb.proto.TaskManager;
import com._4paradigm.openmldb.taskmanager.server.TaskManagerServer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TaskManagerServerImpl implements TaskManagerServer {

    public TaskManagerServerImpl() throws Exception {

    }

    public TaskManager.RunBatchSqlResponse runBatchSql(TaskManager.RunBatchSqlRequest request) {
        return null;
    }

}
