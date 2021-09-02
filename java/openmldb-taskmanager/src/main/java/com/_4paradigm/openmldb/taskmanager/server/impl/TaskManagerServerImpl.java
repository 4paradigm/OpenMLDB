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

package com._4paradigm.openmldb.taskmanager.server.impl;

import com._4paradigm.openmldb.proto.TaskManager;
import com._4paradigm.openmldb.taskmanager.SparkYarnManager;
import com._4paradigm.openmldb.taskmanager.server.TaskManagerServer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TaskManagerServerImpl implements TaskManagerServer {

    public TaskManagerServerImpl() throws Exception {

    }

    public TaskManager.RunBatchSqlResponse runBatchSql(TaskManager.RunBatchSqlRequest request) {
        // TODO: Get default db name from request or client context
        String dbName = "taxitour5";

        SparkYarnManager.batchRunSql(request.getSql(), dbName, request.getOutputTableName());

        // TODO: Return with status code and message
        return TaskManager.RunBatchSqlResponse.newBuilder().build();
    }

}
