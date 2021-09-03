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
import com._4paradigm.openmldb.taskmanager.OpenmldbBatchjobManager;
import com._4paradigm.openmldb.taskmanager.server.TaskManagerServer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TaskManagerServerImpl implements TaskManagerServer {

    public TaskManagerServerImpl() throws Exception {

    }

    public TaskManager.YarnJobResponse runBatchSql(TaskManager.RunBatchSqlRequest request) {
        OpenmldbBatchjobManager.batchRunSql(request.getSql(), request.getDbName(), request.getOutputTableName());

        // TODO: Return with status code and message
        return TaskManager.YarnJobResponse.newBuilder().setCode(0).build();
    }

    @Override
    public TaskManager.YarnJobResponse importHdfsFile(TaskManager.ImportHdfsFileRequest request) {
        OpenmldbBatchjobManager.importHdfsFile(request.getFileType(), request.getFilePath(), request.getDbName(), request.getOutputTableName());
        return TaskManager.YarnJobResponse.newBuilder().setCode(0).build();
    }

    @Override
    public TaskManager.YarnJobStateResponse getYarnJobState(TaskManager.GetYarnJobStateRequest request) {
        String state = OpenmldbBatchjobManager.getJobState(request.getJobId());
        return TaskManager.YarnJobStateResponse.newBuilder().setCode(0).setState(state).build();
    }

}
