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

package com._4paradigm.openmldb.taskmanager.server;

import com._4paradigm.openmldb.proto.TaskManager;
import com.baidu.brpc.protocol.BrpcMeta;

public interface TaskManagerInterface {
    @BrpcMeta(serviceName = "openmldb.taskmanager.TaskManagerServer", methodName = "ShowJobs")
    TaskManager.ShowJobsResponse ShowJobs(TaskManager.ShowJobsRequest request);

    @BrpcMeta(serviceName = "openmldb.taskmanager.TaskManagerServer", methodName = "ShowJob")
    TaskManager.ShowJobResponse ShowJob(TaskManager.ShowJobRequest request);

    @BrpcMeta(serviceName = "openmldb.taskmanager.TaskManagerServer", methodName = "StopJob")
    TaskManager.StopJobResponse StopJob(TaskManager.StopJobRequest request);

    @BrpcMeta(serviceName = "openmldb.taskmanager.TaskManagerServer", methodName = "DeleteJob")
    TaskManager.DeleteJobResponse DeleteJob(TaskManager.DeleteJobRequest request);

    @BrpcMeta(serviceName = "openmldb.taskmanager.TaskManagerServer", methodName = "ShowBatchVersion")
    TaskManager.ShowJobResponse ShowBatchVersion(TaskManager.ShowBatchVersionRequest request);

    @BrpcMeta(serviceName = "openmldb.taskmanager.TaskManagerServer", methodName = "RunBatchSql")
    TaskManager.RunBatchSqlResponse RunBatchSql(TaskManager.RunBatchSqlRequest request);

    @BrpcMeta(serviceName = "openmldb.taskmanager.TaskManagerServer", methodName = "RunBatchAndShow")
    TaskManager.ShowJobResponse RunBatchAndShow(TaskManager.RunBatchAndShowRequest request);

    @BrpcMeta(serviceName = "openmldb.taskmanager.TaskManagerServer", methodName = "ImportOnlineData")
    TaskManager.ShowJobResponse ImportOnlineData(TaskManager.ImportOnlineDataRequest request);

    @BrpcMeta(serviceName = "openmldb.taskmanager.TaskManagerServer", methodName = "ImportOfflineData")
    TaskManager.ShowJobResponse ImportOfflineData(TaskManager.ImportOfflineDataRequest request);

    @BrpcMeta(serviceName = "openmldb.taskmanager.TaskManagerServer", methodName = "ExportOfflineData")
    TaskManager.ShowJobResponse ExportOfflineData(TaskManager.ExportOfflineDataRequest request);

    @BrpcMeta(serviceName = "openmldb.taskmanager.TaskManagerServer", methodName = "DropOfflineTable")
    TaskManager.DropOfflineTableResponse DropOfflineTable(TaskManager.DropOfflineTableRequest request);

    @BrpcMeta(serviceName = "openmldb.taskmanager.TaskManagerServer", methodName = "CreateFunction")
    TaskManager.CreateFunctionResponse CreateFunction(TaskManager.CreateFunctionRequest request);

    @BrpcMeta(serviceName = "openmldb.taskmanager.TaskManagerServer", methodName = "DropFunction")
    TaskManager.DropFunctionResponse DropFunction(TaskManager.DropFunctionRequest request);

    @BrpcMeta(serviceName = "openmldb.taskmanager.TaskManagerServer", methodName = "GetJobLog")
    TaskManager.GetJobLogResponse GetJobLog(TaskManager.GetJobLogRequest request);

    @BrpcMeta(serviceName = "openmldb.taskmanager.TaskManagerServer", methodName = "GetVersion")
    TaskManager.GetVersionResponse GetVersion(TaskManager.EmptyMessage request);

    @BrpcMeta(serviceName = "openmldb.taskmanager.TaskManagerServer", methodName = "SaveJobResult")
    TaskManager.SaveJobResultResponse SaveJobResult(TaskManager.SaveJobResultRequest request);
}
