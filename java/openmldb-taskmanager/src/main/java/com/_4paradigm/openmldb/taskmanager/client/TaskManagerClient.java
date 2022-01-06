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

package com._4paradigm.openmldb.taskmanager.client;

import com._4paradigm.openmldb.proto.TaskManager;
import com._4paradigm.openmldb.taskmanager.server.TaskManagerInterface;
import com.baidu.brpc.RpcContext;
import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.RpcClientOptions;
import com.baidu.brpc.interceptor.Interceptor;
import com.baidu.brpc.loadbalance.LoadBalanceStrategy;
import com.baidu.brpc.protocol.Options;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/***
 * Java wrapper for TaskManager brpc client.
 */

public class TaskManagerClient {

    private RpcClient rpcClient;
    private TaskManagerInterface taskManagerInterface;

    /**
     * Constructor of TaskManager client.
     *
     * @param endpoint the endpoint of TaskManager server, for example "127.0.0.1:9902".
     */
    public TaskManagerClient(String endpoint) {


        RpcClientOptions clientOption = new RpcClientOptions();
        clientOption.setProtocolType(Options.ProtocolType.PROTOCOL_BAIDU_STD_VALUE);
        clientOption.setWriteTimeoutMillis(1000);
        clientOption.setReadTimeoutMillis(50000);
        clientOption.setMaxTotalConnections(1000);
        clientOption.setMinIdleConnections(10);
        clientOption.setLoadBalanceType(LoadBalanceStrategy.LOAD_BALANCE_FAIR);
        clientOption.setCompressType(Options.CompressType.COMPRESS_TYPE_NONE);

        String serviceUrl = "list://" + endpoint;
        List<Interceptor> interceptors = new ArrayList<Interceptor>();
        rpcClient = new RpcClient(serviceUrl, clientOption, interceptors);
        taskManagerInterface = BrpcProxy.getProxy(rpcClient, TaskManagerInterface.class);
        RpcContext.getContext().setLogId(1234L);
    }
    /**
     * Stop the brpc client.
     */
    public void stop() {
        rpcClient.stop();
    }
    /**
     * Stop the job.
     *
     * @param id of job.
     *
     * @throws Exception
     */
    public String stopJob(int id) throws Exception {
        TaskManager.StopJobRequest request = TaskManager.StopJobRequest.newBuilder()
                .setId(id)
                .build();
        TaskManager.StopJobResponse response = taskManagerInterface.StopJob(request);

        if (response.getCode() != 0) {
            String errorMessage = "Fail to request, code: " + response.getCode() + ", error: " + response.getMsg();
            throw new Exception(errorMessage);
        }
        return response.getMsg();
    }
    /**
     * RunBatchSql.
     *
     * @param sql,output_path.
     *
     * @throws Exception
     */
    public String runBatchSql(String sql,String output_path)throws Exception {
        TaskManager.RunBatchSqlRequest request = TaskManager.RunBatchSqlRequest.newBuilder()
                .setSql(sql)
                .setOutputPath(output_path)
                .build();
        TaskManager.ShowJobResponse response = taskManagerInterface.RunBatchSql(request);

        if (response.getCode() != 0) {
            String errorMessage = "Fail to request, code: " + response.getCode() + ", error: " + response.getMsg();
            throw new Exception(errorMessage);
        }
        return response.getMsg();
    }
    /**
     * RunBatchSql.
     *
     * @param sql,output_path,default_db(optional) .
     *
     * @throws Exception
     */
    public String runBatchSql(String sql,String output_path,String default_db)throws Exception {
        TaskManager.RunBatchSqlRequest request = TaskManager.RunBatchSqlRequest.newBuilder()
                .setSql(sql)
                .setOutputPath(output_path)
                .setDefaultDb(default_db)
                .build();
        TaskManager.ShowJobResponse response = taskManagerInterface.RunBatchSql(request);
        if (response.getCode() != 0) {
            String errorMessage = "Fail to request, code: " + response.getCode() + ", error: " + response.getMsg();
            throw new Exception(errorMessage);
        }
        return response.getMsg();
    }
    /**
     * RunBatchAndShow.
     *
     * @param sql
     *
     * @throws Exception
     */
    public String runBatchAndShow(String sql) throws Exception {
        TaskManager.RunBatchAndShowRequest request = TaskManager.RunBatchAndShowRequest.newBuilder()
                .setSql(sql)
                .build();
        TaskManager.ShowJobResponse response = taskManagerInterface.RunBatchAndShow(request);
        if (response.getCode() != 0) {
            String errorMessage = "Fail to request, code: " + response.getCode() + ", error: " + response.getMsg();
            throw new Exception(errorMessage);
        }
        return response.getMsg();
    }
    /**
     * RunBatchAndShow.
     *
     * @param sql,default_db(optional) .
     *
     * @throws Exception
     */
    public String runBatchAndShow(String sql,String default_db) throws Exception {
        TaskManager.RunBatchAndShowRequest request = TaskManager.RunBatchAndShowRequest.newBuilder()
                .setSql(sql)
                .setDefaultDb(default_db)
                .build();
        TaskManager.ShowJobResponse response = taskManagerInterface.RunBatchAndShow(request);
        if (response.getCode() != 0) {
            String errorMessage = "Fail to request, code: " + response.getCode() + ", error: " + response.getMsg();
            throw new Exception(errorMessage);
        }
        return response.getMsg();
    }
    /**
     * Show the job rely on id.
     *
     * @param id of job.
     * @return the JobInfo object.
     * @throws Exception
     */
    public TaskManager.JobInfo showOneJob(int id) throws Exception {
        TaskManager.ShowJobRequest request = TaskManager.ShowJobRequest.newBuilder()
                .setId(id)
                .build();
        TaskManager.ShowJobResponse response = taskManagerInterface.ShowJob(request);
        if (response.getCode() != 0) {
            String errorMessage = "Fail to request, code: " + response.getCode() + ", error: " + response.getMsg();
            throw new Exception(errorMessage);
        }
        return response.getJob();
    }
    /**
     * ImportOnlineData
     *
     * @param sql .
     *
     * @throws Exception
     */
    public String importOnlineData(String sql) throws Exception {
        TaskManager.ImportOnlineDataRequest request = TaskManager.ImportOnlineDataRequest.newBuilder()
                .setSql(sql)
                .build();
        TaskManager.ShowJobResponse response = taskManagerInterface.ImportOnlineData(request);
        if (response.getCode() != 0) {
            String errorMessage = "Fail to request, code: " + response.getCode() + ", error: " + response.getMsg();
            throw new Exception(errorMessage);
        }
        return response.getMsg();
    }
    /**
     * ImportOnlineData
     *
     * @param sql and default_db.
     *
     * @throws Exception
     */
    public String importOnlineData(String sql,String default_db) throws Exception {
        TaskManager.ImportOnlineDataRequest request = TaskManager.ImportOnlineDataRequest.newBuilder()
                .setSql(sql)
                .setDefaultDb(default_db)
                .build();
        TaskManager.ShowJobResponse response = taskManagerInterface.ImportOnlineData(request);
        if (response.getCode() != 0) {
            String errorMessage = "Fail to request, code: " + response.getCode() + ", error: " + response.getMsg();
            throw new Exception(errorMessage);
        }
        return response.getMsg();
    }
    /**
     * ImportOfflineData
     *
     * @param sql
     *
     * @throws Exception
     */
    public String importOfflineData(String sql) throws Exception {
        TaskManager.ImportOfflineDataRequest request = TaskManager.ImportOfflineDataRequest.newBuilder()
                .setSql(sql)
                .build();
        TaskManager.ShowJobResponse response = taskManagerInterface.ImportOfflineData(request);
        if (response.getCode() != 0) {
            String errorMessage = "Fail to request, code: " + response.getCode() + ", error: " + response.getMsg();
            throw new Exception(errorMessage);
        }
        return response.getMsg();
    }
    /**
     * ImportOfflineData
     *
     * @param sql and default_db
     *
     * @throws Exception
     */
    public String importOfflineData(String sql,String default_db) throws Exception {
        TaskManager.ImportOfflineDataRequest request = TaskManager.ImportOfflineDataRequest.newBuilder()
                .setSql(sql)
                .setDefaultDb(default_db)
                .build();
        TaskManager.ShowJobResponse response = taskManagerInterface.ImportOfflineData(request);
        if (response.getCode() != 0) {
            String errorMessage = "Fail to request, code: " + response.getCode() + ", error: " + response.getMsg();
            throw new Exception(errorMessage);
        }
        return response.getMsg();
    }
    /**
     * DropOfflineTable
     *
     * @throws Exception
     */
    public String dropOfflineTable(String db,String table) throws Exception {
        TaskManager.DropOfflineTableRequest request = TaskManager.DropOfflineTableRequest.newBuilder()
                .setDb(db)
                .setTable(table)
                .build();
        TaskManager.DropOfflineTableResponse response = taskManagerInterface.DropOfflineTable(request);
        if (response.getCode() != 0) {
            String errorMessage = "Fail to request, code: " + response.getCode() + ", error: " + response.getMsg();
            throw new Exception(errorMessage);
        }
        return response.getMsg();
    }
    /**
     * DropOfflineTable
     *
     * @param sql .
     *
     * @throws Exception
     */
    public String dropOfflineTable(String sql) throws Exception {
        TaskManager.ExportOfflineDataRequest request = TaskManager.ExportOfflineDataRequest.newBuilder()
                .setSql(sql)
                .build();
        TaskManager.ShowJobResponse response = taskManagerInterface.ExportOfflineData(request);
        if (response.getCode() != 0) {
            String errorMessage = "Fail to request, code: " + response.getCode() + ", error: " + response.getMsg();
            throw new Exception(errorMessage);
        }
        return response.getMsg();
    }
    /**
     * Show all the jobs.
     *
     * @return the list of the JobInfo protobuf objects.
     * @throws Exception
     */
    public List<TaskManager.JobInfo> showJobs() throws Exception {
        return showJobs(false);
    }
    /**
     * Show all the jobs or only unfinished jobs.
     *
     * @param onlyUnfinished only show unfinished jobs or not.
     * @return the list of the JobInfo protobuf objects.
     * @throws Exception
     */
    public List<TaskManager.JobInfo> showJobs(boolean onlyUnfinished) throws Exception {
        TaskManager.ShowJobsRequest request = TaskManager.ShowJobsRequest.newBuilder()
                .setUnfinished(onlyUnfinished)
                .build();
        TaskManager.ShowJobsResponse response = taskManagerInterface.ShowJobs(request);

        if (response.getCode() != 0) {
            String errorMessage = "Fail to request, code: " + response.getCode() + ", error: " + response.getMsg();
            throw new Exception(errorMessage);
        }
        return response.getJobsList();
    }
    /**
     * Print the jobs.
     *
     * @throws Exception
     */
    public void printJobs() throws Exception {
        List<TaskManager.JobInfo> jobInfos = this.showJobs();
        System.out.println("Job count: " + jobInfos.size());
        for (TaskManager.JobInfo jobInfo: jobInfos) {
            System.out.println(jobInfo);
        }
    }
    /**
     * Submit job to show batch version.
     *
     * @throws Exception
     */
    public void showBatchVersion() throws Exception {
        TaskManager.ShowBatchVersionRequest request = TaskManager.ShowBatchVersionRequest.newBuilder()
                .build();
        TaskManager.ShowJobResponse response = taskManagerInterface.ShowBatchVersion(request);
        if (response.getCode() != 0) {
            String errorMessage = "Fail to request, code: " + response.getCode() + ", error: " + response.getMsg();
            throw new Exception(errorMessage);
        }
    }

}
