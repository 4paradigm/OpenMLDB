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
     * Show all the jobs.
     *
     * @return the list of the JobInfo protobuf objects.
     * @throws Exception
     */
    public List<com._4paradigm.openmldb.proto.TaskManager.JobInfo> showJobs() throws Exception {
        return showJobs(false);
    }

    /**
     * Show all the jobs or only unfinished jobs.
     *
     * @param onlyUnfinished only show unfinished jobs or not.
     * @return the list of the JobInfo protobuf objects.
     * @throws Exception
     */
    public List<com._4paradigm.openmldb.proto.TaskManager.JobInfo> showJobs(boolean onlyUnfinished) throws Exception {
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
        List<com._4paradigm.openmldb.proto.TaskManager.JobInfo> jobInfos = this.showJobs();
        System.out.println("Job count: " + jobInfos.size());
        for (com._4paradigm.openmldb.proto.TaskManager.JobInfo jobInfo: jobInfos) {
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
