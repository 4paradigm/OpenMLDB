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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/***
 * Java wrapper for TaskManager brpc client.
 */
public class TaskManagerClient {

    private RpcClient rpcClient;
    private RpcClientOptions clientOption;
    private CuratorFramework zkClient;
    private TaskManagerInterface taskManagerInterface;
    private static final Log logger = LogFactory.getLog(TaskManagerClient.class);

    public TaskManagerClient(String zkCluster, String zkPath) throws Exception {
        if (zkCluster == null || zkPath == null) {
            logger.info("Zookeeper address is wrong, please check the configuration");
        }
        String masterZnode = zkPath + "/taskmanager/leader";

        zkClient = CuratorFrameworkFactory.builder()
                .connectString(zkCluster)
                .sessionTimeoutMs(10000)
                .retryPolicy(new ExponentialBackoffRetry(1000, 10))
                .build();
        zkClient.start();
        Stat stat = zkClient.checkExists().forPath(masterZnode);
        if (stat != null) {  // The original master exists and is directly connected to it.
            String endpoint = new String(zkClient.getData().forPath(masterZnode));
            this.rpcConnect(endpoint);
            watch(zkClient, masterZnode, rpcClient, clientOption);
        } else {
            throw new Exception("TaskManager has not started yet, connection failed");
        }
    }

    /**
     * Create a data change listener event for this node.
     *
     * @param curator the zkClient.
     * @param path the path of zkNode.
     */
    public void watch(CuratorFramework curator, String path, RpcClient rpcClients, RpcClientOptions clientOption) throws Exception {
        final NodeCache nodeCache = new NodeCache(curator, path, false);
        class NodeListener implements NodeCacheListener {
            RpcClient rpcClients;
            RpcClientOptions clientOption;

            public NodeListener(RpcClient rpcClients, RpcClientOptions clientOption) {
                this.rpcClients = rpcClients;
                this.clientOption = clientOption;
            }

            @Override
            public void nodeChanged() throws Exception {
                String endpoint = new String(nodeCache.getCurrentData().getData());
                if (endpoint != null) {
                    rpcClient.stop();
                    logger.info("The content of the node was changed, try to reconnect");
                    rpcClient.stop();

                    String serviceUrl = "list://" + endpoint;
                    rpcClient = new RpcClient(serviceUrl, clientOption, new ArrayList<Interceptor>());
                    taskManagerInterface = BrpcProxy.getProxy(rpcClient, TaskManagerInterface.class);
                    RpcContext.getContext().setLogId(1234L);
                } else {
                    logger.info("The content of the node was deleted, please try to reconnect");
                    close();
                }
            }
        }
        nodeCache.getListenable().addListener(new NodeListener(rpcClients, clientOption));
        nodeCache.start(true);
    }

    /**
     * Constructor of TaskManager client.
     *
     * @param endpoint the endpoint of TaskManager server, for example "127.0.0.1:9902".
     */
    public void rpcConnect(String endpoint) {

        clientOption = new RpcClientOptions();
        clientOption.setProtocolType(Options.ProtocolType.PROTOCOL_BAIDU_STD_VALUE);
        clientOption.setWriteTimeoutMillis(1000);
        clientOption.setReadTimeoutMillis(50000);
        clientOption.setMaxTotalConnections(1000);
        clientOption.setMinIdleConnections(10);
        clientOption.setLoadBalanceType(LoadBalanceStrategy.LOAD_BALANCE_FAIR);
        clientOption.setCompressType(Options.CompressType.COMPRESS_TYPE_NONE);

        String serviceUrl = "list://" + endpoint;
        List<Interceptor> interceptors = new ArrayList<Interceptor>();
        if (rpcClient != null) {
            this.stop();
        }
        rpcClient = new RpcClient(serviceUrl, clientOption, interceptors);
        taskManagerInterface = BrpcProxy.getProxy(rpcClient, TaskManagerInterface.class);
        RpcContext.getContext().setLogId(1234L);
    }

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
     * Stop the client.
     */
    public void stop() {
        rpcClient.stop();
    }

    /**
     * Close the client.
     */
    public void close() {
        stop();
    }

    /**
     * Stop the job.
     *
     * @param id id of job.
     * @throws Exception
     */
    public void stopJob(int id) throws Exception {
        TaskManager.StopJobRequest request = TaskManager.StopJobRequest.newBuilder()
                .setId(id)
                .build();
        TaskManager.StopJobResponse response = taskManagerInterface.StopJob(request);

        if (response.getCode() != 0) {
            String errorMessage = "Fail to request, code: " + response.getCode() + ", error: " + response.getMsg();
            throw new Exception(errorMessage);
        }
    }
    /**
     * Run sql statements in batches without default_db.
     *
     * @param sql query sql string.
     * @param outputPath output path.
     * @return the output string.
     * @throws Exception
     */
    public String runBatchSql(String sql, String outputPath) throws Exception {
        return runBatchSql(sql, outputPath, new HashMap<String, String>(), "");
    }
    /**
     * Run sql statements in batches.
     *
     * @param sql query sql string.
     * @param outputPath output path.
     * @param default_db default database.
     * @return the output string.
     * @throws Exception
     */
    public String runBatchSql(String sql, String outputPath, HashMap<String, String> conf, String default_db) throws Exception {
        TaskManager.RunBatchSqlRequest request = TaskManager.RunBatchSqlRequest.newBuilder()
                .setSql(sql)
                .putAllConf(conf)
                .setDefaultDb(default_db)
                .build();
        TaskManager.RunBatchSqlResponse response = taskManagerInterface.RunBatchSql(request);
        if (response.getCode() != 0) {
            String errorMessage = "Fail to request, code: " + response.getCode() + ", error: " + response.getMsg();
            throw new Exception(errorMessage);
        }
        return response.getOutput();
    }
    /**
     * Run batch sql statements and display the results without default_db.
     *
     * @param sql query sql string.
     * @return id of job.
     * @throws Exception
     */
    public int runBatchAndShow(String sql) throws Exception {
        return runBatchAndShow(sql, new HashMap<String, String>(), "");
    }
    /**
     * Run batch sql statements and display the results.
     *
     * @param sql query sql string.
     * @param conf configure.
     * @param default_db default database.
     * @return id of job.
     * @throws Exception
     */
    public int runBatchAndShow(String sql, HashMap<String, String> conf, String default_db) throws Exception {
        TaskManager.RunBatchAndShowRequest request = TaskManager.RunBatchAndShowRequest.newBuilder()
                .setSql(sql)
                .putAllConf(conf)
                .setDefaultDb(default_db)
                .build();
        TaskManager.ShowJobResponse response = taskManagerInterface.RunBatchAndShow(request);
        if (response.getCode() != 0) {
            String errorMessage = "Fail to request, code: " + response.getCode() + ", error: " + response.getMsg();
            throw new Exception(errorMessage);
        }
        return response.getJob().getId();
    }
    /**
     * Show the job rely on id.
     *
     * @param id id of job.
     * @return the JobInfo object.
     * @throws Exception
     */
    public TaskManager.JobInfo showJob(int id) throws Exception {
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
     * Import online data without default_db.
     *
     * @param sql query sql string.
     * @return id of job.
     * @throws Exception
     */
    public int importOnlineData(String sql) throws Exception {
        return importOnlineData(sql, new HashMap<String, String>(), "");
    }
    /**
     * Import online data.
     *
     * @param sql query sql string.
     * @param conf configure.
     * @param default_db default database.
     * @return id of job.
     * @throws Exception
     */
    public int importOnlineData(String sql, HashMap<String, String> conf, String default_db) throws Exception {
        TaskManager.ImportOnlineDataRequest request = TaskManager.ImportOnlineDataRequest.newBuilder()
                .setSql(sql)
                .putAllConf(conf)
                .setDefaultDb(default_db)
                .build();
        TaskManager.ShowJobResponse response = taskManagerInterface.ImportOnlineData(request);
        if (response.getCode() != 0) {
            String errorMessage = "Fail to request, code: " + response.getCode() + ", error: " + response.getMsg();
            throw new Exception(errorMessage);
        }
        return response.getJob().getId();
    }
    /**
     * Import offline data without default_db.
     *
     * @param sql query sql string.
     * @return id of job.
     * @throws Exception
     */
    public int importOfflineData(String sql) throws Exception {
        return importOfflineData(sql, new HashMap<String, String>(), "");
    }
    /**
     * Import offline data.
     *
     * @param sql query sql string.
     * @param conf configure.
     * @param default_db default database.
     * @return id of job.
     * @throws Exception
     */
    public int importOfflineData(String sql, HashMap<String, String> conf, String default_db) throws Exception {
        TaskManager.ImportOfflineDataRequest request = TaskManager.ImportOfflineDataRequest.newBuilder()
                .setSql(sql)
                .putAllConf(conf)
                .setDefaultDb(default_db)
                .build();
        TaskManager.ShowJobResponse response = taskManagerInterface.ImportOfflineData(request);
        if (response.getCode() != 0) {
            String errorMessage = "Fail to request, code: " + response.getCode() + ", error: " + response.getMsg();
            throw new Exception(errorMessage);
        }
        return response.getJob().getId();
    }
    /**
     * Delete offline table.
     *
     * @param db database name.
     * @param table table name.
     * @throws Exception
     */
    public void dropOfflineTable(String db, String table) throws Exception {
        TaskManager.DropOfflineTableRequest request = TaskManager.DropOfflineTableRequest.newBuilder()
                .setDb(db)
                .setTable(table)
                .build();
        TaskManager.DropOfflineTableResponse response = taskManagerInterface.DropOfflineTable(request);
        if (response.getCode() != 0) {
            String errorMessage = "Fail to request, code: " + response.getCode() + ", error: " + response.getMsg();
            throw new Exception(errorMessage);
        }
    }
    /**
     * Export offline data without default_db.
     *
     * @param sql query sql string.
     * @return id of job.
     * @throws Exception
     */
    public int exportOfflineData(String sql) throws Exception {
        return exportOfflineData(sql, new HashMap<String, String>(), "");
    }
    /**
     * Export offline data.
     *
     * @param sql query sql string.
     * @param conf configure.
     * @param default_db default database.
     * @return id of job.
     * @throws Exception
     */
    public int exportOfflineData(String sql, HashMap<String, String> conf, String default_db) throws Exception {
        TaskManager.ExportOfflineDataRequest request = TaskManager.ExportOfflineDataRequest.newBuilder()
                .setSql(sql)
                .putAllConf(conf)
                .setDefaultDb(default_db)
                .build();
        TaskManager.ShowJobResponse response = taskManagerInterface.ExportOfflineData(request);
        if (response.getCode() != 0) {
            String errorMessage = "Fail to request, code: " + response.getCode() + ", error: " + response.getMsg();
            throw new Exception(errorMessage);
        }
        return response.getJob().getId();
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
     * @return id of job.
     * @throws Exception
     */
    public int showBatchVersion() throws Exception {
        TaskManager.ShowBatchVersionRequest request = TaskManager.ShowBatchVersionRequest.newBuilder()
                .build();
        TaskManager.ShowJobResponse response = taskManagerInterface.ShowBatchVersion(request);
        if (response.getCode() != 0) {
            String errorMessage = "Fail to request, code: " + response.getCode() + ", error: " + response.getMsg();
            throw new Exception(errorMessage);
        }
        return response.getJob().getId();
    }

    /**
     * Submit job to show batch version.
     *
     * @throws Exception
     */
    public String getJobLog(int id) throws Exception {
        TaskManager.GetJobLogRequest request = TaskManager.GetJobLogRequest.newBuilder()
                .setId(id)
                .build();
        TaskManager.GetJobLogResponse response = taskManagerInterface.GetJobLog(request);
        if (response.getCode() != 0) {
            String errorMessage = "Fail to request, code: " + response.getCode() + ", error: " + response.getMsg();
            throw new Exception(errorMessage);
        }
        return response.getLog();
    }

}
