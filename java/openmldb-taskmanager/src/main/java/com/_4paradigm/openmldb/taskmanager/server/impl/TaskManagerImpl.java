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

import com._4paradigm.openmldb.common.zk.ZKClient;
import com._4paradigm.openmldb.common.zk.ZKConfig;
import com._4paradigm.openmldb.proto.TaskManager;
import com._4paradigm.openmldb.proto.Common;
import com._4paradigm.openmldb.taskmanager.JobInfoManager;
import com._4paradigm.openmldb.taskmanager.LogManager;
import com._4paradigm.openmldb.taskmanager.OpenmldbBatchjobManager;
import com._4paradigm.openmldb.taskmanager.config.ConfigException;
import com._4paradigm.openmldb.taskmanager.config.TaskManagerConfig;
import com._4paradigm.openmldb.taskmanager.dao.JobInfo;
import com._4paradigm.openmldb.taskmanager.server.JobResultSaver;
import com._4paradigm.openmldb.taskmanager.server.StatusCode;
import com._4paradigm.openmldb.taskmanager.server.TaskManagerInterface;
import com._4paradigm.openmldb.taskmanager.udf.ExternalFunctionManager;
import com._4paradigm.openmldb.taskmanager.util.VersionUtil;
import com._4paradigm.openmldb.taskmanager.utils.VersionCli;
import com._4paradigm.openmldb.taskmanager.yarn.YarnClientUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import scala.Option;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The implementation of protobuf APIs.
 */
@Slf4j
public class TaskManagerImpl implements TaskManagerInterface {
    private static final Log logger = LogFactory.getLog(TaskManagerImpl.class);

    private volatile JobResultSaver jobResultSaver;

    /**
     * Constructor of TaskManagerImpl.
     *
     * @throws InterruptedException
     * @throws ConfigException
     */
    public TaskManagerImpl() throws InterruptedException, ConfigException {
        jobResultSaver = new JobResultSaver();

        TaskManagerConfig.parse();

        initExternalFunction();
    }

    /**
     * Read ZooKeeper path and load UDF libraries.
     *
     * @throws InterruptedException
     */
    private void initExternalFunction() throws InterruptedException {
        ZKClient zkClient = new ZKClient(ZKConfig.builder()
                .cluster(TaskManagerConfig.getZkCluster())
                .namespace(TaskManagerConfig.getZkRootPath())
                .sessionTimeout(TaskManagerConfig.getZkSessionTimeout())
                .baseSleepTime(TaskManagerConfig.getZkBaseSleepTime())
                .connectionTimeout(TaskManagerConfig.getZkConnectionTimeout())
                .maxConnectWaitTime(TaskManagerConfig.getZkMaxConnectWaitTime())
                .maxRetries(TaskManagerConfig.getZkMaxRetries())
                .build());
        zkClient.connect();

        String funPath = TaskManagerConfig.getZkRootPath() + "/data/function";
        try {
            List<String> funNames = zkClient.getChildren(funPath);
            for (String name : funNames) {
                try {
                    String value = zkClient.getNodeValue(funPath + "/" + name);
                    Common.ExternalFun fun = Common.ExternalFun.parseFrom(value.getBytes());

                    String libraryFileName = fun.getFile().substring(fun.getFile().lastIndexOf("/") + 1);
                    ExternalFunctionManager.addFunction(fun.getName(), libraryFileName);
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.error("Fail to parse protobuf of function: " + name);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Fail to init external function from ZooKeeper");
        }
    }

    /**
     * Covert JobInfo object to protobuf object.
     *
     * @param job the object of class JobInfo
     * @return the protobuf object
     */
    public TaskManager.JobInfo jobInfoToProto(JobInfo job) {
        TaskManager.JobInfo.Builder builder = TaskManager.JobInfo.newBuilder();
        builder.setId(job.getId());
        if (job.getJobType() != null) {
            builder.setJobType(job.getJobType());
        }
        if (job.getState() != null) {
            builder.setState(job.getState());
        }
        if (job.getStartTime() != null) {
            builder.setStartTime(job.getStartTime().getTime());
        }
        if (job.getEndTime() != null) {
            builder.setEndTime(job.getEndTime().getTime());
        }
        if (job.getParameter() != null) {
            builder.setParameter(job.getParameter());
        }
        if (job.getCluster() != null) {
            builder.setCluster(job.getCluster());
        }
        if (job.getApplicationId() != null) {
            builder.setApplicationId(job.getApplicationId());
        }
        if (job.getError() != null) {
            builder.setError(job.getError());
        }
        return builder.build();
    }

    @Override
    public TaskManager.ShowJobsResponse ShowJobs(TaskManager.ShowJobsRequest request) {
        try {
            List<JobInfo> jobInfos = JobInfoManager.getJobs(request.getUnfinished());

            TaskManager.ShowJobsResponse.Builder builder = TaskManager.ShowJobsResponse.newBuilder();
            builder.setCode(StatusCode.SUCCESS);
            for (int i = 0; i < jobInfos.size(); ++i) {
                builder.addJobs(i, jobInfoToProto(jobInfos.get(i)));
            }
            return builder.build();
        } catch (Exception e) {
            e.printStackTrace();
            return TaskManager.ShowJobsResponse.newBuilder().setCode(StatusCode.FAILED).setMsg(e.getMessage()).build();
        }
    }

    @Override
    public TaskManager.ShowJobResponse ShowJob(TaskManager.ShowJobRequest request) {
        try {
            TaskManager.ShowJobResponse.Builder responseBuilder = TaskManager.ShowJobResponse.newBuilder();

            Option<JobInfo> jobInfo = JobInfoManager.getJob(request.getId());

            if (jobInfo.isEmpty()) {
                responseBuilder.setCode(StatusCode.FAILED).setMsg("Fail to get job with id: " + request.getId());
            } else {
                responseBuilder.setCode(StatusCode.SUCCESS).setJob(jobInfoToProto(jobInfo.get()));
            }

            return responseBuilder.build();
        } catch (Exception e) {
            e.printStackTrace();
            return TaskManager.ShowJobResponse.newBuilder().setCode(StatusCode.FAILED).setMsg(e.getMessage()).build();
        }
    }

    @Override
    public TaskManager.StopJobResponse StopJob(TaskManager.StopJobRequest request) {
        try {
            JobInfo jobInfo = JobInfoManager.stopJob(request.getId());
            return TaskManager.StopJobResponse.newBuilder().setCode(StatusCode.SUCCESS).setJob(jobInfoToProto(jobInfo))
                    .build();
        } catch (Exception e) {
            e.printStackTrace();
            return TaskManager.StopJobResponse.newBuilder().setCode(StatusCode.FAILED).setMsg(e.getMessage()).build();
        }
    }

    @Override
    public TaskManager.DeleteJobResponse DeleteJob(TaskManager.DeleteJobRequest request) {
        try {
            JobInfoManager.deleteJob(request.getId());
            return TaskManager.DeleteJobResponse.newBuilder().setCode(StatusCode.SUCCESS).build();
        } catch (Exception e) {
            e.printStackTrace();
            return TaskManager.DeleteJobResponse.newBuilder().setCode(StatusCode.FAILED).setMsg(e.getMessage()).build();
        }
    }

    @Override
    public TaskManager.ShowJobResponse ShowBatchVersion(TaskManager.ShowBatchVersionRequest request) {
        try {
            JobInfo jobInfo = OpenmldbBatchjobManager.showBatchVersion(request.getSyncJob());
            return TaskManager.ShowJobResponse.newBuilder().setCode(StatusCode.SUCCESS).setJob(jobInfoToProto(jobInfo))
                    .build();
        } catch (Exception e) {
            e.printStackTrace();
            return TaskManager.ShowJobResponse.newBuilder().setCode(StatusCode.FAILED).setMsg(e.getMessage()).build();
        }
    }

    @Override
    public TaskManager.RunBatchSqlResponse RunBatchSql(TaskManager.RunBatchSqlRequest request) {
        try {
            Map<String, String> confMap = new HashMap<>(request.getConfMap());
            // add conf about SaveJobResult
            // HOST can't be 0.0.0.0 if distributed or spark is not local
            confMap.put("spark.openmldb.savejobresult.http",
                    String.format("http://%s:%d/openmldb.taskmanager.TaskManagerServer/SaveJobResult",
                            TaskManagerConfig.getServerHost(), TaskManagerConfig.getServerPort()));
            // we can't get spark job id here, so we use JobResultSaver id, != spark job id
            // if too much running jobs to save result, throw exception
            int resultId = jobResultSaver.genResultId();
            confMap.put("spark.openmldb.savejobresult.resultid", String.valueOf(resultId));
            JobInfo jobInfo = OpenmldbBatchjobManager.runBatchSql(request.getSql(), confMap,
                    request.getDefaultDb());

            // Check job state and return failed status code if the Spark job failed
            int jobId = jobInfo.getId();
            JobInfo finalJobInfo = JobInfoManager.getJob(jobId).get();
            if (finalJobInfo.isSuccess()) {
                // wait for all files of result saved and read them, large timeout
                // TODO: Test for K8S backend
                String output = jobResultSaver.readResult(resultId, TaskManagerConfig.getBatchJobResultMaxWaitTime());
                return TaskManager.RunBatchSqlResponse.newBuilder().setCode(StatusCode.SUCCESS).setOutput(output)
                        .build();
            } else {
                String errorMsg = String.format("The job %d fail and use 'SHOW JOBLOG %d' for more info", jobId, jobId);
                return TaskManager.RunBatchSqlResponse.newBuilder().setCode(StatusCode.FAILED).setMsg(errorMsg)
                        .build();
            }
        } catch (Exception e) {
            e.printStackTrace();
            return TaskManager.RunBatchSqlResponse.newBuilder().setCode(StatusCode.FAILED).setMsg(e.getMessage())
                    .build();
        }
    }

    // waitSeconds: 0 means wait max time
    // rpc max time is CHANNEL_KEEP_ALIVE_TIME, so we don't need to wait too long
    private JobInfo busyWaitJobInfo(int jobId, int waitSeconds) throws InterruptedException {
        long maxWaitEnd = System.currentTimeMillis()
                + (waitSeconds == 0 ? TaskManagerConfig.getChannelKeepAliveTime() : waitSeconds) * 1000;
        while (System.currentTimeMillis() < maxWaitEnd) {
            Option<JobInfo> info = JobInfoManager.getJob(jobId);
            if (info.isEmpty()) {
                throw new RuntimeException("job " + jobId + " not found in job_info table");
            }
            if (info.get().isFinished()) {
                return info.get();
            }
            Thread.sleep(10000);
        }
        throw new RuntimeException("wait for job " + jobId + " timeout");
    }

    private JobInfo waitJobInfoWrapper(int jobId) throws Exception {
        try {
            busyWaitJobInfo(jobId, 0);
            // Ref https://github.com/4paradigm/OpenMLDB/issues/1436#issuecomment-1066314684
            // wait for 2s to avoid state jump from FINISHED to FAILED
            Thread.sleep(2000);
            // check the job state again, just check one time
            return busyWaitJobInfo(jobId, 2);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("wait for job failed, use show job to get the job status. Job " + jobId, e);
        }
    }

    @Override
    public TaskManager.ShowJobResponse RunBatchAndShow(TaskManager.RunBatchAndShowRequest request) {
        try {
            JobInfo jobInfo = OpenmldbBatchjobManager.runBatchAndShow(request.getSql(), request.getConfMap(),
                    request.getDefaultDb());
            if (request.getSyncJob()) {
                // wait for final state
                jobInfo = waitJobInfoWrapper(jobInfo.getId());
            }
            return TaskManager.ShowJobResponse.newBuilder().setCode(StatusCode.SUCCESS).setJob(jobInfoToProto(jobInfo))
                    .build();
        } catch (Exception e) {
            e.printStackTrace();
            return TaskManager.ShowJobResponse.newBuilder().setCode(StatusCode.FAILED).setMsg(e.getMessage()).build();
        }
    }

    @Override
    public TaskManager.ShowJobResponse ImportOnlineData(TaskManager.ImportOnlineDataRequest request) {
        try {
            JobInfo jobInfo = OpenmldbBatchjobManager.importOnlineData(request.getSql(), request.getConfMap(),
                    request.getDefaultDb());
            if (request.getSyncJob()) {
                // wait for final state
                jobInfo = waitJobInfoWrapper(jobInfo.getId());
            }
            return TaskManager.ShowJobResponse.newBuilder().setCode(StatusCode.SUCCESS).setJob(jobInfoToProto(jobInfo))
                    .build();
        } catch (Exception e) {
            e.printStackTrace();
            return TaskManager.ShowJobResponse.newBuilder().setCode(StatusCode.FAILED).setMsg(e.getMessage()).build();
        }
    }

    @Override
    public TaskManager.ShowJobResponse ImportOfflineData(TaskManager.ImportOfflineDataRequest request) {
        try {
            JobInfo jobInfo = OpenmldbBatchjobManager.importOfflineData(request.getSql(), request.getConfMap(),
                    request.getDefaultDb());
            if (request.getSyncJob()) {
                // wait for final state
                jobInfo = waitJobInfoWrapper(jobInfo.getId());
            }
            return TaskManager.ShowJobResponse.newBuilder().setCode(StatusCode.SUCCESS).setJob(jobInfoToProto(jobInfo))
                    .build();
        } catch (Exception e) {
            e.printStackTrace();
            return TaskManager.ShowJobResponse.newBuilder().setCode(StatusCode.FAILED).setMsg(e.getMessage()).build();
        }
    }

    @Override
    public TaskManager.ShowJobResponse ExportOfflineData(TaskManager.ExportOfflineDataRequest request) {
        try {
            JobInfo jobInfo = OpenmldbBatchjobManager.exportOfflineData(request.getSql(), request.getConfMap(),
                    request.getDefaultDb());
            if (request.getSyncJob()) {
                // wait for final state
                jobInfo = waitJobInfoWrapper(jobInfo.getId());
            }
            return TaskManager.ShowJobResponse.newBuilder().setCode(StatusCode.SUCCESS).setJob(jobInfoToProto(jobInfo))
                    .build();
        } catch (Exception e) {
            e.printStackTrace();
            return TaskManager.ShowJobResponse.newBuilder().setCode(StatusCode.FAILED).setMsg(e.getMessage()).build();
        }
    }

    @Override
    public TaskManager.DropOfflineTableResponse DropOfflineTable(TaskManager.DropOfflineTableRequest request) {
        try {
            JobInfoManager.dropOfflineTable(request.getDb(), request.getTable());
            return TaskManager.DropOfflineTableResponse.newBuilder().setCode(StatusCode.SUCCESS).build();
        } catch (Exception e) {
            e.printStackTrace();
            return TaskManager.DropOfflineTableResponse.newBuilder().setCode(StatusCode.FAILED).setMsg(e.getMessage())
                    .build();
        }
    }

    @Override
    public TaskManager.GetJobLogResponse GetJobLog(TaskManager.GetJobLogRequest request) {
        try {
            String outLog = "";
            try {
                outLog = LogManager.getJobLog(request.getId());
            } catch (Exception e) {
                logger.warn(String.format("Fail to to get job log of job %s", request.getId()));
            }

            String errorLog = "";
            try {
                errorLog = LogManager.getJobErrorLog(request.getId());
            } catch (Exception e) {
                logger.warn(String.format("Fail to to get job error log of job %s", request.getId()));
            }

            String log = String.format("Stdout:\n%s\n\nStderr:\n%s", outLog, errorLog);

            /* TODO: Can not get yarn log from finished containers
            try {
                JobInfo jobInfo = JobInfoManager.getJob(request.getId()).get();
                if (TaskManagerConfig.isYarnCluster() && jobInfo.isFinished()) {
                    // TODO: The yarn log is printed in front of the string
                    String completeYarnLog = YarnClientUtil.getAppLog(jobInfo.getApplicationId());
                    log += "\n\nYarn log: " + completeYarnLog;
                }
            } catch (Exception e) {
                logger.error("Fail to get yarn log for job " + request.getId());
                e.printStackTrace();
            }
            */

            return TaskManager.GetJobLogResponse.newBuilder().setCode(StatusCode.SUCCESS).setLog(log).build();
        } catch (Exception e) {
            e.printStackTrace();
            return TaskManager.GetJobLogResponse.newBuilder().setCode(StatusCode.FAILED).setMsg(e.getMessage()).build();
        }
    }

    @Override
    public TaskManager.GetVersionResponse GetVersion(TaskManager.EmptyMessage request) {
        String taskmanagerVersion = "unknown";
        String batchVersion = "unknown";

        try {
            taskmanagerVersion = VersionCli.getVersion();
        } catch (Exception e) {
            logger.warn("Fail to get TaskManager version, message: " + e.getMessage());
        }

        try {
            batchVersion = VersionUtil.getBatchVersion();
        } catch (Exception e) {
            logger.warn("Fail to get batch engine version, message: " + e.getMessage());
        }

        return TaskManager.GetVersionResponse.newBuilder().setTaskmanagerVersion(taskmanagerVersion)
                .setBatchVersion(batchVersion).build();
    }

    @Override
    public TaskManager.CreateFunctionResponse CreateFunction(TaskManager.CreateFunctionRequest request) {
        Common.ExternalFun fun = request.getFun();
        if (fun.getFile().isEmpty()) {
            return TaskManager.CreateFunctionResponse.newBuilder()
                    .setCode(StatusCode.FAILED)
                    .setMsg("ExternalFun does not have the file path")
                    .build();
        }
        String libraryFileName = fun.getFile().substring(fun.getFile().lastIndexOf("/") + 1);
        try {
            ExternalFunctionManager.addFunction(fun.getName(), libraryFileName);
        } catch (Exception e) {
            return TaskManager.CreateFunctionResponse.newBuilder().setCode(StatusCode.FAILED).setMsg(e.getMessage())
                    .build();
        }

        return TaskManager.CreateFunctionResponse.newBuilder().setCode(StatusCode.SUCCESS).setMsg("ok").build();
    }

    @Override
    public TaskManager.DropFunctionResponse DropFunction(TaskManager.DropFunctionRequest request) {
        ExternalFunctionManager.dropFunction(request.getName());
        return TaskManager.DropFunctionResponse.newBuilder().setCode(StatusCode.SUCCESS).setMsg("ok").build();
    }

    @Override
    public TaskManager.SaveJobResultResponse SaveJobResult(TaskManager.SaveJobResultRequest request) {
        if (request.getResultId() == -1 && request.getJsonData().equals("reset")) {
            try {
                jobResultSaver.reset();
                return TaskManager.SaveJobResultResponse.newBuilder().setCode(StatusCode.SUCCESS)
                        .setMsg("reset job result saver ok").build();
            } catch (IOException e) {
                e.printStackTrace();
                return TaskManager.SaveJobResultResponse.newBuilder().setCode(StatusCode.FAILED)
                        .setMsg("reset job result saver failed, " + e.getMessage()).build();
            }
        }
        // log if save failed
        if (!jobResultSaver.saveFile(request.getResultId(), request.getJsonData())) {
            log.error("save job result failed(write to local file) for resultId: {}", request.getResultId());
            return TaskManager.SaveJobResultResponse.newBuilder().setCode(StatusCode.FAILED)
                    .setMsg("save job result failed(write to local file)").build();
        }
        return TaskManager.SaveJobResultResponse.newBuilder().setCode(StatusCode.SUCCESS).setMsg("ok").build();
    }

}
