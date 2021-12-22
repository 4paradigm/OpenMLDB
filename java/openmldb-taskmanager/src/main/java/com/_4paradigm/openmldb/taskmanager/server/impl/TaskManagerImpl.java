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
import com._4paradigm.openmldb.taskmanager.JobInfoManager;
import com._4paradigm.openmldb.taskmanager.OpenmldbBatchjobManager;
import com._4paradigm.openmldb.taskmanager.dao.JobInfo;
import com._4paradigm.openmldb.taskmanager.server.StatusCode;
import com._4paradigm.openmldb.taskmanager.server.TaskManagerInterface;
import lombok.extern.slf4j.Slf4j;
import scala.Option;

import java.util.List;

@Slf4j
public class TaskManagerImpl implements TaskManagerInterface {

    public TaskManager.JobInfo jobInfoToProto(JobInfo job) {
        TaskManager.JobInfo.Builder builder =  TaskManager.JobInfo.newBuilder();
        builder.setId(job.getId()).setJobType(job.getJobType()).setState(job.getState()).setStartTime(job.getStartTime().getTime());
        if (job.getEndTime() != null) {
            builder.setEndTime(job.getEndTime().getTime());
        }
        builder.setParameter(job.getParameter()).setCluster(job.getCluster());
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
            for (int i=0; i < jobInfos.size(); ++i) {
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
            Option<JobInfo> jobInfo = JobInfoManager.getJob(request.getId());

            TaskManager.ShowJobResponse.Builder responseBuilder = TaskManager.ShowJobResponse.newBuilder()
                    .setCode(StatusCode.SUCCESS);
            if (jobInfo.nonEmpty()) {
                responseBuilder.setJob(jobInfoToProto(jobInfo.get()));
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
            JobInfo jobInfo = OpenmldbBatchjobManager.showBatchVersion();
            return TaskManager.ShowJobResponse.newBuilder().setCode(StatusCode.SUCCESS).setJob(jobInfoToProto(jobInfo))
                    .build();
        } catch (Exception e) {
            e.printStackTrace();
            return TaskManager.ShowJobResponse.newBuilder().setCode(StatusCode.FAILED).setMsg(e.getMessage()).build();
        }
    }

    @Override
    public TaskManager.ShowJobResponse RunBatchSql(TaskManager.RunBatchSqlRequest request) {
        try {
            JobInfo jobInfo = OpenmldbBatchjobManager.runBatchSql(request.getSql(), request.getOutputPath(), request.getConfMap(), request.getDefaultDb());
            return TaskManager.ShowJobResponse.newBuilder().setCode(StatusCode.SUCCESS).setJob(jobInfoToProto(jobInfo))
                    .build();
        } catch (Exception e) {
            e.printStackTrace();
            return TaskManager.ShowJobResponse.newBuilder().setCode(StatusCode.FAILED).setMsg(e.getMessage()).build();
        }
    }

    @Override
    public TaskManager.ShowJobResponse RunBatchAndShow(TaskManager.RunBatchAndShowRequest request) {
        try {
            JobInfo jobInfo = OpenmldbBatchjobManager.runBatchAndShow(request.getSql(), request.getConfMap(), request.getDefaultDb());
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
            JobInfo jobInfo = OpenmldbBatchjobManager.importOnlineData(request.getSql(), request.getConfMap(), request.getDefaultDb());
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
            JobInfo jobInfo = OpenmldbBatchjobManager.importOfflineData(request.getSql(), request.getConfMap(), request.getDefaultDb());
            return TaskManager.ShowJobResponse.newBuilder().setCode(StatusCode.SUCCESS).setJob(jobInfoToProto(jobInfo))
                    .build();
        } catch (Exception e) {
            e.printStackTrace();
            return TaskManager.ShowJobResponse.newBuilder().setCode(StatusCode.FAILED).setMsg(e.getMessage()).build();
        }
    }
}
