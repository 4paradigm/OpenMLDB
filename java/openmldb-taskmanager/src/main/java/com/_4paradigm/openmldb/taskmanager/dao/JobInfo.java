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

package com._4paradigm.openmldb.taskmanager.dao;

import com._4paradigm.openmldb.taskmanager.JobInfoManager;
import java.sql.Timestamp;
import java.util.Arrays;

/*
// TODO: Design the job type
enum JobType {
    SparkBatchSql, ImportOfflineData, ImportOnlineData;
}
*/

public class JobInfo {

    public static String[] FINAL_STATE = new String[] {"finished", "failed", "killed", "lost"};

    private int id;
    private String jobType;
    private String state;
    private Timestamp startTime;
    private Timestamp endTime;
    private String parameter;
    private String cluster;
    private String applicationId;
    private String error;

    public JobInfo(int id, String jobType, String state, Timestamp startTime, Timestamp endTime, String parameter,
                   String cluster, String applicationId, String error) {
        this.id = id;
        this.jobType = jobType;
        this.state = state;
        this.startTime = startTime;
        this.endTime = endTime;
        this.parameter = parameter;
        this.cluster = cluster;
        this.applicationId = applicationId;
        this.error = error;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getJobType() {
        return jobType;
    }

    public void setJobType(String jobType) {
        this.jobType = jobType;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public Timestamp getStartTime() {
        return startTime;
    }

    public void setStartTime(Timestamp startTime) {
        this.startTime = startTime;
    }

    public Timestamp getEndTime() {
        return endTime;
    }

    public void setEndTime(Timestamp endTime) {
        this.endTime = endTime;
    }

    public String getParameter() {
        return parameter;
    }

    public void setParameter(String parameter) {
        this.parameter = parameter;
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    @Override
    public String toString() {
        return String.format("id: %d, jobType: %s, state: %s, startTime: %s, endTime: %s parameter: %s, cluster: %s," +
                        " applicationId: %s, error: %s",
                id, jobType, state, startTime.toString(), endTime == null ? "": endTime.toString(), parameter, cluster,
                applicationId, error);
    }

    public boolean isFinished() {
        return Arrays.asList(FINAL_STATE).contains(state.toLowerCase());
    }

    public boolean isSuccess() {
        return state.toLowerCase().equals("finished");
    }

    public boolean isYarnJob() {
        return cluster.toLowerCase().startsWith("yarn");
    }

    public boolean isYarnClusterJob() {
        return cluster.equalsIgnoreCase("yarn") || cluster.equalsIgnoreCase("yarn-cluster");
    }

    public void sync() {
        JobInfoManager.syncJob(this);
    }

}
