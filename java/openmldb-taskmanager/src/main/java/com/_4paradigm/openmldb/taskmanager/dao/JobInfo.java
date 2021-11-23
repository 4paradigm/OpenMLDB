package com._4paradigm.openmldb.taskmanager.dao;

import lombok.Data;
import java.sql.Timestamp;
import java.util.Arrays;

/*
// TODO: Design the job type
enum JobType {
    SparkBatchSql, ImportOfflineData, ImportOnlineData;
}
*/

public class JobInfo {

    private long id;
    private String jobType;
    private String state;
    private Timestamp startTime;
    private Timestamp endTime;
    private String cluster;
    private String parameter;
    private String applicationId;
    private String error;

    public long getId() {
        return id;
    }

    public void setId(long id) {
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

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public String getParameter() {
        return parameter;
    }

    public void setParameter(String parameter) {
        this.parameter = parameter;
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
        return String.format("id: %d, jobType: %s, state: %s, cluster: %s, parameter: %s, applicationId: %s, error: %s",
                id, jobType, state, cluster, parameter, applicationId, error);
    }

    public boolean isFinal() {
        String[] finalState = new String[] {"finished", "failed", "killed", "lost"};
        return Arrays.asList(finalState).contains(state.toLowerCase());

    }

}
