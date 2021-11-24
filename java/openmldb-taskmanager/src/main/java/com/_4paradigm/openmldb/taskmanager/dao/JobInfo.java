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
    private String applicationId;
    private String error;

    public JobInfo() {

    }

    public JobInfo(int id, String jobType, String state, Timestamp startTime, Timestamp endTime, String parameter,
                   String applicationId, String error) {
        this.id = id;
        this.jobType = jobType;
        this.state = state;
        this.startTime = startTime;
        this.endTime = endTime;
        this.parameter = parameter;
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
        return String.format("id: %d, jobType: %s, state: %s, parameter: %s, applicationId: %s, error: %s",
                id, jobType, state, parameter, applicationId, error);
    }

    public boolean isFinished() {
        return Arrays.asList(FINAL_STATE).contains(state.toLowerCase());
    }

    public void sync() {
        JobInfoManager.syncJob(this);
    }

}
