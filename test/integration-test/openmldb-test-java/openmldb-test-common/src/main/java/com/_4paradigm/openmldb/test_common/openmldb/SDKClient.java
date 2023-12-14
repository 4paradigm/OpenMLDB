package com._4paradigm.openmldb.test_common.openmldb;

import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.test_common.bean.OpenMLDBJob;
import com._4paradigm.openmldb.test_common.bean.OpenMLDBResult;
import com._4paradigm.openmldb.test_common.chain.result.ResultParserManager;
import com._4paradigm.openmldb.test_common.util.*;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class SDKClient {
    public static final int POLLING_TIME = 10*1000;
    private Statement statement;

    private SDKClient(SqlExecutor executor){
        this.statement = executor.getStatement();
    }
    public static SDKClient of(SqlExecutor executor){
        return new SDKClient(executor);
    }
    public OpenMLDBResult execute(String sql) {
        log.info("execute sql:{}",sql.replaceAll("\\n", "\\r"));
        OpenMLDBResult openMLDBResult = new OpenMLDBResult();
        openMLDBResult.setSql(sql);
        try {
            boolean ok = statement.execute(sql);
            openMLDBResult.setHaveResult(ok);
            openMLDBResult.setMsg("success");
            openMLDBResult.setOk(true);
            SQLUtil.setExecuteMode(sql);
            if(ok){
                ResultUtil.parseResultSet(statement,openMLDBResult);
                ResultParserManager.of().parseResult(openMLDBResult);
                if(sql.toLowerCase().startsWith("load data")||sql.toLowerCase().contains("into outfile")){
                    OpenMLDBJob openMLDBJob = ResultUtil.parseJob(openMLDBResult);
                    openMLDBResult.setOpenMLDBJob(openMLDBJob);
                    if (!openMLDBJob.getState().equals("FINISHED")) {
                        openMLDBResult.setOk(false);
                        openMLDBResult.setMsg(openMLDBJob.getError());
                    }
                    // 异步方式需要使用下面的方式 轮询去过去job的状态
                    //OpenMLDBJob finishJobInfo = showJob(openMLDBJob.getId());
                    //openMLDBResult.setOpenMLDBJob(finishJobInfo);
                }
            }
            if(sql.toLowerCase().startsWith("create index")||sql.toLowerCase().startsWith("drop index")){
                Tool.sleep(10*1000);
            }
        } catch (SQLException e) {
            openMLDBResult.setOk(false);
            openMLDBResult.setMsg(e.getMessage());
            e.printStackTrace();
        }
        log.debug("openMLDBResult:{}",openMLDBResult);
        return openMLDBResult;
    }
    public OpenMLDBResult execute(List<String> sqlList) {
        OpenMLDBResult openMLDBResult = null;
        for(String sql:sqlList){
            openMLDBResult = execute(sql);
        }
        return openMLDBResult;
    }
    public void checkComponentStatus(String endpoint,String status){
        String sql = "show components;";
        boolean b = WaitUtil.waitCondition(()->{
            OpenMLDBResult openMLDBResult = execute(sql);
            List<List<Object>> rows = openMLDBResult.getResult();
            long count = rows.stream().filter(row -> row.get(0).equals(endpoint) && row.get(3).equals(status)).count();
            return count==1;
        });
        Assert.assertTrue(b,"check endpoint:"+endpoint+",status:"+status+"failed.");
    }
    public void checkComponentNotExist(String endpoint){
        String sql = "show components;";
        boolean b = WaitUtil.waitCondition(()->{
            OpenMLDBResult openMLDBResult = execute(sql);
            List<List<Object>> rows = openMLDBResult.getResult();
            long count = rows.stream().filter(row -> row.get(0).equals(endpoint)).count();
            return count==0;
        });
        Assert.assertTrue(b,"check endpoint not exist :"+endpoint +"failed.");
    }
    public void createDB(String dbName){
        String sql = String.format("create database %s",dbName);
        execute(sql);
    }
    public List<String> showTables(){
        String sql = String.format("show tables;");
        OpenMLDBResult openMLDBResult = execute(sql);
        List<String> tableNames = openMLDBResult.getResult().stream().map(l -> String.valueOf(l.get(0))).collect(Collectors.toList());
        return tableNames;
    }
    public boolean tableIsExist(String tableName){
        List<String> tableNames = showTables();
        return tableNames.contains(tableName);
    }
    public void setOnline(){
        execute("SET @@execute_mode='online';");
        OpenMLDBGlobalVar.EXECUTE_MODE="online";
    }
    public void setOffline(){
        execute("SET @@execute_mode='offline';");
        OpenMLDBGlobalVar.EXECUTE_MODE="offline";
    }
    public void useDB(String dbName){
        String sql = String.format("use %s",dbName);
        execute(sql);
    }
    public void createAndUseDB(String dbName){
        List<String> sqlList = new ArrayList<>();
        if(!OpenMLDBGlobalVar.CREATE_DB_NAMES.contains(dbName)){
            if (!SDKUtil.dbIsExist(statement,dbName)) {
                sqlList.add(String.format("create database if not exists %s;", dbName));
                OpenMLDBGlobalVar.CREATE_DB_NAMES.add(dbName);
            }
        }
        sqlList.add(String.format("use %s;", dbName));
        execute(sqlList);
    }
    public void insert(String tableName,List<Object> list){
        List<List<Object>> dataList = new ArrayList<>();
        dataList.add(list);
        insertList(tableName,dataList);
    }
    public void insertList(String tableName,List<List<Object>> dataList){
        String sql = SQLUtil.genInsertSQL(tableName,dataList);
        execute(sql);
    }
    public int getTableRowCount(String tableName){
        String sql = String.format("select * from %s",tableName);
        OpenMLDBResult openMLDBResult = execute(sql);
        return openMLDBResult.getCount();
    }
    public OpenMLDBJob getJobInfo(String id){
        // id int,job_type string,state string,start_time timestamp,end_time timestamp,parameter string,cluster string,application_id string,error string,
        // 3,ImportOnlineData,Submitted,2022-09-09 14:10:09.637,null(obj),/tmp/sql-3189165767685976831,local,,
        setOnline();
        OpenMLDBJob openMLDBJob = new OpenMLDBJob();
        String sql = String.format("SELECT * FROM __INTERNAL_DB.JOB_INFO where id = %s",id);
        OpenMLDBResult openMLDBResult = execute(sql);
        List<Object> row = openMLDBResult.getResult().get(0);
        openMLDBJob.setId(Integer.parseInt(String.valueOf(row.get(0))));
        openMLDBJob.setJobType(String.valueOf(row.get(1)));
        openMLDBJob.setState(String.valueOf(row.get(2)));
        openMLDBJob.setStartTime((Timestamp)row.get(3));
        openMLDBJob.setEndTime((Timestamp)row.get(4));
        openMLDBJob.setParameter(String.valueOf(row.get(5)));
        openMLDBJob.setCluster(String.valueOf(row.get(6)));
        openMLDBJob.setApplicationId(String.valueOf(row.get(7)));
        openMLDBJob.setError(String.valueOf(row.get(8)));
        return openMLDBJob;
    }
    public OpenMLDBJob showJob(int id){
        OpenMLDBJob openMLDBJob = new OpenMLDBJob();
        String sql = String.format("show job %s",id);
        OpenMLDBResult openMLDBResult = execute(sql);
        List<Object> row = openMLDBResult.getResult().get(0);
        openMLDBJob.setId(Integer.parseInt(String.valueOf(row.get(0))));
        openMLDBJob.setJobType(String.valueOf(row.get(1)));
        openMLDBJob.setState(String.valueOf(row.get(2)));
        openMLDBJob.setStartTime((Timestamp)row.get(3));
        openMLDBJob.setEndTime((Timestamp)row.get(4));
        openMLDBJob.setParameter(String.valueOf(row.get(5)));
        openMLDBJob.setCluster(String.valueOf(row.get(6)));
        openMLDBJob.setApplicationId(String.valueOf(row.get(7)));
        openMLDBJob.setError(String.valueOf(row.get(8)));
        return openMLDBJob;
    }
    public OpenMLDBJob getFinishJobInfo(String jobId){
        while (true){
            OpenMLDBJob openMLDBJob = getJobInfo(jobId);
            String state = openMLDBJob.getState();
            log.info("job state:{}",state);
            if("FINISHED".equals(state)){
                log.info("job to finish");
                Tool.sleep(POLLING_TIME);
                OpenMLDBJob finishJob = getJobInfo(jobId);
                String finishState = finishJob.getState();
                return finishJob;
            }
            if("FAILED".equals(state)||"LOST".equals(state)) {
                return openMLDBJob;
            }
            Tool.sleep(POLLING_TIME);
        }
    }

    public void close(){
        if(statement!=null){
            try {
                statement.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
