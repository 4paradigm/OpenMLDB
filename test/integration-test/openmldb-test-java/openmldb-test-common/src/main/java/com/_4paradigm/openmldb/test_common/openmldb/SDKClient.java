package com._4paradigm.openmldb.test_common.openmldb;

import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.test_common.bean.OpenMLDBResult;
import com._4paradigm.openmldb.test_common.util.ResultUtil;
import com._4paradigm.openmldb.test_common.util.SDKUtil;
import com._4paradigm.openmldb.test_common.util.SQLUtil;
import com._4paradigm.openmldb.test_common.util.WaitUtil;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class SDKClient {
    private Statement statement;

    private SDKClient(SqlExecutor executor){
        this.statement = executor.getStatement();
    }
    public static SDKClient of(SqlExecutor executor){
        return new SDKClient(executor);
    }
    public OpenMLDBResult execute(String sql) {
        log.info("execute sql:{}",sql);
        OpenMLDBResult openMLDBResult = new OpenMLDBResult();
        openMLDBResult.setSql(sql);
        try {
            boolean ok = statement.execute(sql);
            openMLDBResult.setHaveResult(ok);
            openMLDBResult.setMsg("success");
            openMLDBResult.setOk(true);
            if(ok){
                ResultUtil.parseResultSet(statement,openMLDBResult);
            }
//            ResultChainManager.of().toOpenMLDBResult(statement,openMLDBResult);
        } catch (SQLException e) {
            openMLDBResult.setOk(false);
            openMLDBResult.setMsg(e.getMessage());
            e.printStackTrace();
        }
        log.info(openMLDBResult.toString());
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
    }
    public void useDB(String dbName){
        String sql = String.format("use %s",dbName);
        execute(sql);
    }
    public void createAndUseDB(String dbName){
        List<String> sqlList = new ArrayList<>();
        if (!SDKUtil.dbIsExist(statement,dbName)) {
            sqlList.add(String.format("create database %s;", dbName));
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
