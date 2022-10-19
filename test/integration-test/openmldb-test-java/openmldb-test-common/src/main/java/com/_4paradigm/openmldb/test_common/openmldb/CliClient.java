package com._4paradigm.openmldb.test_common.openmldb;

import com._4paradigm.openmldb.test_common.bean.OpenMLDBResult;
import com._4paradigm.openmldb.test_common.command.OpenMLDBCommandFacade;
import com._4paradigm.openmldb.test_common.util.*;
import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBInfo;
import lombok.extern.slf4j.Slf4j;
import org.testng.collections.Lists;

import java.util.*;

@Slf4j
public class CliClient {
    private OpenMLDBInfo openMLDBInfo;
    private String dbName;

    private CliClient(OpenMLDBInfo openMLDBInfo,String dbName){
        this.openMLDBInfo = openMLDBInfo;
        this.dbName = dbName;
    }
    public static CliClient of(OpenMLDBInfo openMLDBInfo,String dbName){
        return new CliClient(openMLDBInfo,dbName);
    }
    public void create(String dbName){
        List<String> sqlList = new ArrayList<>();
        if (!dbIsExist(dbName)) {
            sqlList.add(String.format("create database %s;", dbName));
        }
        OpenMLDBCommandFacade.sqls(openMLDBInfo, dbName, sqlList);
    }

    public boolean dbIsExist(String dbName){
        String sql = "show databases;";
        try {
            OpenMLDBResult openMLDBResult = OpenMLDBCommandFacade.sql(openMLDBInfo, dbName, sql);
            List<List<Object>> rows = openMLDBResult.getResult();
            for(List<Object> row:rows){
                if(row.get(0).equals(dbName)){
                    return true;
                }
            }
            return false;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public OpenMLDBResult execute(String sql) {
        OpenMLDBResult openMLDBResult = OpenMLDBCommandFacade.sql(openMLDBInfo, dbName, sql);
        openMLDBResult.setSql(sql);;
        return openMLDBResult;
    }
    public OpenMLDBResult execute(List<String> sqlList) {
        OpenMLDBResult openMLDBResult = null;
        for(String sql:sqlList){
            openMLDBResult = execute(sql);
        }
        return openMLDBResult;
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
    public void setGlobalOnline(){
        String sql = "set @@global.execute_mode='online';";
        execute(sql);
    }
    public Map<String,List<Long>> showTableStatus(){
        OpenMLDBResult openMLDBResult = execute("show table status;");
        List<List<Object>> result = openMLDBResult.getResult();
        Map<String,List<Long>> map = new HashMap<>();
        result.forEach(l->map.put(String.valueOf(l.get(1)), Lists.newArrayList(Long.parseLong(String.valueOf(l.get(4))))));
        return map;
    }
}
