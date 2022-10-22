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

package com._4paradigm.openmldb.test_common.util;

import com._4paradigm.openmldb.test_common.bean.OpenMLDBResult;
import com._4paradigm.openmldb.jdbc.CallablePreparedStatement;
import com._4paradigm.openmldb.jdbc.SQLResultSet;
import com._4paradigm.openmldb.sdk.QueryFuture;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.test_common.chain.result.ResultParserManager;
import com._4paradigm.openmldb.test_common.model.InputDesc;
import com._4paradigm.openmldb.test_common.model.OpenmldbDeployment;
import com._4paradigm.openmldb.test_common.model.SQLCase;
import com._4paradigm.openmldb.test_common.openmldb.OpenMLDBGlobalVar;
import com._4paradigm.openmldb.test_common.openmldb.SDKClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import org.testng.collections.Lists;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @author zhaowei
 * @date 2020/6/17 4:00 PM
 */
@Slf4j
public class SDKUtil {
//    private static final log log = new LogProxy(log);

    public static OpenMLDBResult sqlList(SqlExecutor executor, String dbName, List<String> sqls) {
        OpenMLDBResult openMLDBResult = null;
        for (String sql : sqls) {
            openMLDBResult = sql(executor, dbName, sql);
        }
        return openMLDBResult;
    }

    public static OpenMLDBResult executeLongWindowDeploy(SqlExecutor executor, SQLCase sqlCase, boolean isAsyn) throws SQLException {
        return executeLongWindowDeploy(executor,sqlCase,sqlCase.getSql(),isAsyn);
    }

    public static OpenMLDBResult executeLongWindowDeploy(SqlExecutor executor, SQLCase sqlCase, String sql, boolean isAsyn) throws SQLException {
        String deploySQL = SQLUtil.getLongWindowDeploySQL(sqlCase.getSpName(),sqlCase.getLongWindow(),sql);
        log.info("long window deploy sql: {}", deploySQL);
        return SDKUtil.sqlRequestModeWithProcedure(
                executor, sqlCase.getDb(), sqlCase.getSpName(), null == sqlCase.getBatch_request(),
                deploySQL, sqlCase.getInputs().get(0), isAsyn);
    }

    public static OpenMLDBResult deploy(SqlExecutor sqlExecutor,String sql){
        OpenMLDBResult openMLDBResult = new OpenMLDBResult();
        openMLDBResult.setSql(sql);
        Statement statement = sqlExecutor.getStatement();
        try {
            statement.execute(sql);
            openMLDBResult.setOk(true);
            openMLDBResult.setMsg("success");
        } catch (SQLException e) {
            openMLDBResult.setOk(false);
            openMLDBResult.setMsg(e.getMessage());
            e.printStackTrace();
        }
        log.info("deploy:{}",openMLDBResult);
        return openMLDBResult;
    }

    public static OpenMLDBResult sqlRequestMode(SqlExecutor executor, String dbName,
                                                Boolean need_insert_request_row, String sql, InputDesc input) {
        OpenMLDBResult openMLDBResult = null;
        if (sql.toLowerCase().startsWith("select")||sql.toLowerCase().startsWith("deploy")) {
            openMLDBResult = selectRequestModeWithPreparedStatement(executor, dbName, need_insert_request_row, sql, input);
        } else {
            log.error("unsupport sql: {}", sql);
        }
        return openMLDBResult;
    }

    public static OpenMLDBResult sqlBatchRequestMode(SqlExecutor executor, String dbName,
                                                     String sql, InputDesc input,
                                                     List<Integer> commonColumnIndices) {
        OpenMLDBResult openMLDBResult = null;
        if (sql.toLowerCase().startsWith("select")) {
            openMLDBResult = selectBatchRequestModeWithPreparedStatement(
                    executor, dbName, sql, input, commonColumnIndices);
        } else {
            log.error("unsupport sql: {}", sql);
        }
        return openMLDBResult;
    }

    public static OpenMLDBResult sqlRequestModeWithProcedure(SqlExecutor executor, String dbName, String spName,
                                                             Boolean needInsertRequestRow, String sql,
                                                             InputDesc rows, boolean isAsyn) throws SQLException {
        OpenMLDBResult openMLDBResult = null;
        if (sql.toLowerCase().startsWith("create procedure") || sql.toLowerCase().startsWith("deploy ")) {
            openMLDBResult = selectRequestModeWithSp(executor, dbName, spName, needInsertRequestRow, sql, rows, isAsyn);
        } else {
            throw new IllegalArgumentException("not support sql: "+ sql);
        }
        return openMLDBResult;
    }

    public static OpenMLDBResult sql(SqlExecutor executor, String dbName, String sql) {
        useDB(executor,dbName);
        OpenMLDBResult openMLDBResult = null;
        if (sql.startsWith("create database") || sql.startsWith("drop database")) {
            openMLDBResult = db(executor, sql);
        }else if(sql.startsWith("CREATE INDEX")||sql.startsWith("create index")){
            openMLDBResult = createIndex(executor, sql);
        }else if (sql.startsWith("create") || sql.startsWith("CREATE") || sql.startsWith("DROP")|| sql.startsWith("drop")) {
            openMLDBResult = ddl(executor, dbName, sql);
        }else if (sql.startsWith("insert")||sql.startsWith("INSERT")) {
            openMLDBResult = insert(executor, dbName, sql);
        }else if (sql.startsWith("delete from")) {
            openMLDBResult = delete(executor, dbName, sql);
        }else if(sql.startsWith("show deployments;")){
            openMLDBResult  = showDeploys(executor,dbName,sql);
        }else if(sql.startsWith("show deployment")){
            openMLDBResult = showDeploy(executor, dbName, sql);
        }else if(sql.startsWith("desc ")){
            openMLDBResult = desc(executor,dbName,sql);
        }else if(sql.contains("outfile")){
            openMLDBResult = selectInto(executor, dbName, sql);
        }else if(sql.contains("deploy ")){
            openMLDBResult = deploy(executor, sql);
        }else if(sql.startsWith("set")){
            openMLDBResult = set(executor, dbName, sql);
        }else {
            openMLDBResult = select(executor, dbName, sql);
        }
        openMLDBResult.setSql(sql);
        log.info("openMLDBResult:{}",openMLDBResult);
        return openMLDBResult;
    }

    public static OpenMLDBResult selectInto(SqlExecutor executor, String dbName, String outSql){
        if (outSql.isEmpty()){
            return null;
        }
        log.info("select into:{}",outSql);
        OpenMLDBResult openMLDBResult = new OpenMLDBResult();
        ResultSet rawRs = executor.executeSQL(dbName, outSql);
        if (rawRs == null) {
            openMLDBResult.setOk(false);
            openMLDBResult.setMsg("executeSQL fail, result is null");
        } else if  (rawRs instanceof SQLResultSet){
            try {
                SQLResultSet rs = (SQLResultSet)rawRs;
                openMLDBResult.setOk(true);
            } catch (Exception e) {
                openMLDBResult.setOk(false);
                openMLDBResult.setMsg(e.getMessage());
            }
        }
        log.info("select result:{} \n", openMLDBResult);
        return openMLDBResult;
    }

    public static OpenMLDBResult showDeploy(SqlExecutor executor, String dbName, String showDeploySql){
        if (showDeploySql.isEmpty()){
            return null;
        }
        log.info("show deployment:{}",showDeploySql);
        OpenMLDBResult openMLDBResult = new OpenMLDBResult();
        ResultSet rawRs = executor.executeSQL(dbName, showDeploySql);
        if (rawRs == null) {
            openMLDBResult.setOk(false);
            openMLDBResult.setMsg("executeSQL fail, result is null");
        } else if  (rawRs instanceof SQLResultSet){
            try {
                SQLResultSet rs = (SQLResultSet)rawRs;
                ResultUtil.setSchema(rs.getMetaData(),openMLDBResult);
                openMLDBResult.setOk(true);
                String deployStr = ResultUtil.convertResultSetToListDeploy(rs);
                String[] strings = deployStr.split("\n");
                List<String> stringList = Arrays.asList(strings);
                OpenmldbDeployment openmldbDeployment = ResultUtil.parseDeployment(stringList);
                openMLDBResult.setDeployment(openmldbDeployment);
            } catch (Exception e) {
                openMLDBResult.setOk(false);
                openMLDBResult.setMsg(e.getMessage());
            }
        }
        log.info("select result:{} \n", openMLDBResult);
        return openMLDBResult;
    }

    public static OpenMLDBResult showDeploys(SqlExecutor executor, String dbName, String showdeploySqls){
        if (showdeploySqls.isEmpty()){
            return null;
        }
        log.info("show deployments:{}",showdeploySqls);
        OpenMLDBResult openMLDBResult = new OpenMLDBResult();
        ResultSet rawRs = executor.executeSQL(dbName, showdeploySqls);
        if (rawRs == null) {
            openMLDBResult.setOk(false);
            openMLDBResult.setMsg("executeSQL fail, result is null");
        } else if  (rawRs instanceof SQLResultSet){
            try {
                SQLResultSet rs = (SQLResultSet)rawRs;
                ResultUtil.setSchema(rs.getMetaData(),openMLDBResult);
                openMLDBResult.setOk(true);
                List<List<Object>> lists = ResultUtil.toList(rs);
                if(lists.size() == 0 ||lists.isEmpty()){
                    openMLDBResult.setDeploymentCount(0);
                }else {
                    openMLDBResult.setDeploymentCount(lists.size());
                }
                //String[] strings = deployStr.split("\n");
                //List<String> stringList = Arrays.asList(strings);

            } catch (Exception e) {
                openMLDBResult.setOk(false);
                openMLDBResult.setMsg(e.getMessage());
            }
        }
        return openMLDBResult;
    }



    public static OpenMLDBResult desc(SqlExecutor executor, String dbName, String descSql){
        if (descSql.isEmpty()){
            return null;
        }
        log.info("desc:{}",descSql);
        OpenMLDBResult openMLDBResult = new OpenMLDBResult();
        openMLDBResult.setSql(descSql);
        ResultSet rawRs = executor.executeSQL(dbName, descSql);

        if (rawRs == null) {
            openMLDBResult.setOk(false);
            openMLDBResult.setMsg("executeSQL fail, result is null");
        } else if  (rawRs instanceof SQLResultSet){
            try {
                SQLResultSet rs = (SQLResultSet)rawRs;
                ResultUtil.setSchema(rs.getMetaData(),openMLDBResult);
                openMLDBResult.setOk(true);
                List<List<Object>> result = ResultUtil.toList(rs);
                openMLDBResult.setResult(result);
                ResultParserManager.of().parseResult(openMLDBResult);
            } catch (Exception e) {
                e.printStackTrace();
                openMLDBResult.setOk(false);
                openMLDBResult.setMsg(e.getMessage());
            }
        }
        log.info("create index result:{}", openMLDBResult);
        return openMLDBResult;
    }


    public static OpenMLDBResult createIndex(SqlExecutor executor, String sql) {
        if (sql.isEmpty()) {
            return null;
        }
        log.info("ddl sql:{}", sql);
        OpenMLDBResult openMLDBResult = new OpenMLDBResult();
        boolean createOk = false;
        try {
            createOk = executor.getStatement().execute(sql);
            openMLDBResult.setOk(true);
            Tool.sleep(20*1000);
        } catch (Exception e) {
            e.printStackTrace();
            openMLDBResult.setOk(false);
            openMLDBResult.setMsg(e.getMessage());
        }
        log.info("create index result:{}", openMLDBResult);
        return openMLDBResult;
    }


    public static OpenMLDBResult insert(SqlExecutor executor, String dbName, String insertSql) {
        if (insertSql.isEmpty()) {
            return null;
        }
        log.info("insert sql:{}", insertSql);

        useDB(executor,dbName);
        OpenMLDBResult openMLDBResult = new OpenMLDBResult();
        Statement statement = executor.getStatement();
        try {
            statement.execute(insertSql);
            openMLDBResult.setOk(true);
            openMLDBResult.setMsg("success");
        } catch (Exception e) {
            e.printStackTrace();
            openMLDBResult.setOk(false);
            openMLDBResult.setMsg(e.getMessage());
        }finally {
            try {
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

//        OpenMLDBResult openMLDBResult = new OpenMLDBResult();
//        boolean createOk = executor.executeInsert(dbName, insertSql);
//        openMLDBResult.setOk(createOk);
//        log.info("insert result:{}" + openMLDBResult);
        return openMLDBResult;
    }
    public static OpenMLDBResult delete(SqlExecutor executor, String dbName, String deleteSql) {
        useDB(executor,dbName);
        OpenMLDBResult openMLDBResult = new OpenMLDBResult();
        Statement statement = executor.getStatement();
        try {
            statement.execute(deleteSql);
            openMLDBResult.setOk(true);
            openMLDBResult.setMsg("success");
        } catch (Exception e) {
            e.printStackTrace();
            openMLDBResult.setOk(false);
            openMLDBResult.setMsg(e.getMessage());
        }finally {
            try {
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return openMLDBResult;
    }
    public static OpenMLDBResult set(SqlExecutor executor, String dbName, String sql) {
        useDB(executor,dbName);
        OpenMLDBResult openMLDBResult = new OpenMLDBResult();
        Statement statement = executor.getStatement();
        try {
            statement.execute(sql);
            openMLDBResult.setOk(true);
            openMLDBResult.setMsg("success");
        } catch (Exception e) {
            e.printStackTrace();
            openMLDBResult.setOk(false);
            openMLDBResult.setMsg(e.getMessage());
        }finally {
            try {
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return openMLDBResult;
    }

    public static OpenMLDBResult selectWithPrepareStatement(SqlExecutor executor, String dbName, String sql, List<String> paramterTypes, List<Object> params) {
        OpenMLDBResult openMLDBResult = new OpenMLDBResult();
        try {
            if (sql.isEmpty()) {
                return null;
            }
            log.info("prepare sql:{}", sql);
            PreparedStatement preparedStmt = executor.getPreparedStatement(dbName, sql);
            DataUtil.setPreparedData(preparedStmt,paramterTypes,params);
            ResultSet resultSet = preparedStmt.executeQuery();

            if (resultSet == null) {
                openMLDBResult.setOk(false);
                openMLDBResult.setMsg("executeSQL fail, result is null");
            } else if  (resultSet instanceof SQLResultSet){
                try {
                    SQLResultSet rs = (SQLResultSet)resultSet;
                    ResultUtil.setSchema(rs.getMetaData(),openMLDBResult);
                    openMLDBResult.setOk(true);
                    List<List<Object>> result = ResultUtil.toList(rs);
                    openMLDBResult.setCount(result.size());
                    openMLDBResult.setResult(result);
                } catch (Exception e) {
                    openMLDBResult.setOk(false);
                    openMLDBResult.setMsg(e.getMessage());
                }
            }
            log.info("insert result:{}" + openMLDBResult);
        }catch (Exception e){
            e.printStackTrace();
            openMLDBResult.setOk(false);
            openMLDBResult.setMsg(e.getMessage());
        }
        return openMLDBResult;
    }

    public static OpenMLDBResult insertWithPrepareStatement(SqlExecutor executor, String dbName, String insertSql, List<Object> params) {
        OpenMLDBResult openMLDBResult = new OpenMLDBResult();
        try {
            if (insertSql.isEmpty()) {
                return null;
            }
            log.info("prepare sql:{}", insertSql);
            PreparedStatement preparedStmt = executor.getInsertPreparedStmt(dbName, insertSql);
            DataUtil.setRequestData(preparedStmt,params);
            // for(int i=0;i<types.size();i++){
                // preparedStatementSetValue(preparedStmt,i+1,types.get(i).split("\\s+")[1],params.get(i));
            // }
            boolean createOk =preparedStmt.execute();
            openMLDBResult.setOk(createOk);
            log.info("insert result:{}" + openMLDBResult);
        }catch (Exception e){
            e.printStackTrace();
            openMLDBResult.setOk(false);
            openMLDBResult.setMsg(e.getMessage());
        }
        return openMLDBResult;
    }

    public static OpenMLDBResult db(SqlExecutor executor, String ddlSql) {
        if (ddlSql.isEmpty()) {
            return null;
        }
        log.info("db sql:{}", ddlSql);
        String dbName = ddlSql.substring(0,ddlSql.lastIndexOf(";")).split("\\s+")[2];
        boolean db = false;
        if(ddlSql.startsWith("create database")){
            db = executor.createDB(dbName);
        }else{
            db = executor.dropDB(dbName);
        }
        OpenMLDBResult openMLDBResult = new OpenMLDBResult();
        openMLDBResult.setOk(db);
        log.info("db result:{}", openMLDBResult);
        return openMLDBResult;
    }

    public static OpenMLDBResult ddl(SqlExecutor executor, String dbName, String ddlSql) {
        if (ddlSql.isEmpty()) {
            return null;
        }
        log.info("ddl sql:{}", ddlSql);
        OpenMLDBResult openMLDBResult = new OpenMLDBResult();
        boolean createOk = executor.executeDDL(dbName, ddlSql);
        openMLDBResult.setOk(createOk);
        log.info("ddl result:{}", openMLDBResult);
        return openMLDBResult;
    }


    private static OpenMLDBResult selectRequestModeWithPreparedStatement(SqlExecutor executor, String dbName,
                                                                         Boolean need_insert_request_row,
                                                                         String selectSql, InputDesc input) {
        if (selectSql.isEmpty()) {
            log.error("fail to execute sql in request mode: select sql is empty");
            return null;
        }

        List<List<Object>> rows = null == input ? null : input.getRows();
        if (CollectionUtils.isEmpty(rows)) {
            log.error("fail to execute sql in request mode: request rows is null or empty");
            return null;
        }
        List<String> inserts = input.extractInserts();
        if (CollectionUtils.isEmpty(inserts)) {
            log.error("fail to execute sql in request mode: fail to build insert sql for request rows");
            return null;
        }

        if (rows.size() != inserts.size()) {
            log.error("fail to execute sql in request mode: rows size isn't match with inserts size");
            return null;
        }

        String insertDbName= input.getDb().isEmpty() ? dbName : input.getDb();
        log.info("select sql:{}", selectSql);
        OpenMLDBResult openMLDBResult = new OpenMLDBResult();
        List<List<Object>> result = Lists.newArrayList();
        for (int i = 0; i < rows.size(); i++) {
            PreparedStatement rps = null;
            try {
                rps = executor.getRequestPreparedStmt(dbName, selectSql);
            } catch (SQLException e) {
                openMLDBResult.setOk(false);
                openMLDBResult.setMsg(e.getMessage());
                e.printStackTrace();
                return openMLDBResult;
            }
            ResultSet resultSet = null;
            try {
                resultSet = buildRequestPreparedStatement(rps, rows.get(i));

            } catch (SQLException throwables) {
                openMLDBResult.setOk(false);
                openMLDBResult.setMsg("Build Request PreparedStatement Fail");
                return openMLDBResult;
            }
            if (resultSet == null) {
                openMLDBResult.setOk(false);
                openMLDBResult.setMsg("Select result is null");
                log.error("select result:{}", openMLDBResult);
                return openMLDBResult;
            }
            try {
                result.addAll(ResultUtil.toList((SQLResultSet) resultSet));
            } catch (SQLException throwables) {
                openMLDBResult.setOk(false);
                openMLDBResult.setMsg("Convert Result Set To List Fail");
                return openMLDBResult;
            }
            if (need_insert_request_row && !executor.executeInsert(insertDbName, inserts.get(i))) {
                openMLDBResult.setOk(false);
                openMLDBResult.setMsg("Fail to execute sql in request mode fail to insert request row after query");
                log.error(openMLDBResult.getMsg());
                return openMLDBResult;
            }
            if (i == rows.size()-1) {
                try {
                    ResultUtil.setSchema(resultSet.getMetaData(),openMLDBResult);
                } catch (SQLException throwables) {
                    openMLDBResult.setOk(false);
                    openMLDBResult.setMsg("Fail to set meta data");
                    return openMLDBResult;
                }
            }
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (rps != null) {
                    rps.close();
                }
            } catch (Exception throwables) {
                throwables.printStackTrace();
            }
        }
        openMLDBResult.setResult(result);
        openMLDBResult.setCount(result.size());
        openMLDBResult.setOk(true);

        log.info("select result:{}", openMLDBResult);
        return openMLDBResult;
    }

    private static OpenMLDBResult selectBatchRequestModeWithPreparedStatement(SqlExecutor executor, String dbName,
                                                                              String selectSql, InputDesc input,
                                                                              List<Integer> commonColumnIndices) {
        if (selectSql.isEmpty()) {
            log.error("fail to execute sql in batch request mode: select sql is empty");
            return null;
        }
        List<List<Object>> rows = null == input ? null : input.getRows();
        if (CollectionUtils.isEmpty(rows)) {
            log.error("fail to execute sql in batch request mode: request rows is null or empty");
            return null;
        }
        List<String> inserts = input.extractInserts();
        if (CollectionUtils.isEmpty(inserts)) {
            log.error("fail to execute sql in batch request mode: fail to build insert sql for request rows");
            return null;
        }
        if (rows.size() != inserts.size()) {
            log.error("fail to execute sql in batch request mode: rows size isn't match with inserts size");
            return null;
        }
        log.info("select sql:{}", selectSql);
        OpenMLDBResult fesqlResult = new OpenMLDBResult();

        PreparedStatement rps = null;
        SQLResultSet sqlResultSet = null;
        try {
            rps = executor.getBatchRequestPreparedStmt(dbName, selectSql, commonColumnIndices);

            for (List<Object> row : rows) {
                boolean ok = DataUtil.setRequestData(rps, row);
                if (ok) {
                    rps.addBatch();
                }
            }

            sqlResultSet = (SQLResultSet) rps.executeQuery();
            List<List<Object>> result = Lists.newArrayList();
            result.addAll(ResultUtil.toList(sqlResultSet));
            fesqlResult.setResult(result);
            ResultUtil.setSchema(sqlResultSet.getMetaData(),fesqlResult);
            fesqlResult.setCount(result.size());
            // fesqlResult.setResultSchema(sqlResultSet.GetInternalSchema());

        } catch (SQLException sqlException) {
            fesqlResult.setOk(false);
            fesqlResult.setMsg("Fail to execute batch request");
            sqlException.printStackTrace();
        } finally {
            try {
                if (sqlResultSet != null) {
                    sqlResultSet.close();
                }
                if (rps != null) {
                    rps.close();
                }
            } catch (SQLException closeException) {
                closeException.printStackTrace();
            }
        }
        fesqlResult.setOk(true);
        log.info("select result:{}", fesqlResult);
        return fesqlResult;
    }

    private static OpenMLDBResult selectRequestModeWithSp(SqlExecutor executor, String dbName, String spName,
                                                          Boolean needInsertRequestRow,
                                                          String sql, InputDesc input, boolean isAsyn) {
        if (sql.isEmpty()) {
            log.error("fail to execute sql in request mode: select sql is empty");
            return null;
        }

        List<List<Object>> rows = null == input ? null : input.getRows();
        if (CollectionUtils.isEmpty(rows)) {
            log.error("fail to execute sql in request mode: request rows is null or empty");
            return null;
        }
        List<String> inserts = needInsertRequestRow ? input.extractInserts() : Lists.newArrayList();
        if (needInsertRequestRow){
            if (CollectionUtils.isEmpty(inserts)) {
                log.error("fail to execute sql in request mode: fail to build insert sql for request rows");
                return null;
            }
            if (rows.size() != inserts.size()) {
                log.error("fail to execute sql in request mode: rows size isn't match with inserts size");
                return null;
            }
        }

        log.info("procedure sql:{}", sql);
        String insertDbName = input.getDb().isEmpty() ? dbName : input.getDb();
        OpenMLDBResult openMLDBResult = new OpenMLDBResult();
        openMLDBResult.setSpName(spName);
        if(sql.startsWith("deploy ")){
            OpenMLDBResult deployResult = deploy(executor, sql);
            if(!deployResult.isOk()){
                return deployResult;
            }
        }else if (!executor.executeDDL(dbName, sql)) {
            log.error("execute ddl failed! sql: {}", sql);
            openMLDBResult.setOk(false);
            openMLDBResult.setMsg("execute ddl failed");
            return openMLDBResult;
        }
        List<List<Object>> result = Lists.newArrayList();
        for (int i = 0; i < rows.size(); i++) {
            Object[] objects = new Object[rows.get(i).size()];
            for (int k = 0; k < objects.length; k++) {
                objects[k] = rows.get(i).get(k);
            }
            CallablePreparedStatement rps = null;
            ResultSet resultSet = null;
            try {
                rps = executor.getCallablePreparedStmt(dbName, spName);
                if (rps == null) {
                    openMLDBResult.setOk(false);
                    openMLDBResult.setMsg("Fail to getCallablePreparedStmt");
                    return openMLDBResult;
                }
                if (!isAsyn) {
                    resultSet = buildRequestPreparedStatement(rps, rows.get(i));
                } else {
                    resultSet = buildRequestPreparedStatementAsync(rps, rows.get(i));
                }
                if (resultSet == null) {
                    openMLDBResult.setOk(false);
                    openMLDBResult.setMsg("result set is null");
                    log.error("select result:{}", openMLDBResult);
                    return openMLDBResult;
                }
                result.addAll(ResultUtil.toList((SQLResultSet) resultSet));
                if (needInsertRequestRow && !executor.executeInsert(insertDbName, inserts.get(i))) {
                    openMLDBResult.setOk(false);
                    openMLDBResult.setMsg("fail to execute sql in request mode: fail to insert request row after query");
                    log.error(openMLDBResult.getMsg());
                    return openMLDBResult;
                }
                if (i == 0) {
                    try {
                        ResultUtil.setSchema(resultSet.getMetaData(),openMLDBResult);
                    } catch (SQLException throwables) {
                        openMLDBResult.setOk(false);
                        openMLDBResult.setMsg("fail to get/set meta data");
                        return openMLDBResult;
                    }
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
                log.error("has exception. sql: {}", sql);
                openMLDBResult.setOk(false);
                openMLDBResult.setMsg("fail to execute sql");
                return openMLDBResult;
            } finally {
                try {
                    if (resultSet != null) resultSet.close();
                    if (rps != null) rps.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }
        openMLDBResult.setResult(result);
        openMLDBResult.setCount(result.size());
        openMLDBResult.setOk(true);
        log.info("select result:{}", openMLDBResult);
        return openMLDBResult;
    }

    public static OpenMLDBResult selectBatchRequestModeWithSp(SqlExecutor executor, String dbName, String spName,
                                                              String sql, InputDesc input, boolean isAsyn) {
        if (sql.isEmpty()) {
            log.error("fail to execute sql in batch request mode: select sql is empty");
            return null;
        }
        List<List<Object>> rows = null == input ? null : input.getRows();
        if (CollectionUtils.isEmpty(rows)) {
            log.error("fail to execute sql in batch request mode: request rows is null or empty");
            return null;
        }
        log.info("procedure sql: {}", sql);
        OpenMLDBResult openMLDBResult = new OpenMLDBResult();
        if (!executor.executeDDL(dbName, sql)) {
            openMLDBResult.setOk(false);
            openMLDBResult.setMsg("fail to execute ddl");
            return openMLDBResult;
        }
        Object[][] rowArray = new Object[rows.size()][];
        for (int i = 0; i < rows.size(); ++i) {
            List<Object> row = rows.get(i);
            rowArray[i] = new Object[row.size()];
            for (int j = 0; j < row.size(); ++j) {
                rowArray[i][j] = row.get(j);
            }
        }
        CallablePreparedStatement rps = null;
        ResultSet sqlResultSet = null;
        try {
            rps = executor.getCallablePreparedStmtBatch(dbName, spName);
            if (rps == null) {
                openMLDBResult.setOk(false);
                openMLDBResult.setMsg("fail to getCallablePreparedStmtBatch");
                return openMLDBResult;
            }
            for (List<Object> row : rows) {
                boolean ok = DataUtil.setRequestData(rps, row);
                if (ok) {
                    rps.addBatch();
                }
            }

            if (!isAsyn) {
                sqlResultSet = rps.executeQuery();
            } else {
                QueryFuture future = rps.executeQueryAsync(10000, TimeUnit.MILLISECONDS);
                try {
                    sqlResultSet = future.get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
            List<List<Object>> result = Lists.newArrayList();
            result.addAll(ResultUtil.toList((SQLResultSet) sqlResultSet));
            openMLDBResult.setResult(result);
            ResultUtil.setSchema(sqlResultSet.getMetaData(),openMLDBResult);
            openMLDBResult.setCount(result.size());

        } catch (SQLException e) {
            log.error("Call procedure failed", e);
            openMLDBResult.setOk(false);
            openMLDBResult.setMsg("Call procedure failed");
            return openMLDBResult;
        } finally {
            try {
                if (sqlResultSet != null) {
                    sqlResultSet.close();
                }
                if (rps != null) {
                    rps.close();
                }
            } catch (SQLException closeException) {
                closeException.printStackTrace();
            }
        }
        openMLDBResult.setOk(true);
        log.info("select result:{}", openMLDBResult);
        return openMLDBResult;
    }


//    private static boolean buildRequestRow(SQLRequestRow requestRow, List<Object> objects) {
//        Schema schema = requestRow.GetSchema();
//        int totalSize = 0;
//        for (int i = 0; i < schema.GetColumnCnt(); i++) {
//            if (null == objects.get(i)) {
//                continue;
//            }
//            if (DataType.kTypeString.equals(schema.GetColumnType(i))) {
//                totalSize += objects.get(i).toString().length();
//            }
//        }
//
//        log.info("init request row: {}", totalSize);
//        requestRow.Init(totalSize);
//        for (int i = 0; i < schema.GetColumnCnt(); i++) {
//            Object obj = objects.get(i);
//            if (null == obj) {
//                requestRow.AppendNULL();
//                continue;
//            }
//
//            DataType dataType = schema.GetColumnType(i);
//            if (DataType.kTypeInt16.equals(dataType)) {
//                requestRow.AppendInt16(Short.parseShort(obj.toString()));
//            } else if (DataType.kTypeInt32.equals(dataType)) {
//                requestRow.AppendInt32(Integer.parseInt(obj.toString()));
//            } else if (DataType.kTypeInt64.equals(dataType)) {
//                requestRow.AppendInt64(Long.parseLong(obj.toString()));
//            } else if (DataType.kTypeFloat.equals(dataType)) {
//                requestRow.AppendFloat(Float.parseFloat(obj.toString()));
//            } else if (DataType.kTypeDouble.equals(dataType)) {
//                requestRow.AppendDouble(Double.parseDouble(obj.toString()));
//            } else if (DataType.kTypeTimestamp.equals(dataType)) {
//                requestRow.AppendTimestamp(Long.parseLong(obj.toString()));
//            } else if (DataType.kTypeDate.equals(dataType)) {
//                try {
//                    Date date = new Date(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(obj.toString() + " 00:00:00").getTime());
//                    log.info("build request row: obj: {}, append date: {},  {}, {}, {}",
//                            obj, date.toString(), date.getYear() + 1900, date.getMonth() + 1, date.getDate());
//                    requestRow.AppendDate(date.getYear() + 1900, date.getMonth() + 1, date.getDate());
//                } catch (ParseException e) {
//                    log.error("Fail convert {} to date", obj.toString());
//                    return false;
//                }
//            } else if (DataType.kTypeString.equals(schema.GetColumnType(i))) {
//                requestRow.AppendString(obj.toString());
//            } else {
//                log.error("fail to build request row: invalid data type {]", schema.GetColumnType(i));
//                return false;
//            }
//        }
//        return requestRow.Build();
//    }


    private static ResultSet buildRequestPreparedStatement(PreparedStatement requestPs,
                                                           List<Object> objects) throws SQLException {
        boolean success = DataUtil.setRequestData(requestPs, objects);
        if (success) {
            return requestPs.executeQuery();
        } else {
            return null;
        }
    }

    private static ResultSet buildRequestPreparedStatementAsync(CallablePreparedStatement requestPs,
                                                                List<Object> objects) throws SQLException {
        boolean success = DataUtil.setRequestData(requestPs, objects);
        if (success) {
            QueryFuture future = requestPs.executeQueryAsync(1000, TimeUnit.MILLISECONDS);
            ResultSet sqlResultSet = null;
            try {
                sqlResultSet = future.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            return sqlResultSet;
        } else {
            return null;
        }
    }

    public static OpenMLDBResult select(SqlExecutor executor, String dbName, String selectSql) {
        if (selectSql.isEmpty()) {
            return null;
        }
        log.info("select sql:{}", selectSql);
        OpenMLDBResult openMLDBResult = new OpenMLDBResult();
        ResultSet rawRs = executor.executeSQL(dbName, selectSql);
        if (rawRs == null) {
            openMLDBResult.setOk(false);
            openMLDBResult.setMsg("executeSQL fail, result is null");
        } else if  (rawRs instanceof SQLResultSet){
            try {
                SQLResultSet rs = (SQLResultSet)rawRs;
                ResultUtil.setSchema(rs.getMetaData(),openMLDBResult);
                openMLDBResult.setOk(true);
                List<List<Object>> result = ResultUtil.toList(rs);
                openMLDBResult.setCount(result.size());
                openMLDBResult.setResult(result);
            } catch (Exception e) {
                openMLDBResult.setOk(false);
                openMLDBResult.setMsg(e.getMessage());
            }
        }
        log.info("select result:{} \n", openMLDBResult);
        return openMLDBResult;
    }

    // public static Object getColumnData(com._4paradigm.openmldb.ResultSet rs, Schema schema, int index) {
    //     Object obj = null;
    //     DataType dataType = schema.GetColumnType(index);
    //     if (rs.IsNULL(index)) {
    //         log.info("rs is null");
    //         return null;
    //     }
    //     if (dataType.equals(DataType.kTypeBool)) {
    //         obj = rs.GetBoolUnsafe(index);
    //     } else if (dataType.equals(DataType.kTypeDate)) {
    //         try {
    //             obj = new Date(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //                     .parse(rs.GetAsString(index) + " 00:00:00").getTime());
    //         } catch (ParseException e) {
    //             e.printStackTrace();
    //             return null;
    //         }
    //     } else if (dataType.equals(DataType.kTypeDouble)) {
    //         obj = rs.GetDoubleUnsafe(index);
    //     } else if (dataType.equals(DataType.kTypeFloat)) {
    //         obj = rs.GetFloatUnsafe(index);
    //     } else if (dataType.equals(DataType.kTypeInt16)) {
    //         obj = rs.GetInt16Unsafe(index);
    //     } else if (dataType.equals(DataType.kTypeInt32)) {
    //         obj = rs.GetInt32Unsafe(index);
    //     } else if (dataType.equals(DataType.kTypeInt64)) {
    //         obj = rs.GetInt64Unsafe(index);
    //     } else if (dataType.equals(DataType.kTypeString)) {
    //         obj = rs.GetStringUnsafe(index);
    //         log.info("conver string data {}", obj);
    //     } else if (dataType.equals(DataType.kTypeTimestamp)) {
    //         obj = new Timestamp(rs.GetTimeUnsafe(index));
    //     }
    //     return obj;
    // }



    public static OpenMLDBResult createTable(SqlExecutor executor, String dbName, String createSql){
        if (StringUtils.isNotEmpty(createSql)) {
            OpenMLDBResult res = SDKUtil.ddl(executor, dbName, createSql);
            if (!res.isOk()) {
                log.error("fail to create table");
                return res;
            }
            return res;
        }
        throw new IllegalArgumentException("create sql is null");
    }

    public static OpenMLDBResult createAndInsert(SqlExecutor executor,
                                                 String defaultDBName,
                                                 List<InputDesc> inputs,
                                                 boolean useFirstInputAsRequests) {
        // Create inputs' databasess if exist
        HashSet<String> dbNames = new HashSet<>();
        if (!StringUtils.isEmpty(defaultDBName)) {
            dbNames.add(defaultDBName);
        }
        if (!Objects.isNull(inputs)) {
            for (InputDesc input : inputs) {
                // CreateDB if input's db has been configured and hasn't been created before
                if (!StringUtils.isEmpty(input.getDb()) && !dbNames.contains(input.getDb())) {
                    boolean dbOk = executor.createDB(input.getDb());
                    dbNames.add(input.getDb());
                    log.info("create db:{},{}", input.getDb(), dbOk);
                }
            }
        }

        OpenMLDBResult openMLDBResult = new OpenMLDBResult();
        if (inputs != null && inputs.size() > 0) {
            for (int i = 0; i < inputs.size(); i++) {
                String tableName = inputs.get(i).getName();
                String createSql = inputs.get(i).extractCreate();
                if(StringUtils.isEmpty(createSql)){
                    continue;
                }
                createSql = SQLCase.formatSql(createSql, i, tableName);
                createSql = SQLUtil.formatSql(createSql, OpenMLDBGlobalVar.mainInfo);
                String dbName = inputs.get(i).getDb().isEmpty() ? defaultDBName : inputs.get(i).getDb();
                createTable(executor,dbName,createSql);
                InputDesc input = inputs.get(i);
                if (0 == i && useFirstInputAsRequests) {
                    continue;
                }
                List<String> inserts = inputs.get(i).extractInserts();
                for (String insertSql : inserts) {
                    insertSql = SQLCase.formatSql(insertSql, i, input.getName());
                    if (!insertSql.isEmpty()) {
                        OpenMLDBResult res = SDKUtil.insert(executor, dbName, insertSql);
                        if (!res.isOk()) {
                            log.error("fail to insert table");
                            return res;
                        }
                    }
                }
            }
        }
        openMLDBResult.setOk(true);
        return openMLDBResult;
    }

    public static OpenMLDBResult createAndInsertWithPrepared(SqlExecutor executor, String defaultDBName, List<InputDesc> inputs, boolean useFirstInputAsRequests) {
        OpenMLDBResult openMLDBResult = new OpenMLDBResult();
        if (inputs != null && inputs.size() > 0) {
            for (int i = 0; i < inputs.size(); i++) {
                String tableName = inputs.get(i).getName();
                String createSql = inputs.get(i).extractCreate();
                createSql = SQLCase.formatSql(createSql, i, tableName);
                String dbName = inputs.get(i).getDb().isEmpty() ? defaultDBName : inputs.get(i).getDb();
                createTable(executor,dbName,createSql);
                InputDesc input = inputs.get(i);
                if (0 == i && useFirstInputAsRequests) {
                    continue;
                }
                String insertSql = inputs.get(i).getPreparedInsert();
                insertSql = SQLCase.formatSql(insertSql, i, tableName);
                List<List<Object>> rows = input.getRows();
                for(List<Object> row:rows){
                    OpenMLDBResult res = SDKUtil.insertWithPrepareStatement(executor, dbName, insertSql, row);
                    if (!res.isOk()) {
                        log.error("fail to insert table");
                        return res;
                    }
                }
            }
        }
        openMLDBResult.setOk(true);
        return openMLDBResult;
    }

    public static void show(com._4paradigm.openmldb.ResultSet rs) {
        if (null == rs || rs.Size() == 0) {
            System.out.println("EMPTY RESULT");
            return;
        }
        StringBuffer sb = new StringBuffer();

        while (rs.Next()) {
            sb.append(rs.GetRowString()).append("\n");
        }
        log.info("RESULT:\n{} row in set\n{}", rs.Size(), sb.toString());
    }




    public static void useDB(SqlExecutor executor,String dbName){
        Statement statement = executor.getStatement();
        String sql = String.format("use %s",dbName);
        try {
            statement.execute(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void setOnline(SqlExecutor sqlExecutor){
        Statement statement = sqlExecutor.getStatement();
        try {
            statement.execute("SET @@execute_mode='online';");
            OpenMLDBGlobalVar.EXECUTE_MODE="online";
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public static boolean dbIsExist(Statement statement,String dbName){
        String sql = "show databases;";
        try {
            ResultSet resultSet = statement.executeQuery(sql);
            List<List<Object>> rows = ResultUtil.toList((SQLResultSet) resultSet);
            for(List<Object> row:rows){
                if(row.get(0).equals(dbName)){
                    return true;
                }
            }
            return false;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
