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

package com._4paradigm.openmldb.java_sdk_test.util;

import com._4paradigm.openmldb.DataType;
import com._4paradigm.openmldb.SQLRequestRow;
import com._4paradigm.openmldb.Schema;
import com._4paradigm.openmldb.java_sdk_test.common.FedbGlobalVar;
import com._4paradigm.openmldb.java_sdk_test.entity.FesqlResult;
import com._4paradigm.openmldb.jdbc.CallablePreparedStatement;
import com._4paradigm.openmldb.jdbc.SQLResultSet;
import com._4paradigm.openmldb.sdk.QueryFuture;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.test_common.bean.FEDBInfo;
import com._4paradigm.openmldb.test_common.common.LogProxy;
import com._4paradigm.openmldb.test_common.model.InputDesc;
import com._4paradigm.openmldb.test_common.model.SQLCase;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.testng.collections.Lists;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author zhaowei
 * @date 2020/6/17 4:00 PM
 */
@Slf4j
public class FesqlUtil {
    private static String reg = "\\{(\\d+)\\}";
    private static Pattern pattern = Pattern.compile(reg);
    private static final Logger logger = new LogProxy(log);

    public static String buildSpSQLWithConstColumns(String spName,
                                                    String sql,
                                                    InputDesc input) throws SQLException {
        StringBuilder builder = new StringBuilder("create procedure " + spName + "(");
        HashSet<Integer> commonColumnIndices = new HashSet<>();
        if (input.getCommon_column_indices() != null) {
            for (String str : input.getCommon_column_indices()) {
                if (str != null) {
                    commonColumnIndices.add(Integer.parseInt(str));
                }
            }
        }
        if (input.getColumns() == null) {
            throw new SQLException("No schema defined in input desc");
        }
        for (int i = 0; i < input.getColumns().size(); ++i) {
            String[] parts = input.getColumns().get(i).split(" ");
            if (commonColumnIndices.contains(i)) {
                builder.append("const ");
            }
            builder.append(parts[0]);
            builder.append(" ");
            builder.append(parts[1]);
            if (i != input.getColumns().size() - 1) {
                builder.append(",");
            }
        }
        builder.append(") ");
        builder.append("BEGIN ");
        builder.append(sql.trim());
        builder.append(" ");
        builder.append("END;");
        sql = builder.toString();
        return sql;
    }

    public static int getIndexByColumnName(List<String> columnNames, String columnName) {
        for (int i = 0; i < columnNames.size(); i++) {
            if (columnNames.get(i).equals(columnName)) {
                return i;
            }
        }
        return -1;
    }

    public static DataType getColumnType(String type) {
        switch (type) {
            case "smallint":
            case "int16":
                return DataType.kTypeInt16;
            case "int32":
            case "i32":
            case "int":
                return DataType.kTypeInt32;
            case "int64":
            case "bigint":
                return DataType.kTypeInt64;
            case "float":
                return DataType.kTypeFloat;
            case "double":
                return DataType.kTypeDouble;
            case "bool":
                return DataType.kTypeBool;
            case "string":
                return DataType.kTypeString;
            case "timestamp":
                return DataType.kTypeTimestamp;
            case "date":
                return DataType.kTypeDate;
            default:
                return null;
        }
    }

    public static DataType getColumnTypeByJDBC(String type) {
        switch (type) {
            case "smallint":
            case "int16":
                return DataType.kTypeInt16;
            case "int32":
            case "i32":
            case "int":
            case "bool":
                return DataType.kTypeInt32;
            case "int64":
            case "bigint":
                return DataType.kTypeInt64;
            case "float":
                return DataType.kTypeFloat;
            case "double":
                return DataType.kTypeDouble;
            // case "bool":
            //     return DataType.kTypeBool;
            case "string":
                return DataType.kTypeString;
            case "timestamp":
                return DataType.kTypeTimestamp;
            case "date":
                return DataType.kTypeDate;
            default:
                return null;
        }
    }

    public static int getSQLType(String type) {
        switch (type) {
            case "smallint":
            case "int16":
                return Types.SMALLINT;
            case "int32":
            case "i32":
            case "int":
                return Types.INTEGER;
            case "int64":
            case "bigint":
                return Types.BIGINT;
            case "float":
                return Types.FLOAT;
            case "double":
                return Types.DOUBLE;
            case "bool":
                return Types.BOOLEAN;
            case "string":
                return Types.VARCHAR;
            case "timestamp":
                return Types.TIMESTAMP;
            case "date":
                return Types.DATE;
            default:
                return 0;
        }
    }

    public static String getColumnTypeString(DataType dataType) {
        if (dataType.equals(DataType.kTypeBool)) {
            return "bool";
        } else if (dataType.equals(DataType.kTypeString)) {
            return "string";
        } else if (dataType.equals(DataType.kTypeInt16)) {
            return "smallint";
        } else if (dataType.equals(DataType.kTypeInt32)) {
            return "int";
        } else if (dataType.equals(DataType.kTypeInt64)) {
            return "bigint";
        } else if (dataType.equals(DataType.kTypeFloat)) {
            return "float";
        } else if (dataType.equals(DataType.kTypeDouble)) {
            return "double";
        } else if (dataType.equals(DataType.kTypeTimestamp)) {
            return "timestamp";
        } else if (dataType.equals(DataType.kTypeDate)) {
            return "date";
        }
        return null;
    }

    public static String getSQLTypeString(int dataType) {
        switch (dataType){
            case Types.BIT:
            case Types.BOOLEAN:
                return "bool";
            case Types.VARCHAR:
                return "string";
            case Types.SMALLINT:
                return "smallint";
            case Types.INTEGER:
                return "int";
            case Types.BIGINT:
                return "bigint";
            case Types.REAL:
            case Types.FLOAT:
                return "float";
            case Types.DOUBLE:
                return "double";
            case Types.TIMESTAMP:
                return "timestamp";
            case Types.DATE:
                return "date";
            default:
                return null;
        }
    }

    public static FesqlResult sqls(SqlExecutor executor, String dbName, List<String> sqls) {
        FesqlResult fesqlResult = null;
        for (String sql : sqls) {
            fesqlResult = sql(executor, dbName, sql);
        }
        return fesqlResult;
    }

    public static FesqlResult sqlRequestMode(SqlExecutor executor, String dbName,
                                             Boolean need_insert_request_row, String sql, InputDesc input) throws SQLException {
        FesqlResult fesqlResult = null;
        if (sql.toLowerCase().startsWith("select")) {
            fesqlResult = selectRequestModeWithPreparedStatement(executor, dbName, need_insert_request_row, sql, input);
        } else {
            logger.error("unsupport sql: {}", sql);
        }
        return fesqlResult;
    }

    public static FesqlResult sqlBatchRequestMode(SqlExecutor executor, String dbName,
                                                  String sql, InputDesc input,
                                                  List<Integer> commonColumnIndices) throws SQLException {
        FesqlResult fesqlResult = null;
        if (sql.toLowerCase().startsWith("select")) {
            fesqlResult = selectBatchRequestModeWithPreparedStatement(
                    executor, dbName, sql, input, commonColumnIndices);
        } else {
            logger.error("unsupport sql: {}", sql);
        }
        return fesqlResult;
    }

    public static FesqlResult sqlRequestModeWithSp(SqlExecutor executor, String dbName, String spName,
                                                   Boolean needInsertRequestRow, String sql,
                                                   InputDesc rows, boolean isAsyn) throws SQLException {
        FesqlResult fesqlResult = null;
        if (sql.toLowerCase().startsWith("create procedure")) {
            fesqlResult = selectRequestModeWithSp(executor, dbName, spName, needInsertRequestRow, sql, rows, isAsyn);
        } else {
            logger.error("unsupport sql: {}", sql);
        }
        return fesqlResult;
    }

    public static FesqlResult sql(SqlExecutor executor, String dbName, String sql) {
        FesqlResult fesqlResult = null;
        if (sql.startsWith("create database") || sql.startsWith("drop database")) {
            fesqlResult = db(executor, sql);
        }else if (sql.startsWith("create") || sql.startsWith("CREATE") || sql.startsWith("DROP")|| sql.startsWith("drop")) {
            fesqlResult = ddl(executor, dbName, sql);
        } else if (sql.startsWith("insert")||sql.startsWith("INSERT")) {
            fesqlResult = insert(executor, dbName, sql);
        } else {
            fesqlResult = select(executor, dbName, sql);
        }
        return fesqlResult;
    }

    public static FesqlResult insert(SqlExecutor executor, String dbName, String insertSql) {
        if (insertSql.isEmpty()) {
            return null;
        }
        logger.info("insert sql:{}", insertSql);
        FesqlResult fesqlResult = new FesqlResult();
        boolean createOk = executor.executeInsert(dbName, insertSql);
        fesqlResult.setOk(createOk);
        logger.info("insert result:{}" + fesqlResult);
        return fesqlResult;
    }

    public static FesqlResult selectWithPrepareStatement(SqlExecutor executor, String dbName, String sql,List<String> paramterTypes,List<Object> params) {
        FesqlResult fesqlResult = new FesqlResult();
        try {
            if (sql.isEmpty()) {
                return null;
            }
            logger.info("prepare sql:{}", sql);
            PreparedStatement preparedStmt = executor.getPreparedStatement(dbName, sql);
            setPreparedData(preparedStmt,paramterTypes,params);
            ResultSet resultSet = preparedStmt.executeQuery();

            if (resultSet == null) {
                fesqlResult.setOk(false);
                fesqlResult.setMsg("executeSQL fail, result is null");
            } else if  (resultSet instanceof SQLResultSet){
                try {
                    SQLResultSet rs = (SQLResultSet)resultSet;
                    JDBCUtil.setSchema(rs.getMetaData(),fesqlResult);
                    fesqlResult.setOk(true);
                    List<List<Object>> result = convertRestultSetToList(rs);
                    fesqlResult.setCount(result.size());
                    fesqlResult.setResult(result);
                } catch (Exception e) {
                    fesqlResult.setOk(false);
                    fesqlResult.setMsg(e.getMessage());
                }
            }
            logger.info("insert result:{}" + fesqlResult);
        }catch (Exception e){
            e.printStackTrace();
            fesqlResult.setOk(false);
            fesqlResult.setMsg(e.getMessage());
        }
        return fesqlResult;
    }

    public static FesqlResult insertWithPrepareStatement(SqlExecutor executor, String dbName, String insertSql,List<Object> params) {
        FesqlResult fesqlResult = new FesqlResult();
        try {
            if (insertSql.isEmpty()) {
                return null;
            }
            logger.info("prepare sql:{}", insertSql);
            PreparedStatement preparedStmt = executor.getInsertPreparedStmt(dbName, insertSql);
            setRequestData(preparedStmt,params);
            // for(int i=0;i<types.size();i++){
                // preparedStatementSetValue(preparedStmt,i+1,types.get(i).split("\\s+")[1],params.get(i));
            // }
            boolean createOk =preparedStmt.execute();
            fesqlResult.setOk(createOk);
            logger.info("insert result:{}" + fesqlResult);
        }catch (Exception e){
            e.printStackTrace();
            fesqlResult.setOk(false);
            fesqlResult.setMsg(e.getMessage());
        }
        return fesqlResult;
    }

    public static FesqlResult db(SqlExecutor executor, String ddlSql) {
        if (ddlSql.isEmpty()) {
            return null;
        }
        logger.info("db sql:{}", ddlSql);
        String dbName = ddlSql.substring(0,ddlSql.lastIndexOf(";")).split("\\s+")[2];
        boolean db = false;
        if(ddlSql.startsWith("create database")){
            db = executor.createDB(dbName);
        }else{
            db = executor.dropDB(dbName);
        }
        FesqlResult fesqlResult = new FesqlResult();
        fesqlResult.setOk(db);
        logger.info("db result:{}", fesqlResult);
        return fesqlResult;
    }

    public static FesqlResult ddl(SqlExecutor executor, String dbName, String ddlSql) {
        if (ddlSql.isEmpty()) {
            return null;
        }
        logger.info("ddl sql:{}", ddlSql);
        FesqlResult fesqlResult = new FesqlResult();
        boolean createOk = executor.executeDDL(dbName, ddlSql);
        fesqlResult.setOk(createOk);
        logger.info("ddl result:{}", fesqlResult);
        return fesqlResult;
    }


    private static List<List<Object>> convertRestultSetToList(SQLResultSet rs) throws SQLException {
        List<List<Object>> result = new ArrayList<>();
        while (rs.next()) {
            List list = new ArrayList();
            int columnCount = rs.getMetaData().getColumnCount();
            for (int i = 0; i < columnCount; i++) {
                list.add(getColumnData(rs, i));
            }
            result.add(list);
        }
        return result;
    }

    private static FesqlResult selectRequestModeWithPreparedStatement(SqlExecutor executor, String dbName,
                                                                      Boolean need_insert_request_row,
                                                                      String selectSql, InputDesc input) {
        if (selectSql.isEmpty()) {
            logger.error("fail to execute sql in request mode: select sql is empty");
            return null;
        }

        List<List<Object>> rows = null == input ? null : input.getRows();
        if (CollectionUtils.isEmpty(rows)) {
            logger.error("fail to execute sql in request mode: request rows is null or empty");
            return null;
        }
        List<String> inserts = input.extractInserts();
        if (CollectionUtils.isEmpty(inserts)) {
            logger.error("fail to execute sql in request mode: fail to build insert sql for request rows");
            return null;
        }

        if (rows.size() != inserts.size()) {
            logger.error("fail to execute sql in request mode: rows size isn't match with inserts size");
            return null;
        }

        String insertDbName= input.getDb().isEmpty() ? dbName : input.getDb();
        logger.info("select sql:{}", selectSql);
        FesqlResult fesqlResult = new FesqlResult();
        List<List<Object>> result = Lists.newArrayList();
        for (int i = 0; i < rows.size(); i++) {
            PreparedStatement rps = null;
            try {
                rps = executor.getRequestPreparedStmt(dbName, selectSql);
            } catch (SQLException throwables) {
                fesqlResult.setOk(false);
                fesqlResult.setMsg("Get Request PreparedStatement Fail");
                return fesqlResult;
            }
            ResultSet resultSet = null;
            try {
                resultSet = buildRequestPreparedStatment(rps, rows.get(i));

            } catch (SQLException throwables) {
                fesqlResult.setOk(false);
                fesqlResult.setMsg("Build Request PreparedStatement Fail");
                return fesqlResult;
            }
            if (resultSet == null) {
                fesqlResult.setOk(false);
                fesqlResult.setMsg("Select result is null");
                logger.error("select result:{}", fesqlResult);
                return fesqlResult;
            }
            try {
                result.addAll(convertRestultSetToList((SQLResultSet) resultSet));
            } catch (SQLException throwables) {
                fesqlResult.setOk(false);
                fesqlResult.setMsg("Convert Result Set To List Fail");
                return fesqlResult;
            }
            if (need_insert_request_row && !executor.executeInsert(insertDbName, inserts.get(i))) {
                fesqlResult.setOk(false);
                fesqlResult.setMsg("Fail to execute sql in request mode fail to insert request row after query");
                logger.error(fesqlResult.getMsg());
                return fesqlResult;
            }
            if (i == rows.size()-1) {
                try {
                    JDBCUtil.setSchema(resultSet.getMetaData(),fesqlResult);
                } catch (SQLException throwables) {
                    fesqlResult.setOk(false);
                    fesqlResult.setMsg("Fail to set meta data");
                    return fesqlResult;
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
        fesqlResult.setResult(result);
        fesqlResult.setCount(result.size());
        fesqlResult.setOk(true);

        logger.info("select result:{}", fesqlResult);
        return fesqlResult;
    }

    private static FesqlResult selectBatchRequestModeWithPreparedStatement(SqlExecutor executor, String dbName,
                                                                           String selectSql, InputDesc input,
                                                                           List<Integer> commonColumnIndices) {
        if (selectSql.isEmpty()) {
            logger.error("fail to execute sql in batch request mode: select sql is empty");
            return null;
        }
        List<List<Object>> rows = null == input ? null : input.getRows();
        if (CollectionUtils.isEmpty(rows)) {
            logger.error("fail to execute sql in batch request mode: request rows is null or empty");
            return null;
        }
        List<String> inserts = input.extractInserts();
        if (CollectionUtils.isEmpty(inserts)) {
            logger.error("fail to execute sql in batch request mode: fail to build insert sql for request rows");
            return null;
        }
        if (rows.size() != inserts.size()) {
            logger.error("fail to execute sql in batch request mode: rows size isn't match with inserts size");
            return null;
        }
        logger.info("select sql:{}", selectSql);
        FesqlResult fesqlResult = new FesqlResult();

        PreparedStatement rps = null;
        SQLResultSet sqlResultSet = null;
        try {
            rps = executor.getBatchRequestPreparedStmt(dbName, selectSql, commonColumnIndices);

            for (List<Object> row : rows) {
                boolean ok = setRequestData(rps, row);
                if (ok) {
                    rps.addBatch();
                }
            }

            sqlResultSet = (SQLResultSet) rps.executeQuery();
            List<List<Object>> result = Lists.newArrayList();
            result.addAll(convertRestultSetToList(sqlResultSet));
            fesqlResult.setResult(result);
            JDBCUtil.setSchema(sqlResultSet.getMetaData(),fesqlResult);
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
        logger.info("select result:{}", fesqlResult);
        return fesqlResult;
    }

    private static FesqlResult selectRequestModeWithSp(SqlExecutor executor, String dbName, String spName,
                                                       Boolean needInsertRequestRow,
                                                       String sql, InputDesc input, boolean isAsyn) {
        if (sql.isEmpty()) {
            logger.error("fail to execute sql in request mode: select sql is empty");
            return null;
        }

        List<List<Object>> rows = null == input ? null : input.getRows();
        if (CollectionUtils.isEmpty(rows)) {
            logger.error("fail to execute sql in request mode: request rows is null or empty");
            return null;
        }
        List<String> inserts = needInsertRequestRow ? input.extractInserts() : Lists.newArrayList();
        if (needInsertRequestRow){
            if (CollectionUtils.isEmpty(inserts)) {
                logger.error("fail to execute sql in request mode: fail to build insert sql for request rows");
                return null;
            }
            if (rows.size() != inserts.size()) {
                logger.error("fail to execute sql in request mode: rows size isn't match with inserts size");
                return null;
            }
        }

        logger.info("procedure sql:{}", sql);
        String insertDbName = input.getDb().isEmpty() ? dbName : input.getDb();
        FesqlResult fesqlResult = new FesqlResult();
        if (!executor.executeDDL(dbName, sql)) {
            logger.error("execute ddl failed! sql: {}", sql);
            fesqlResult.setOk(false);
            fesqlResult.setMsg("execute ddl failed");
            return fesqlResult;
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
                    fesqlResult.setOk(false);
                    fesqlResult.setMsg("Fail to getCallablePreparedStmt");
                    return fesqlResult;
                }
                if (!isAsyn) {
                    resultSet = buildRequestPreparedStatment(rps, rows.get(i));
                } else {
                    resultSet = buildRequestPreparedStatmentAsync(rps, rows.get(i));
                }
                if (resultSet == null) {
                    fesqlResult.setOk(false);
                    fesqlResult.setMsg("result set is null");
                    logger.error("select result:{}", fesqlResult);
                    return fesqlResult;
                }
                result.addAll(convertRestultSetToList((SQLResultSet) resultSet));
                if (needInsertRequestRow && !executor.executeInsert(insertDbName, inserts.get(i))) {
                    fesqlResult.setOk(false);
                    fesqlResult.setMsg("fail to execute sql in request mode: fail to insert request row after query");
                    logger.error(fesqlResult.getMsg());
                    return fesqlResult;
                }
                if (i == 0) {
                    try {
                        JDBCUtil.setSchema(resultSet.getMetaData(),fesqlResult);
                    } catch (SQLException throwables) {
                        fesqlResult.setOk(false);
                        fesqlResult.setMsg("fail to get/set meta data");
                        return fesqlResult;
                    }
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
                logger.error("has exception. sql: {}", sql);
                fesqlResult.setOk(false);
                fesqlResult.setMsg("fail to execute sql");
                return fesqlResult;
            } finally {
                try {
                    if (resultSet != null) resultSet.close();
                    if (rps != null) rps.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }
        fesqlResult.setResult(result);
        fesqlResult.setCount(result.size());
        fesqlResult.setOk(true);
        logger.info("select result:{}", fesqlResult);
        return fesqlResult;
    }

    public static FesqlResult selectBatchRequestModeWithSp(SqlExecutor executor, String dbName, String spName,
                                                           String sql, InputDesc input, boolean isAsyn) {
        if (sql.isEmpty()) {
            logger.error("fail to execute sql in batch request mode: select sql is empty");
            return null;
        }
        List<List<Object>> rows = null == input ? null : input.getRows();
        if (CollectionUtils.isEmpty(rows)) {
            logger.error("fail to execute sql in batch request mode: request rows is null or empty");
            return null;
        }
        logger.info("procedure sql: {}", sql);
        FesqlResult fesqlResult = new FesqlResult();
        if (!executor.executeDDL(dbName, sql)) {
            fesqlResult.setOk(false);
            fesqlResult.setMsg("fail to execute ddl");
            return fesqlResult;
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
                fesqlResult.setOk(false);
                fesqlResult.setMsg("fail to getCallablePreparedStmtBatch");
                return fesqlResult;
            }
            for (List<Object> row : rows) {
                boolean ok = setRequestData(rps, row);
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
            result.addAll(convertRestultSetToList((SQLResultSet) sqlResultSet));
            fesqlResult.setResult(result);
            JDBCUtil.setSchema(sqlResultSet.getMetaData(),fesqlResult);
            fesqlResult.setCount(result.size());

        } catch (SQLException e) {
            logger.error("Call procedure failed", e);
            fesqlResult.setOk(false);
            fesqlResult.setMsg("Call procedure failed");
            return fesqlResult;
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
        logger.info("select result:{}", fesqlResult);
        return fesqlResult;
    }

    public static List<List<Object>> convertRows(List<List<Object>> rows, List<String> columns) throws ParseException {
        List<List<Object>> list = new ArrayList<>();
        for (List row : rows) {
            list.add(convertList(row, columns));
        }
        return list;
    }

    public static List<Object> convertList(List<Object> datas, List<String> columns) throws ParseException {
        List<Object> list = new ArrayList();
        for (int i = 0; i < datas.size(); i++) {
            if (datas.get(i) == null) {
                list.add(null);
            } else {
                String obj = datas.get(i).toString();
                String column = columns.get(i);
                list.add(convertData(obj, column));
            }
        }
        return list;
    }

    public static Object convertData(String data, String column) throws ParseException {
        String[] ss = column.split("\\s+");
        String type = ss[ss.length - 1];
        Object obj = null;
        if(data == null){
            return null;
        }
        if ("null".equalsIgnoreCase(data)) {
            return "null";
        }
        switch (type) {
            case "smallint":
            case "int16":
                obj = Short.parseShort(data);
                break;
            case "int32":
            case "i32":
            case "int":
                obj = Integer.parseInt(data);
                break;
            case "int64":
            case "bigint":
                obj = Long.parseLong(data);
                break;
            case "float": {
                if (data.equalsIgnoreCase("nan")||data.equalsIgnoreCase("-nan")) {
                    obj = Float.NaN;
                }else if(data.equalsIgnoreCase("inf")){
                    obj = Float.POSITIVE_INFINITY;
                }else if(data.equalsIgnoreCase("-inf")){
                    obj = Float.NEGATIVE_INFINITY;
                }else {
                    obj = Float.parseFloat(data);
                }
                break;
            }
            case "double": {
                if (data.equalsIgnoreCase("nan")||data.equalsIgnoreCase("-nan")) {
                    obj = Double.NaN;
                }else if(data.equalsIgnoreCase("inf")){
                    obj = Double.POSITIVE_INFINITY;
                }else if(data.equalsIgnoreCase("-inf")){
                    obj = Double.NEGATIVE_INFINITY;
                }else {
                    obj = Double.parseDouble(data);
                }
                break;
            }
            case "bool":
                obj = Boolean.parseBoolean(data);
                break;
            case "string":
                obj = data;
                break;
            case "timestamp":
                obj = new Timestamp(Long.parseLong(data));
                break;
            case "date":
                try {
                    obj = new Date(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(data.trim() + " 00:00:00").getTime());
                } catch (ParseException e) {
                    log.error("Fail convert {} to date", data.trim());
                    throw e;
                }
                break;
            default:
                obj = data;
                break;
        }
        return obj;
    }

    private static boolean buildRequestRow(SQLRequestRow requestRow, List<Object> objects) {
        Schema schema = requestRow.GetSchema();
        int totalSize = 0;
        for (int i = 0; i < schema.GetColumnCnt(); i++) {
            if (null == objects.get(i)) {
                continue;
            }
            if (DataType.kTypeString.equals(schema.GetColumnType(i))) {
                totalSize += objects.get(i).toString().length();
            }
        }

        logger.info("init request row: {}", totalSize);
        requestRow.Init(totalSize);
        for (int i = 0; i < schema.GetColumnCnt(); i++) {
            Object obj = objects.get(i);
            if (null == obj) {
                requestRow.AppendNULL();
                continue;
            }

            DataType dataType = schema.GetColumnType(i);
            if (DataType.kTypeInt16.equals(dataType)) {
                requestRow.AppendInt16(Short.parseShort(obj.toString()));
            } else if (DataType.kTypeInt32.equals(dataType)) {
                requestRow.AppendInt32(Integer.parseInt(obj.toString()));
            } else if (DataType.kTypeInt64.equals(dataType)) {
                requestRow.AppendInt64(Long.parseLong(obj.toString()));
            } else if (DataType.kTypeFloat.equals(dataType)) {
                requestRow.AppendFloat(Float.parseFloat(obj.toString()));
            } else if (DataType.kTypeDouble.equals(dataType)) {
                requestRow.AppendDouble(Double.parseDouble(obj.toString()));
            } else if (DataType.kTypeTimestamp.equals(dataType)) {
                requestRow.AppendTimestamp(Long.parseLong(obj.toString()));
            } else if (DataType.kTypeDate.equals(dataType)) {
                try {
                    Date date = new Date(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(obj.toString() + " 00:00:00").getTime());
                    logger.info("build request row: obj: {}, append date: {},  {}, {}, {}",
                            obj, date.toString(), date.getYear() + 1900, date.getMonth() + 1, date.getDate());
                    requestRow.AppendDate(date.getYear() + 1900, date.getMonth() + 1, date.getDate());
                } catch (ParseException e) {
                    logger.error("Fail convert {} to date", obj.toString());
                    return false;
                }
            } else if (DataType.kTypeString.equals(schema.GetColumnType(i))) {
                requestRow.AppendString(obj.toString());
            } else {
                logger.error("fail to build request row: invalid data type {]", schema.GetColumnType(i));
                return false;
            }
        }
        return requestRow.Build();
    }
    private static boolean setPreparedData(PreparedStatement ps,List<String> paramterType, List<Object> objects) throws SQLException {
        for(int i=0;i<objects.size();i++){
            String type = paramterType.get(i);
            Object value = objects.get(i);
            switch (type){
                case "varchar":
                case "string":
                    ps.setString(i+1,String.valueOf(value));
                    break;
                case "bool":
                    ps.setBoolean(i+1,Boolean.parseBoolean(String.valueOf(value)));
                    break;
                case "int16":
                case "smallint":
                    ps.setShort(i+1,Short.parseShort(String.valueOf(value)));
                    break;
                case "int":
                    ps.setInt(i+1,Integer.parseInt(String.valueOf(value)));
                    break;
                case "int64":
                case "long":
                case "bigint":
                    ps.setLong(i+1,Long.parseLong(String.valueOf(value)));
                    break;
                case "float":
                    ps.setFloat(i+1,Float.parseFloat(String.valueOf(value)));
                    break;
                case "double":
                    ps.setDouble(i+1,Double.parseDouble(String.valueOf(value)));
                    break;
                case "timestamp":
                    ps.setTimestamp(i+1,new Timestamp(Long.parseLong(String.valueOf(value))));
                    break;
                case "date":
                    try {
                        Date date = new Date(new SimpleDateFormat("yyyy-MM-dd").parse(String.valueOf(value)).getTime());
                        ps.setDate(i + 1, date);
                        break;
                    }catch (ParseException e){
                        e.printStackTrace();
                        return false;
                    }
                default:
                    throw new IllegalArgumentException("type not match");
            }
        }
        return true;
    }
    private static boolean setRequestData(PreparedStatement requestPs, List<Object> objects) throws SQLException {
        ResultSetMetaData metaData = requestPs.getMetaData();
        int totalSize = 0;
        for (int i = 0; i < metaData.getColumnCount(); i++) {
            if (null == objects.get(i)) {
                continue;
            }
            if (metaData.getColumnType(i + 1) == Types.VARCHAR) {
                totalSize += objects.get(i).toString().length();
            }
        }
        logger.info("init request row: {}", totalSize);
        for (int i = 0; i < metaData.getColumnCount(); i++) {
            Object obj = objects.get(i);
            if (null == obj || obj.toString().equalsIgnoreCase("null")) {
                requestPs.setNull(i + 1, 0);
                continue;
            }
            int columnType = metaData.getColumnType(i + 1);
            if (columnType == Types.BOOLEAN) {
                requestPs.setBoolean(i + 1, Boolean.parseBoolean(obj.toString()));
            } else if (columnType == Types.SMALLINT) {
                requestPs.setShort(i + 1, Short.parseShort(obj.toString()));
            } else if (columnType == Types.INTEGER) {
                requestPs.setInt(i + 1, Integer.parseInt(obj.toString()));
            } else if (columnType == Types.BIGINT) {
                requestPs.setLong(i + 1, Long.parseLong(obj.toString()));
            } else if (columnType == Types.FLOAT) {
                requestPs.setFloat(i + 1, Float.parseFloat(obj.toString()));
            } else if (columnType == Types.DOUBLE) {
                requestPs.setDouble(i + 1, Double.parseDouble(obj.toString()));
            } else if (columnType == Types.TIMESTAMP) {
                requestPs.setTimestamp(i + 1, new Timestamp(Long.parseLong(obj.toString())));
            } else if (columnType == Types.DATE) {
                if (obj instanceof java.util.Date) {
                    requestPs.setDate(i + 1, new Date(((java.util.Date) obj).getTime()));
                } else if (obj instanceof Date) {
                    requestPs.setDate(i + 1, (Date) (obj));
                } else if (obj instanceof DateTime) {
                    requestPs.setDate(i + 1, new Date(((DateTime) obj).getMillis()));
                } else {
                    try {
                        Date date = new Date(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(obj.toString() + " 00:00:00").getTime());
                        logger.info("build request row: obj: {}, append date: {},  {}, {}, {}",obj, date.toString(), date.getYear() + 1900, date.getMonth() + 1, date.getDate());
                        requestPs.setDate(i + 1, date);
                    } catch (ParseException e) {
                        logger.error("Fail convert {} to date: {}", obj, e);
                        return false;
                    }
                }
            } else if (columnType == Types.VARCHAR) {
                requestPs.setString(i + 1, obj.toString());
            } else {
                logger.error("fail to build request row: invalid data type {]", columnType);
                return false;
            }
        }
        return true;
    }

    private static ResultSet buildRequestPreparedStatment(PreparedStatement requestPs,
                                                                   List<Object> objects) throws SQLException {
        boolean success = setRequestData(requestPs, objects);
        if (success) {
            return requestPs.executeQuery();
        } else {
            return null;
        }
    }

    private static ResultSet buildRequestPreparedStatmentAsync(CallablePreparedStatement requestPs,
                                                               List<Object> objects) throws SQLException {
        boolean success = setRequestData(requestPs, objects);
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

    public static FesqlResult select(SqlExecutor executor, String dbName, String selectSql) {
        if (selectSql.isEmpty()) {
            return null;
        }
        logger.info("select sql:{}", selectSql);
        FesqlResult fesqlResult = new FesqlResult();
        ResultSet rawRs = executor.executeSQL(dbName, selectSql);
        if (rawRs == null) {
            fesqlResult.setOk(false);
            fesqlResult.setMsg("executeSQL fail, result is null");
        } else if  (rawRs instanceof SQLResultSet){
            try {
                SQLResultSet rs = (SQLResultSet)rawRs;
                JDBCUtil.setSchema(rs.getMetaData(),fesqlResult);
                fesqlResult.setOk(true);
                List<List<Object>> result = convertRestultSetToList(rs);
                fesqlResult.setCount(result.size());
                fesqlResult.setResult(result);
            } catch (Exception e) {
                fesqlResult.setOk(false);
                fesqlResult.setMsg(e.getMessage());
            }
        }
        logger.info("select result:{} \n", fesqlResult);
        return fesqlResult;
    }

    // public static Object getColumnData(com._4paradigm.openmldb.ResultSet rs, Schema schema, int index) {
    //     Object obj = null;
    //     DataType dataType = schema.GetColumnType(index);
    //     if (rs.IsNULL(index)) {
    //         logger.info("rs is null");
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
    //         logger.info("conver string data {}", obj);
    //     } else if (dataType.equals(DataType.kTypeTimestamp)) {
    //         obj = new Timestamp(rs.GetTimeUnsafe(index));
    //     }
    //     return obj;
    // }

    public static Object getColumnData(SQLResultSet rs, int index) throws SQLException {
        Object obj = null;
        int columnType = rs.getMetaData().getColumnType(index + 1);
        if (rs.getNString(index + 1) == null) {
            logger.info("rs is null");
            return null;
        }
        if (columnType == Types.BOOLEAN) {
            obj = rs.getBoolean(index + 1);
        } else if (columnType == Types.DATE) {
            try {
//                obj = new Date(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//                        .parse(rs.getNString(index + 1) + " 00:00:00").getTime());
                obj = rs.getDate(index + 1);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        } else if (columnType == Types.DOUBLE) {
            obj = rs.getDouble(index + 1);
        } else if (columnType == Types.FLOAT) {
            obj = rs.getFloat(index + 1);
        } else if (columnType == Types.SMALLINT) {
            obj = rs.getShort(index + 1);
        } else if (columnType == Types.INTEGER) {
            obj = rs.getInt(index + 1);
        } else if (columnType == Types.BIGINT) {
            obj = rs.getLong(index + 1);
        } else if (columnType == Types.VARCHAR) {
            obj = rs.getString(index + 1);
            logger.info("conver string data {}", obj);
        } else if (columnType == Types.TIMESTAMP) {
            obj = rs.getTimestamp(index + 1);
        }
        return obj;
    }

    public static String formatSql(String sql, List<String> tableNames, FEDBInfo fedbInfo) {
        Matcher matcher = pattern.matcher(sql);
        while (matcher.find()) {
            int index = Integer.parseInt(matcher.group(1));
            sql = sql.replace("{" + index + "}", tableNames.get(index));
        }
        sql = formatSql(sql,fedbInfo);
        return sql;
    }

    public static String formatSql(String sql, FEDBInfo fedbInfo) {
        if(sql.contains("{tb_endpoint_0}")){
            sql = sql.replace("{tb_endpoint_0}", fedbInfo.getTabletEndpoints().get(0));
        }
        if(sql.contains("{tb_endpoint_1}")){
            sql = sql.replace("{tb_endpoint_1}", fedbInfo.getTabletEndpoints().get(1));
        }
        if(sql.contains("{tb_endpoint_2}")){
            sql = sql.replace("{tb_endpoint_2}", fedbInfo.getTabletEndpoints().get(2));
        }
        return sql;
    }

    public static String formatSql(String sql, List<String> tableNames) {
        return formatSql(sql,tableNames, FedbGlobalVar.mainInfo);
    }

    // public static FesqlResult createAndInsert(SqlExecutor executor, String dbName,
    //                                           List<InputDesc> inputs,
    //                                           boolean useFirstInputAsRequests) {
    //     return createAndInsert(executor, dbName, inputs, useFirstInputAsRequests);
    // }
    public static FesqlResult createTable(SqlExecutor executor,String dbName,String createSql){
        if (StringUtils.isNotEmpty(createSql)) {
            FesqlResult res = FesqlUtil.ddl(executor, dbName, createSql);
            if (!res.isOk()) {
                logger.error("fail to create table");
                return res;
            }
            return res;
        }
        throw new IllegalArgumentException("create sql is null");
    }

    public static FesqlResult createAndInsert(SqlExecutor executor,
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

        FesqlResult fesqlResult = new FesqlResult();
        if (inputs != null && inputs.size() > 0) {
            for (int i = 0; i < inputs.size(); i++) {
                String tableName = inputs.get(i).getName();
                String createSql = inputs.get(i).extractCreate();
                if(StringUtils.isEmpty(createSql)){
                    continue;
                }
                createSql = SQLCase.formatSql(createSql, i, tableName);
                createSql = formatSql(createSql,FedbGlobalVar.mainInfo);
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
                        FesqlResult res = FesqlUtil.insert(executor, dbName, insertSql);
                        if (!res.isOk()) {
                            logger.error("fail to insert table");
                            return res;
                        }
                    }
                }
            }
        }
        fesqlResult.setOk(true);
        return fesqlResult;
    }

    public static FesqlResult createAndInsertWithPrepared(SqlExecutor executor,
                                              String defaultDBName,
                                              List<InputDesc> inputs,
                                              boolean useFirstInputAsRequests) {
        FesqlResult fesqlResult = new FesqlResult();
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
                    FesqlResult res = FesqlUtil.insertWithPrepareStatement(executor, dbName, insertSql, row);
                    if (!res.isOk()) {
                        logger.error("fail to insert table");
                        return res;
                    }
                }
            }
        }
        fesqlResult.setOk(true);
        return fesqlResult;
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
        logger.info("RESULT:\n{} row in set\n{}", rs.Size(), sb.toString());
    }
    public static String getColumnTypeByType(int type){
        switch (type){
            case Types.BIGINT: return "bigint";
            case Types.SMALLINT: return "smallint";
            case Types.INTEGER: return "int";
            case Types.VARCHAR: return "string";
            case Types.FLOAT: return "float";
            case Types.DOUBLE: return "double";
            case Types.DATE: return "date";
            case Types.TIMESTAMP: return "timestamp";
            case Types.BOOLEAN: return "bool";
        }
        throw new IllegalArgumentException("not know type");
    }
}
