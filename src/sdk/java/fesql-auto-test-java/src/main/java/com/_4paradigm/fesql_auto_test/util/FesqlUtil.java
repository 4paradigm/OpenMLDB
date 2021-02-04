package com._4paradigm.fesql_auto_test.util;

import com._4paradigm.fesql.sqlcase.model.InputDesc;
import com._4paradigm.fesql.sqlcase.model.SQLCase;
import com._4paradigm.fesql_auto_test.common.FesqlConfig;
import com._4paradigm.fesql_auto_test.entity.FesqlResult;
import com._4paradigm.sql.*;
import com._4paradigm.sql.ResultSet;
import com._4paradigm.sql.jdbc.CallablePreparedStatement;
import com._4paradigm.sql.jdbc.SQLResultSet;
import com._4paradigm.sql.sdk.SqlExecutor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.collections.Lists;

import java.sql.*;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
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
    private static final Logger logger = LoggerFactory.getLogger(FesqlUtil.class);

    public static int getIndexByColumnName(Schema schema, String columnName) {
        int count = schema.GetColumnCnt();
        for (int i = 0; i < count; i++) {
            if (schema.GetColumnName(i).equals(columnName)) {
                return i;
            }
        }
        return -1;
    }

    public static int getIndexByColumnName(ResultSetMetaData metaData, String columnName) {
        int count = 0;
        try {
            count = metaData.getColumnCount();
            for (int i = 0; i < count; i++) {
                if (metaData.getColumnName(i + 1).equals(columnName)) {
                    return i;
                }
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
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
        if (dataType == Types.BOOLEAN) {
            return "bool";
        } else if (dataType == Types.VARCHAR) {
            return "string";
        } else if (dataType == Types.SMALLINT) {
            return "smallint";
        } else if (dataType == Types.INTEGER) {
            return "int";
        } else if (dataType == Types.BIGINT) {
            return "bigint";
        } else if (dataType == Types.FLOAT) {
            return "float";
        } else if (dataType == Types.DOUBLE) {
            return "double";
        } else if (dataType == Types.TIMESTAMP) {
            return "timestamp";
        } else if (dataType == Types.DATE) {
            return "date";
        }
        return null;
    }

    public static FesqlResult sqls(SqlExecutor executor, String dbName, List<String> sqls) {
        FesqlResult fesqlResult = null;
        for (String sql : sqls) {
            fesqlResult = sql(executor, dbName, sql);
        }
        return fesqlResult;
    }

    public static FesqlResult sqlRequestMode(SqlExecutor executor, String dbName, String sql, InputDesc input) throws SQLException {
        FesqlResult fesqlResult = null;
        if (sql.toLowerCase().startsWith("select")) {
            fesqlResult = selectRequestModeWithPreparedStatement(executor, dbName, sql, input);
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

    public static FesqlResult sqlRequestModeWithSp(SqlExecutor executor, String dbName, String spName, String sql, InputDesc rows, boolean isAsyn) throws SQLException {
        FesqlResult fesqlResult = null;
        if (sql.toLowerCase().startsWith("create procedure")) {
            fesqlResult = selectRequestModeWithSp(executor, dbName, spName, sql, rows, isAsyn);
        } else {
            logger.error("unsupport sql: {}", sql);
        }
        return fesqlResult;
    }

    public static FesqlResult sql(SqlExecutor executor, String dbName, String sql) {
        FesqlResult fesqlResult = null;
        if (sql.startsWith("create")) {
            fesqlResult = ddl(executor, dbName, sql);
        } else if (sql.startsWith("insert")) {
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
        log.info("insert sql:{}", insertSql);
        FesqlResult fesqlResult = new FesqlResult();
        boolean createOk = executor.executeInsert(dbName, insertSql);
        fesqlResult.setOk(createOk);
        log.info("insert result:{}" + fesqlResult);
        return fesqlResult;
    }

    public static FesqlResult ddl(SqlExecutor executor, String dbName, String ddlSql) {
        if (ddlSql.isEmpty()) {
            return null;
        }
        log.info("ddl sql:{}", ddlSql);
        FesqlResult fesqlResult = new FesqlResult();
        boolean createOk = executor.executeDDL(dbName, ddlSql);
        fesqlResult.setOk(createOk);
        log.info("ddl result:{}", fesqlResult);
        return fesqlResult;
    }


    private static List<List<Object>> convertRestultSetToList(ResultSet rs, Schema schema) {
        List<List<Object>> result = new ArrayList<>();
        while (rs.Next()) {
            List list = new ArrayList();
            int columnCount = schema.GetColumnCnt();
            for (int i = 0; i < columnCount; i++) {
                list.add(getColumnData(rs, schema, i));
            }
            result.add(list);
        }
        return result;
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


//    private static FesqlResult selectRequestMode(SqlExecutor executor, String dbName,
//                                                 String selectSql, InputDesc input) {
//        if (selectSql.isEmpty()) {
//            logger.error("fail to execute sql in request mode: select sql is empty");
//            return null;
//        }
//
//        List<List<Object>> rows = null == input ? null : input.getRows();
//        if (CollectionUtils.isEmpty(rows)) {
//            logger.error("fail to execute sql in request mode: request rows is null or empty");
//            return null;
//        }
//        List<String> inserts = input.getInserts();
//        if (CollectionUtils.isEmpty(inserts)) {
//            logger.error("fail to execute sql in request mode: fail to build insert sql for request rows");
//            return null;
//        }
//
//        if (rows.size() != inserts.size()) {
//            logger.error("fail to execute sql in request mode: rows size isn't match with inserts size");
//            return null;
//        }
//
//        log.info("select sql:{}", selectSql);
//        FesqlResult fesqlResult = new FesqlResult();
//        SQLRequestRow requestRow = executor.getRequestRow(dbName, selectSql);
//        if (null == requestRow) {
//            fesqlResult.setOk(false);
//        } else {
//            List<List<Object>> result = Lists.newArrayList();
//            Schema schema = null;
//            for (int i = 0; i < rows.size(); i++) {
//                if (!buildRequestRow(requestRow, rows.get(i))) {
//                    log.info("fail to execute sql in request mode: fail to build request row");
//                    return null;
//                }
//                ResultSet rs = executor.executeSQL(dbName, selectSql, requestRow);
//                result.addAll(convertRestultSetToList(rs, rs.GetSchema()));
//                if (null == rs) {
//                    fesqlResult.setOk(false);
//                    log.info("select result:{}", fesqlResult);
//                    return fesqlResult;
//                }
//                if (i == 0) {
//                    schema = rs.GetSchema();
//                }
//                if (!executor.executeInsert(dbName, inserts.get(i))) {
//                    log.error("fail to execute sql in request mode: fail to insert request row after query");
//                    fesqlResult.setOk(false);
//                    return fesqlResult;
//                }
//            }
//            fesqlResult.setResult(result);
//            fesqlResult.setCount(result.size());
//            fesqlResult.setResultSchema(schema);
//            fesqlResult.setOk(true);
//        }
//        log.info("select result:{}", fesqlResult);
//        return fesqlResult;
//    }

    private static FesqlResult selectRequestModeWithPreparedStatement(SqlExecutor executor, String dbName,
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

        log.info("select sql:{}", selectSql);
        FesqlResult fesqlResult = new FesqlResult();
        List<List<Object>> result = Lists.newArrayList();
        for (int i = 0; i < rows.size(); i++) {
            PreparedStatement rps = null;
            try {
                rps = executor.getRequestPreparedStmt(dbName, selectSql);
            } catch (SQLException throwables) {
                fesqlResult.setOk(false);
                return fesqlResult;
            }
            java.sql.ResultSet resultSet = null;
            try {
                resultSet = buildRequestPreparedStatment(rps, rows.get(i));
            } catch (SQLException throwables) {
                fesqlResult.setOk(false);
                return fesqlResult;
            }
            if (resultSet == null) {
                fesqlResult.setOk(false);
                log.error("select result:{}", fesqlResult);
                return fesqlResult;
            }
            try {
                result.addAll(convertRestultSetToList((SQLResultSet) resultSet));
            } catch (SQLException throwables) {
                fesqlResult.setOk(false);
                return fesqlResult;
            }
            if (!executor.executeInsert(dbName, inserts.get(i))) {
                log.error("fail to execute sql in request mode: fail to insert request row after query");
                fesqlResult.setOk(false);
                return fesqlResult;
            }
            if (i == 0) {
                try {
                    fesqlResult.setMetaData(resultSet.getMetaData());
                } catch (SQLException throwables) {
                    fesqlResult.setOk(false);
                    return fesqlResult;
                }
            }
            try {
                rps.close();
                //resultSet.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
        fesqlResult.setResult(result);
        fesqlResult.setCount(result.size());
        fesqlResult.setOk(true);

        log.info("select result:{}", fesqlResult);
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
        log.info("select sql:{}", selectSql);
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
            fesqlResult.setMetaData(sqlResultSet.getMetaData());
            fesqlResult.setCount(result.size());
            fesqlResult.setResultSchema(sqlResultSet.GetInternalSchema());

        } catch (SQLException sqlException) {
            fesqlResult.setOk(false);
            sqlException.printStackTrace();
        } finally {
            try {
                if (rps != null) {
                    rps.close();
                }
                if (sqlResultSet != null) {
                    //sqlResultSet.close();
                }
            } catch (SQLException closeException) {
                closeException.printStackTrace();
            }
        }
        fesqlResult.setOk(true);
        log.info("select result:{}", fesqlResult);
        return fesqlResult;
    }

    private static FesqlResult selectRequestModeWithSp(SqlExecutor executor, String dbName, String spName,
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
        List<String> inserts = input.extractInserts();
        if (CollectionUtils.isEmpty(inserts)) {
            logger.error("fail to execute sql in request mode: fail to build insert sql for request rows");
            return null;
        }

        if (rows.size() != inserts.size()) {
            logger.error("fail to execute sql in request mode: rows size isn't match with inserts size");
            return null;
        }

        log.info("procedure sql:{}", sql);
        FesqlResult fesqlResult = new FesqlResult();
        if (!executor.executeDDL(dbName, sql)) {
            fesqlResult.setOk(false);
            return fesqlResult;
        }
        List<List<Object>> result = Lists.newArrayList();
        for (int i = 0; i < rows.size(); i++) {
            Object[] objects = new Object[rows.get(i).size()];
            for (int k = 0; k < objects.length; k++) {
                objects[k] = rows.get(i).get(k);
            }
            CallablePreparedStatement rps = null;
            java.sql.ResultSet resultSet = null;
            try {
                rps = executor.getCallablePreparedStmt(dbName, spName);
                if (rps == null) {
                    fesqlResult.setOk(false);
                    return fesqlResult;
                }
                if (!isAsyn) {
                    resultSet = buildRequestPreparedStatment(rps, rows.get(i));
                } else {
                    resultSet = buildRequestPreparedStatmentAsync(rps, rows.get(i));
                }
                if (resultSet == null) {
                    fesqlResult.setOk(false);
                    log.error("select result:{}", fesqlResult);
                    return fesqlResult;
                }
                result.addAll(convertRestultSetToList((SQLResultSet) resultSet));
                if (!executor.executeInsert(dbName, inserts.get(i))) {
                    log.error("fail to execute sql in request mode: fail to insert request row after query");
                    fesqlResult.setOk(false);
                    return fesqlResult;
                }
                if (i == 0) {
                    try {
                        fesqlResult.setMetaData(resultSet.getMetaData());
                    } catch (SQLException throwables) {
                        fesqlResult.setOk(false);
                        return fesqlResult;
                    }
                }
            } catch (SQLException throwables) {
                fesqlResult.setOk(false);
                return fesqlResult;
            } finally {
                try {
                    if (rps != null) rps.close();
                    //if (resultSet != null) resultSet.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }
        fesqlResult.setResult(result);
        fesqlResult.setCount(result.size());
        fesqlResult.setOk(true);
        log.info("select result:{}", fesqlResult);
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
        log.info("procedure sql: {}", sql);
        FesqlResult fesqlResult = new FesqlResult();
        if (!executor.executeDDL(dbName, sql)) {
            fesqlResult.setOk(false);
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
        java.sql.ResultSet sqlResultSet = null;
        try {
            rps = executor.getCallablePreparedStmtBatch(dbName, spName);
            if (rps == null) {
                fesqlResult.setOk(false);
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
                com._4paradigm.sql.sdk.QueryFuture future = rps.executeQueryAsync(10000, TimeUnit.MILLISECONDS);
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
            fesqlResult.setMetaData(sqlResultSet.getMetaData());
            fesqlResult.setCount(result.size());

        } catch (SQLException e) {
            log.error("Call procedure failed", e);
            fesqlResult.setOk(false);
            return fesqlResult;
        } finally {
            try {
                if (rps != null) {
                    rps.close();
                }
                if (sqlResultSet != null) {
                    //sqlResultSet.close();
                }
            } catch (SQLException closeException) {
                closeException.printStackTrace();
            }
        }
        fesqlResult.setOk(true);
        log.info("select result:{}", fesqlResult);
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
        if ("null".equalsIgnoreCase(data)) {
            return null;
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
                if (data.equalsIgnoreCase("nan")) {
                    obj = Float.NaN;
                } else {
                    obj = Float.parseFloat(data);
                }
                break;
            }
            case "double": {
                if (data.equalsIgnoreCase("nan")) {
                    obj = Double.NaN;
                } else {
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
                    logger.error("Fail convert {} to date", data.trim());
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
                    requestPs.setDate(i+1, new Date(((java.util.Date) obj).getTime()));
                } else if(obj instanceof Date) {
                    requestPs.setDate(i + 1, (Date)(obj));
                } else if (obj instanceof DateTime) {
                    requestPs.setDate(i+1, new Date(((DateTime) obj).getMillis()));
                } else {
                    try {
                        Date date = new Date(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(obj.toString() + " 00:00:00").getTime());
                        logger.info("build request row: obj: {}, append date: {},  {}, {}, {}",
                                obj, date.toString(), date.getYear() + 1900, date.getMonth() + 1, date.getDate());
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

    private static java.sql.ResultSet buildRequestPreparedStatment(PreparedStatement requestPs,
                                                                   List<Object> objects) throws SQLException {
        boolean success = setRequestData(requestPs, objects);
        if (success) {
            return requestPs.executeQuery();
        } else {
            return null;
        }
    }

    private static java.sql.ResultSet buildRequestPreparedStatmentAsync(CallablePreparedStatement requestPs,
                                                                   List<Object> objects) throws SQLException {
        boolean success = setRequestData(requestPs, objects);
        if (success) {
            com._4paradigm.sql.sdk.QueryFuture future = requestPs.executeQueryAsync(100, TimeUnit.MILLISECONDS);
            java.sql.ResultSet sqlResultSet = null;
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
        log.info("select sql:{}", selectSql);
        FesqlResult fesqlResult = new FesqlResult();
        ResultSet rs = executor.executeSQL(dbName, selectSql);
        if (rs == null) {
            fesqlResult.setOk(false);
        } else {
            fesqlResult.setOk(true);
            fesqlResult.setCount(rs.Size());
            Schema schema = rs.GetSchema();
            fesqlResult.setResultSchema(schema);
            List<List<Object>> result = convertRestultSetToList(rs, schema);
            fesqlResult.setResult(result);
        }
        log.info("select result:{} \nschema={}", fesqlResult, fesqlResult.getResultSchema());
        return fesqlResult;
    }

    public static Object getColumnData(ResultSet rs, Schema schema, int index) {
        Object obj = null;
        DataType dataType = schema.GetColumnType(index);
        if (rs.IsNULL(index)) {
            logger.info("rs is null");
            return null;
        }
        if (dataType.equals(DataType.kTypeBool)) {
            obj = rs.GetBoolUnsafe(index);
        } else if (dataType.equals(DataType.kTypeDate)) {
            try {
                obj = new Date(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                        .parse(rs.GetAsString(index) + " 00:00:00").getTime());
            } catch (ParseException e) {
                e.printStackTrace();
                return null;
            }
        } else if (dataType.equals(DataType.kTypeDouble)) {
            obj = rs.GetDoubleUnsafe(index);
        } else if (dataType.equals(DataType.kTypeFloat)) {
            obj = rs.GetFloatUnsafe(index);
        } else if (dataType.equals(DataType.kTypeInt16)) {
            obj = rs.GetInt16Unsafe(index);
        } else if (dataType.equals(DataType.kTypeInt32)) {
            obj = rs.GetInt32Unsafe(index);
        } else if (dataType.equals(DataType.kTypeInt64)) {
            obj = rs.GetInt64Unsafe(index);
        } else if (dataType.equals(DataType.kTypeString)) {
            obj = rs.GetStringUnsafe(index);
            logger.info("conver string data {}", obj);
        } else if (dataType.equals(DataType.kTypeTimestamp)) {
            obj = new Timestamp(rs.GetTimeUnsafe(index));
        }
        return obj;
    }

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

    public static String formatSql(String sql, List<String> tableNames) {
        Matcher matcher = pattern.matcher(sql);
        while (matcher.find()) {
            int index = Integer.parseInt(matcher.group(1));
            sql = sql.replace("{" + index + "}", tableNames.get(index));
        }
        if(sql.contains("{tb_endpoint_0}")){
            sql = sql.replace("{tb_endpoint_0}", FesqlConfig.TB_ENDPOINT_0);
        }
        if(sql.contains("{tb_endpoint_1}")){
            sql = sql.replace("{tb_endpoint_1}", FesqlConfig.TB_ENDPOINT_1);
        }
        if(sql.contains("{tb_endpoint_2}")){
            sql = sql.replace("{tb_endpoint_2}", FesqlConfig.TB_ENDPOINT_2);
        }
        return sql;
    }


    public static FesqlResult createAndInsert(SqlExecutor executor, String dbName,
                                              List<InputDesc> inputs,
                                              boolean useFirstInputAsRequests) {
        return createAndInsert(executor, dbName, inputs, useFirstInputAsRequests, 1);
    }

    public static FesqlResult createAndInsert(SqlExecutor executor,
                                              String dbName,
                                              List<InputDesc> inputs,
                                              boolean useFirstInputAsRequests,
                                              int replicaNum) {
        FesqlResult fesqlResult = new FesqlResult();
        if (inputs != null && inputs.size() > 0) {
            for (int i = 0; i < inputs.size(); i++) {
                String tableName = inputs.get(i).getName();
                //create table
                String createSql = inputs.get(i).extractCreate(replicaNum);
                createSql = SQLCase.formatSql(createSql, i, inputs.get(i).getName());
                if (!createSql.isEmpty()) {
                    FesqlResult res = FesqlUtil.ddl(executor, dbName, createSql);
                    if (!res.isOk()) {
                        logger.error("fail to create table");
                        return res;
                    }
                }
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

    public static void show(ResultSet rs) {
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
}
