package com._4paradigm.fesql_auto_test.util;

import com._4paradigm.fesql.sqlcase.model.InputDesc;
import com._4paradigm.fesql.sqlcase.model.SQLCase;
import com._4paradigm.fesql.sqlcase.model.Table;
import com._4paradigm.fesql_auto_test.entity.FesqlResult;
import com._4paradigm.sql.DataType;
import com._4paradigm.sql.ResultSet;
import com._4paradigm.sql.SQLRequestRow;
import com._4paradigm.sql.Schema;
import com._4paradigm.sql.sdk.SqlExecutor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.collections.Lists;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
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

    public static String getColumnType(DataType dataType) {
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

    public static FesqlResult sqls(SqlExecutor executor, String dbName, List<String> sqls) {
        FesqlResult fesqlResult = null;
        for (String sql : sqls) {
            fesqlResult = sql(executor, dbName, sql);
        }
        return fesqlResult;
    }

    public static FesqlResult sqlRequestMode(SqlExecutor executor, String dbName, String sql, InputDesc rows) {
        FesqlResult fesqlResult = null;
        if (sql.toLowerCase().startsWith("select")) {
            fesqlResult = selectRequestMode(executor, dbName, sql, rows);
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

    private static FesqlResult selectRequestMode(SqlExecutor executor, String dbName,
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
        List<String> inserts = input.getInserts();
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
        SQLRequestRow requestRow = executor.getRequestRow(dbName, selectSql);
        if (null == requestRow) {
            fesqlResult.setOk(false);
        } else {
            List<List<Object>> result = Lists.newArrayList();
            Schema schema = null;
            for (int i = 0; i < rows.size(); i++) {
                if (!buildRequestRow(requestRow, rows.get(i))) {
                    log.info("fail to execute sql in request mode: fail to build request row");
                    return null;
                }
                ResultSet rs = executor.executeSQL(dbName, selectSql, requestRow);
                result.addAll(convertRestultSetToList(rs, rs.GetSchema()));
                if (null == rs) {
                    fesqlResult.setOk(false);
                    log.info("select result:{}", fesqlResult);
                    return fesqlResult;
                }
                if (i == 0) {
                    schema = rs.GetSchema();
                }
                if (!executor.executeInsert(dbName, inserts.get(i))) {
                    log.error("fail to execute sql in request mode: fail to insert request row after query");
                    fesqlResult.setOk(false);
                    return fesqlResult;
                }
            }
            fesqlResult.setResult(result);
            fesqlResult.setCount(result.size());
            fesqlResult.setResultSchema(schema);
            fesqlResult.setOk(true);
        }
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
            case "float":
                obj = Float.parseFloat(data);
                break;
            case "double":
                obj = Double.parseDouble(data);
                break;
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
        log.info("select result:{}", fesqlResult);
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

    public static String formatSql(String sql, List<String> tableNames) {
        Matcher matcher = pattern.matcher(sql);
        while (matcher.find()) {
            int index = Integer.parseInt(matcher.group(1));
            sql = sql.replace("{" + index + "}", tableNames.get(index));
        }
        return sql;
    }

    public static FesqlResult createAndInsert(SqlExecutor executor, String
            dbName, List<InputDesc> inputs,
                                              boolean requestMode) {
        FesqlResult fesqlResult = new FesqlResult();
        if (inputs != null && inputs.size() > 0) {
            for (int i = 0; i < inputs.size(); i++) {
                String tableName = inputs.get(i).getName();
                //create table
                String createSql = inputs.get(i).getCreate();
                createSql = SQLCase.formatSql(createSql, i, inputs.get(i).getName());
                if (!createSql.isEmpty()) {
                    FesqlUtil.ddl(executor, dbName, createSql);
                }
                if (0 == i && requestMode) {
                    continue;
                }
                List<String> inserts = inputs.get(i).getInserts();
                for (String insertSql : inserts) {
                    insertSql = SQLCase.formatSql(insertSql, i, inputs.get(i).getName());
                    if (!insertSql.isEmpty()) {
                        FesqlResult res = FesqlUtil.insert(executor, dbName, insertSql);
                        if (!res.isOk()) {
                            logger.error("fail to insert table: {}", insertSql);
                            return res;
                        }
                    }
                }
            }
        }
        fesqlResult.setOk(true);
        return fesqlResult;
    }
}
