package com._4paradigm.fesql_auto_test.util;

import com._4paradigm.fesql_auto_test.entity.FesqlCaseInput;
import com._4paradigm.fesql_auto_test.entity.FesqlResult;
import com._4paradigm.sql.DataType;
import com._4paradigm.sql.ResultSet;
import com._4paradigm.sql.Schema;
import com._4paradigm.sql.sdk.SqlExecutor;
import lombok.extern.slf4j.Slf4j;

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
    public static int getIndexByColumnName(Schema schema,String columnName){
        int count = schema.GetColumnCnt();
        for(int i=0;i<count;i++){
            if(schema.GetColumnName(i).equals(columnName)){
                return i;
            }
        }
        return -1;
    }
    public static String getColumnType(DataType dataType){
        if(dataType.equals(DataType.kTypeBool)){
            return "bool";
        }else if(dataType.equals(DataType.kTypeString)){
            return "string";
        }
        else if(dataType.equals(DataType.kTypeInt16)){
            return "smallint";
        }
        else if(dataType.equals(DataType.kTypeInt32)){
            return "int";
        }
        else if(dataType.equals(DataType.kTypeInt64)){
            return "bigint";
        }
        else if(dataType.equals(DataType.kTypeFloat)){
            return "float";
        }
        else if(dataType.equals(DataType.kTypeDouble)){
            return "double";
        }
        else if(dataType.equals(DataType.kTypeTimestamp)){
            return "timestamp";
        }
        else if(dataType.equals(DataType.kTypeDate)){
            return "date";
        }
        return null;
    }
    public static FesqlResult sqls(SqlExecutor executor,String dbName, List<String> sqls){
        FesqlResult fesqlResult = null;
        for(String sql:sqls){
            fesqlResult = sql(executor,dbName,sql);
        }
        return fesqlResult;
    }
    public static FesqlResult sql(SqlExecutor executor,String dbName, String sql){
        FesqlResult fesqlResult = null;
        if(sql.startsWith("create")){
            fesqlResult = ddl(executor,dbName,sql);
        }else if(sql.startsWith("insert")) {
            fesqlResult = insert(executor,dbName, sql);
        }else {
            fesqlResult = select(executor,dbName,sql);
        }
        return fesqlResult;
    }

    public static FesqlResult insert(SqlExecutor executor,String dbName,String insertSql){
        log.info("insert sql:{}",insertSql);
        FesqlResult fesqlResult = new FesqlResult();
        boolean createOk = executor.executeInsert(dbName,insertSql);
        fesqlResult.setOk(createOk);
        log.info("insert result:{}"+fesqlResult);
        return fesqlResult;
    }

    public static FesqlResult ddl(SqlExecutor executor,String dbName,String ddlSql){
        log.info("ddl sql:{}",ddlSql);
        FesqlResult fesqlResult = new FesqlResult();
        boolean createOk = executor.executeDDL(dbName, ddlSql);
        fesqlResult.setOk(createOk);
        log.info("ddl result:{}",fesqlResult);
        return fesqlResult;
    }

    public static FesqlResult select(SqlExecutor executor,String dbName,String selectSql){
        log.info("select sql:{}",selectSql);
        FesqlResult fesqlResult = new FesqlResult();
        ResultSet rs = executor.executeSQL(dbName,selectSql);
        if(rs==null){
            fesqlResult.setOk(false);
        }else {
            fesqlResult.setOk(true);
            fesqlResult.setCount(rs.Size());
            Schema schema = rs.GetSchema();
            fesqlResult.setResultSchema(schema);
            List<List> result = new ArrayList<>();
            while (rs.Next()) {
                List list = new ArrayList();
                int columnCount = schema.GetColumnCnt();
                for (int i = 0; i < columnCount; i++) {
                    list.add(getColumnData(rs, schema, i));
                }
                result.add(list);
            }
            fesqlResult.setResult(result);
        }
        log.info("select result:{}",fesqlResult);
        return fesqlResult;
    }
    public static Object getColumnData(ResultSet rs,Schema schema,int index){
        Object obj = null;
        DataType dataType = schema.GetColumnType(index);
        if(dataType.equals(DataType.kTypeBool)){
            obj = rs.GetBoolUnsafe(index);
        }else if(dataType.equals(DataType.kTypeDate)){
            obj = rs.GetAsString(index);
        }else if(dataType.equals(DataType.kTypeDouble)){
            obj = rs.GetDoubleUnsafe(index);
        }else if(dataType.equals(DataType.kTypeFloat)){
            obj = rs.GetFloatUnsafe(index);
        }else if(dataType.equals(DataType.kTypeInt16)){
            obj = rs.GetInt16Unsafe(index);
        }else if(dataType.equals(DataType.kTypeInt32)){
            obj = rs.GetInt32Unsafe(index);
        }else if(dataType.equals(DataType.kTypeInt64)){
            obj = rs.GetInt64Unsafe(index);
        }else if(dataType.equals(DataType.kTypeString)){
            obj = rs.GetStringUnsafe(index);
        }else if(dataType.equals(DataType.kTypeTimestamp)){
            obj = rs.GetTimeUnsafe(index);
        }
        return obj;
    }
    public static String formatSql(String sql,List<String> tableNames){
        Matcher matcher = pattern.matcher(sql);
        while (matcher.find()) {
            int index = Integer.parseInt(matcher.group(1));
            sql = sql.replace("{"+index+"}", tableNames.get(index));
        }
        return sql;
    }
    public static List<String> createAndInsert(SqlExecutor executor,String dbName,List<FesqlCaseInput> inputs){
        List<String> tableNames = new ArrayList<>();
        if(inputs!=null&&inputs.size()>0){
            for(FesqlCaseInput input:inputs){
                String tableName = input.getName();
                tableNames.add(tableName);
                //create table
                String createSql = input.getSql();
                if(createSql.contains("{ran}")){
                    createSql = createSql.replace("{ran}",tableName);
                }
                FesqlUtil.ddl(executor,dbName,createSql);
                // insert rows
                List<List> datas = input.getRows();
                if(datas!=null) {
                    for (List list : datas) {
                    StringBuilder builder = new StringBuilder("insert into "+tableName+" values(");
                    for(int i=0;i<list.size();i++){
                        String columnType = input.getColumnType(i);
                        Object data = list.get(i);
                        String dataStr = data+"";
                        if(columnType.equals("string")||columnType.equals("date")){
                            dataStr = "'"+data+"'";
                        }else if (columnType.equals("timestamp")){
                            dataStr = data+"L";
                        }
                        builder.append(dataStr+",");
                    }

                    String insertSql = builder.toString();
                    if(insertSql.endsWith(",")){
                        insertSql = insertSql.substring(0,insertSql.length()-1);
                    }
                    insertSql+=");";
//                        String insertSql = "insert into %s values(%s);";
//                        insertSql = String.format(insertSql, tableName, StringUtils.join(list, ","));
                        FesqlUtil.insert(executor,dbName, insertSql);
                    }
                }
            }
        }
        return tableNames;
    }
}
