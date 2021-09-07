package com._4paradigm.openmldb.java_sdk_test.util;

import com._4paradigm.openmldb.test_common.model.InputDesc;
import com._4paradigm.openmldb.test_common.model.Table;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * @author zhaowei
 * @date 2021/3/19 2:36 PM
 */
@Slf4j
public class ANSISQLUtil {
    public static List<String> getCreateIndexSql(InputDesc inputDesc){
        List<String> indexSqls = new ArrayList<>();
        List<String> indexs = inputDesc.getIndexs();
        String name = inputDesc.getName();
        if (CollectionUtils.isEmpty(indexs)) {
            return indexSqls;
        }
        for (String index : indexs) {
            String indexName = InputDesc.getIndexName(index);
            String indexColumn = InputDesc.getColumnName(index);
            //create index index1 on auto_dOIozCAS(c1)
            String index_temp = "CREATE INDEX %s ON %s(%s)";
            String indexSql = String.format(index_temp,indexName,name,indexColumn);
            indexSqls.add(indexSql);
        }
        return indexSqls;
    }
    public static String getCreateTableSql(InputDesc inputDesc, Function<String,String> convertType){
        if(StringUtils.isNotEmpty(inputDesc.getCreate())){
            return inputDesc.getCreate();
        }
        List<String> columns = inputDesc.getColumns();
        String name = inputDesc.getName();
        if (CollectionUtils.isEmpty(columns)) {
            return "";
        }
        String sql;
        StringBuilder builder = new StringBuilder("create table " + name + "(\n");
        for (int i = 0; i < columns.size(); i++) {
            if (0 < i) {
                builder.append("\n");
            }
            String column = columns.get(i);
            if(convertType!=null){
                String columnType = Table.getColumnType(column);
                String columnName = Table.getColumnName(column);
                columnType = convertType.apply(columnType);
                column = columnName+" "+columnType;
            }
            builder.append(column + ",");
        }
        sql = builder.toString();
        if (sql.endsWith(",")) {
            sql = sql.substring(0, sql.length() - 1);
        }
        sql+=")";
        return sql;
    }

    public static List<String> getInsertSqls(InputDesc inputDesc,BiFunction<String,String,String> convertData){
        String insert = inputDesc.getInsert();
        if (!StringUtils.isEmpty(insert)) {
            return Lists.newArrayList(insert);
        }
        List<String> inserts = inputDesc.getInserts();
        if (!CollectionUtils.isEmpty(inserts)) {
            return inserts;
        }
        List<String> insertSqls = Lists.newArrayList();
        List<String> columns = inputDesc.getColumns();
        List<List<Object>> datas = inputDesc.getRows();
        String name = inputDesc.getName();
        for (List<Object> row : datas) {
            List<List<Object>> rows = Lists.newArrayList();
            rows.add(row);
            insertSqls.add(getInsertSql(name, columns, rows,convertData));
        }
        return insertSqls;
    }

    public static String getInsertSql(String name,List<String> columns,List<List<Object>> datas,BiFunction<String,String,String> convertData){

        if (CollectionUtils.isEmpty(columns) || CollectionUtils.isEmpty(datas)) {
            return "";
        }
        // insert rows
        StringBuilder builder = new StringBuilder("insert into ").append(name).append(" values");
        for (int row_id = 0; row_id < datas.size(); row_id++) {
            List list = datas.get(row_id);
            builder.append("\n(");
            for (int i = 0; i < list.size(); i++) {
                String columnType = Table.getColumnType(columns.get(i));
                Object data = list.get(i);
                String dataStr = String.valueOf(data);
                if(dataStr.equals("{currentTime}")){
                    dataStr = String.valueOf(System.currentTimeMillis());
                }else if(dataStr.startsWith("{currentTime}-")){
                    long t = Long.parseLong(dataStr.substring(14));
                    dataStr = String.valueOf(System.currentTimeMillis()-t);
                }else if(dataStr.startsWith("{currentTime}+")){
                    long t = Long.parseLong(dataStr.substring(14));
                    dataStr = String.valueOf(System.currentTimeMillis()+t);
                }
                dataStr = convertData.apply(columnType,dataStr);
                // if (null != data && (columnType.equals("string") || columnType.equals("date"))) {
                //     dataStr = "'" + data.toString() + "'";
                // }
                // if(dataStr.equals("true")){
                //     dataStr = "1";
                // }else if(dataStr.equals("false")){
                //     dataStr = "0";
                // }
                builder.append(dataStr);
                if (i < list.size() - 1) {
                    builder.append(",");
                }
            }
            if (row_id < datas.size() - 1) {
                builder.append("),");
            } else {
                builder.append(");");
            }
        }
        return builder.toString();
    }
}
