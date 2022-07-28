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

package com._4paradigm.openmldb.test_common.model;

import com._4paradigm.openmldb.test_common.util.DataUtil;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Data
@Slf4j
public class Table implements Serializable{
    private String name = genAutoName();
    private String db = "";
    private String index;
    private String schema;
    private String data;
    private List<String> indexs;
    private List<String> columns;
    private List<List<Object>> rows;
    private String create;
    private String insert;
    private List<String> inserts;
    private String repeat_tag = "";
    private int repeat = 1;
    private int replicaNum = 1;
    private int partitionNum = 1;
    private String storage;
    private List<OpenMLDBDistribution> distribution;

    private List<String> common_column_indices;

    private boolean drop = true;

    public static String genAutoName() {
        return "auto_" + RandomStringUtils.randomAlphabetic(8);
    }

    /**
     * 从输入构造Create SQL：
     * 如果create非空，直接返回create，否则需要根据columns/schema来构造Create SQL语句
     *
     * @return
     */
    public String extractCreate() {
        if (!StringUtils.isEmpty(create)) {
            return create;
        }
        return buildCreateSQLFromColumnsIndexs(name, getColumns(), getIndexs(), replicaNum,partitionNum,distribution,storage);
    }

    // public String extractCreate() {
    //     return extractCreate(1);
    // }
    public String getCreate() {
        return create;
    }

    /**
     * 从输入构造Insert SQL：
     * 如果insert非空，直接返回insert，否则需要根据columns和rows来构造Insert SQL语句
     *
     * @return
     */
    public String getInsert() {
        return insert;
    }
    public String extractInsert() {
        if (!StringUtils.isEmpty(insert)) {
            return insert;
        }
        return buildInsertSQLFromRows(name, getColumns(), getRows());
    }


    /**
     * 从输入构造Insert SQLS：
     * 如果insert非空，直接返回insert，否则需要根据columns和rows来构造Insert SQL语句
     *
     * @return
     */
    public List<String> getInserts() {
        return inserts;
    }
    public List<String> extractInserts() {
        if (!StringUtils.isEmpty(insert)) {
            return Lists.newArrayList(insert);
        }
        if (!CollectionUtils.isEmpty(inserts)) {
            return inserts;
        }
        List<String> inserts = Lists.newArrayList();
        for (List<Object> row : getRows()) {
            List<List<Object>> rows = Lists.newArrayList();
            rows.add(row);
            inserts.add(buildInsertSQLFromRows(name, getColumns(), rows));
        }
        return inserts;
    }

    public String getPreparedInsert() {
        if (!StringUtils.isEmpty(insert)) {
            return insert;
        }
        String insertSql = Table.buildInsertSQLWithPrepared(name,getColumns());
        return insertSql;
    }

    /**
     * 获取Indexs
     * 如果indexs非空，直接返回indexs，否则需要从index解析出indexs
     *
     * @return
     */
    public List<String> getIndexs() {
        if (!CollectionUtils.isEmpty(indexs)) {
            return indexs;
        }

        if (StringUtils.isEmpty(index)) {
            return Collections.emptyList();
        }

        List<String> parserd_indexs = new ArrayList<>();

        for (String index : index.trim().split(",|\n")) {
            parserd_indexs.add(index.trim());
        }
        return parserd_indexs;
    }

    /**
     * Return columns list.
     * if columns is empty,  convert schema to columns:
     * <code>col_name:col_type to columns</code>
     *
     * @return ["col_name col_type", ...]
     */
    public List<String> getColumns() {
        if (!CollectionUtils.isEmpty(columns)) {
            return columns;
        }

        if (StringUtils.isEmpty(schema)) {
            return Collections.emptyList();
        }

        List<String> parserd_columns = new ArrayList<>();

        for (String col : schema.trim().split(",")) {
            parserd_columns.add(col.trim().replaceAll(":", " "));
        }
        return parserd_columns;
    }

    public List<List<Object>> getRepeatRows(List<List<Object>> rows, int repeat) {
        if (repeat <= 1) {
            return rows;
        }
        List<List<Object>> repeatRows = Lists.newArrayList();
        for (int i = 0; i < repeat; i++) {
            repeatRows.addAll(rows);
        }
        return repeatRows;
    }

    /**
     * 获取Rows
     * 如果 rows 非空，直接返回 rows, 否则需要从 data 解析出 rows
     *
     * @return
     */
    public List<List<Object>> getRows() {
        if (!CollectionUtils.isEmpty(rows)) {
            return getRepeatRows(rows, repeat);
        }

        if (StringUtils.isEmpty(data)) {
            return Collections.emptyList();
        }

        List<List<Object>> parserd_rows = new ArrayList<>();

        for (String row : data.trim().split("\n")) {
            List<Object> each_row = new ArrayList<Object>();
            for (String item : row.trim().split(",")) {
                String data = item.trim();
                if(data.equalsIgnoreCase("null")){
                    data = null;
                }
                each_row.add(data);
            }
            parserd_rows.add(each_row);
        }

        return getRepeatRows(parserd_rows, repeat);
    }

    /**
     * extract indexName from index content
     *
     * @param index
     * @return
     */
    public static String getIndexName(String index) {
        String[] splits = index.trim().split(":");
        if (splits.length < 1) {
            return "";
        }
        return splits[0].trim();
    }

    public static boolean validateIndex(String index) {
        int length = index.trim().split(":").length;

        if (length < 1) {
            log.info("Index is invalid: empty index");
            return false;
        }

        if (length < 2) {
            log.info("Index is invalid: missing keys and ts, {}", index);
            return false;
        }

        if (length > 5) {
            log.info("Index is invalid: index items {} > 5", index);
            return false;
        }
        return true;
    }

    /**
     * extract indexKeys from index content
     *
     * @param index
     * @return
     */
    public static List<String> getIndexKeys(String index) {
        String[] splits = index.trim().split(":");
        if (splits.length < 2) {
            return Collections.emptyList();
        }
        List<String> keys = Lists.newArrayList();
        for (String split : splits[1].trim().split("\\|")) {
            keys.add(split.trim());
        }
        return keys;
    }

    /**
     * extract index tsCol from index content
     *
     * @param index
     * @return
     */
    public static String getIndexTsCol(String index) {
        String[] splits = index.trim().split(":");
        if (splits.length < 3) {
            return "";
        }
        return splits[2].trim();
    }

    /**
     * extract index tsCol from index content
     *
     * @param index
     * @return
     */
    public static String getIndexTTL(String index) {
        String[] splits = index.trim().split(":");
        if (splits.length < 4) {
            return "";
        }
        return splits[3].trim();
    }

    /**
     * extract index tsCol from index content
     *
     * @param index
     * @return
     */
    public static String getIndexTTLType(String index) {
        String[] splits = index.trim().split(":");
        if (splits.length < 5) {
            return "";
        }
        return splits[4].trim();
    }

    /**
     * extract columnName from column content
     *
     * @param column
     * @return
     */
    public static String getColumnName(String column) {
        int pos = column.trim().lastIndexOf(' ');
        return column.trim().substring(0, pos).trim();
    }

    /**
     * extract columnType string from column content
     *
     * @param column
     * @return
     */
    public static String getColumnType(String column) {
//        int pos = column.trim().lastIndexOf(' ');
//        return column.trim().substring(pos).trim();
        String[] ss = column.split("\\s+");
        return ss[1];
    }
    public static String getColumnTypeByExpect(String column) {
        int pos = column.trim().lastIndexOf(' ');
        return column.trim().substring(pos).trim();
    }

    /**
     * format columns and rows
     *
     * @param columns
     * @param rows
     * @return
     */
    public static String getTableString(List<String> columns, List<List<Object>> rows) {
        StringBuffer sb = new StringBuffer();
        sb.append(Joiner.on(",").useForNull("null(obj)").join(columns)).append("\n");
        for (List<Object> row : rows) {
            sb.append(Joiner.on(",").useForNull("null(obj)").join(row)).append("\n");
        }
        return sb.toString();
    }


    public static String buildInsertSQLFromRows(String name, List<String> columns, List<List<Object>> datas) {
        if (CollectionUtils.isEmpty(columns) || CollectionUtils.isEmpty(datas)) {
            return "";
        }
        // insert rows
        StringBuilder builder = new StringBuilder("insert into ").append(name).append(" values");
        for (int row_id = 0; row_id < datas.size(); row_id++) {
            List list = datas.get(row_id);
            builder.append("\n(");
            for (int i = 0; i < list.size(); i++) {
                String columnType = getColumnType(columns.get(i));
                Object data = list.get(i);
                String dataStr = null == data ? "null" : data.toString();
                if(dataStr.equals("{currentTime}")){
                    dataStr = String.valueOf(System.currentTimeMillis());
                }else if(dataStr.startsWith("{currentTime}-")){
                    long t = Long.parseLong(dataStr.substring(14));
                    dataStr = String.valueOf(System.currentTimeMillis()-t);
                }else if(dataStr.startsWith("{currentTime}+")){
                    long t = Long.parseLong(dataStr.substring(14));
                    dataStr = String.valueOf(System.currentTimeMillis()+t);
                }
                if (null != data && (columnType.equals("string") || columnType.equals("date"))) {
                    dataStr = "'" + data.toString() + "'";
                }
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

    public static String buildInsertSQLWithPrepared(String name, List<String> columns) {
        if (CollectionUtils.isEmpty(columns)) {
            return "";
        }
        // insert rows
        StringBuilder builder = new StringBuilder("insert into ").append(name).append(" values");
        builder.append("\n(");
        for (int i = 0; i < columns.size(); i++) {
            builder.append("?");
            if (i < columns.size() - 1) {
                builder.append(",");
            }
        }
        builder.append(");");
        return builder.toString();
    }

    public static String buildCreateSQLFromColumnsIndexs(String name, List<String> columns, List<String> indexs,
                                                         int replicaNum,int partitionNum,List<OpenMLDBDistribution> distribution,String storage) {
        if (CollectionUtils.isEmpty(columns)) {
            return "";
        }
        String sql;
        StringBuilder builder = new StringBuilder("create table " + name + "(\n");
        for (int i = 0; i < columns.size(); i++) {
            if (0 < i) {
                builder.append("\n");
            }
            builder.append(columns.get(i) + ",");
        }
        if(CollectionUtils.isNotEmpty(indexs)) {
            for (String index : indexs) {
                builder.append("\nindex(");
                if (!validateIndex(index)) {
                    return "";
                }

                builder.append("key=(").append(Joiner.on(",").join(getIndexKeys(index))).append(")");
                String tsIndex = getIndexTsCol(index);
                if (!tsIndex.isEmpty()) {
                    builder.append(",ts=").append(getIndexTsCol(index));
                }

                String ttl = getIndexTTL(index);
                if (!ttl.isEmpty()) {
                    builder.append(",ttl=").append(ttl);
                }

                String ttlType = getIndexTTLType(index);
                if (!ttlType.isEmpty()) {
                    builder.append(",ttl_type=").append(ttlType);
                }
                builder.append("),");
            }
        }
        sql = builder.toString();
        if (sql.endsWith(",")) {
            sql = sql.substring(0, sql.length() - 1);
        }
        sql += ")";
        StringBuilder distributionStr = new StringBuilder();
        // distribution = [ ('{tb_endpoint_0}',[]),('{tb_endpoint_1}',[])]
        // distribution = [ ('{tb_endpoint_1}', [ '{tb_endpoint_0}','{tb_endpoint_2}' ])]
        if(CollectionUtils.isNotEmpty(distribution)){
            distributionStr.append(",distribution = [");
            for(OpenMLDBDistribution d:distribution){
                distributionStr.append("('").append(d.getLeader()).append("'");
                String followersStr = d.getFollowers().stream().map(f -> "'" + f + "'").collect(Collectors.joining(","));
                distributionStr.append(",").append(String.format("[%s]",followersStr)).append("),");
            }
            distributionStr.deleteCharAt(distributionStr.length()-1).append("]");
        }
        String option = null;
        if(StringUtils.isNotEmpty(storage)){
            option = String.format("options(partitionnum=%s,replicanum=%s%s,storage_mode=\"%s\")",partitionNum,replicaNum,distributionStr,storage);
        }else {
            option = String.format("options(partitionnum=%s,replicanum=%s%s)",partitionNum,replicaNum,distributionStr);
        }
        //String option = String.format("options(partitionnum=%s,replicanum=%s%s)",partitionNum,replicaNum,distributionStr);
        sql = sql+option+";";
        // if (replicaNum == 1) {
        //     sql += ");";
        // } else {
        //     sql = sql + ")options (replicanum=" + replicaNum + ");";
        // }
        return sql;
    }


}
