package com._4paradigm.fesql.sqlcase.model;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import lombok.Data;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Data
public class Table implements Serializable{
    String name = genAutoName();
    String index;
    String schema;
    String data;
    List<String> indexs;
    List<String> columns;
    List<List<Object>> rows;
    String create;
    String insert;
    int repeat = 1;

    List<String> common_column_indices;

    private boolean drop = true;

    private static final Logger logger = LoggerFactory.getLogger(Table.class);

    public static String genAutoName() {
        return "auto_" + RandomStringUtils.randomAlphabetic(8);
    }

    /**
     * 从输入构造Create SQL：
     * 如果create非空，直接返回create，否则需要根据columns/schema来构造Create SQL语句
     *
     * @return
     */
    public String getCreate(int replicaNum) {
        if (!StringUtils.isEmpty(create)) {
            return create;
        }
        return buildCreateSQLFromColumnsIndexs(name, getColumns(), getIndexs(), replicaNum);
    }

    public String getCreate() {
        return getCreate(1);
    }

    /**
     * 从输入构造Insert SQL：
     * 如果insert非空，直接返回insert，否则需要根据columns和rows来构造Insert SQL语句
     *
     * @return
     */
    public String getInsert() {
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
        if (!StringUtils.isEmpty(insert)) {
            return Lists.newArrayList(insert);
        }
        List<String> inserts = Lists.newArrayList();
        for (List<Object> row : getRows()) {
            List<List<Object>> rows = Lists.newArrayList();
            rows.add(row);
            inserts.add(buildInsertSQLFromRows(name, getColumns(),
                    rows));
        }
        return inserts;
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

    /**
     * 获取Rows
     * 如果 rows 非空，直接返回 rows, 否则需要从 data 解析出 rows
     *
     * @return
     */
    public List<List<Object>> getRows() {
        if (!CollectionUtils.isEmpty(rows)) {
            return rows;
        }

        if (StringUtils.isEmpty(data)) {
            return Collections.emptyList();
        }

        List<List<Object>> parserd_rows = new ArrayList<>();

        for (String row : data.trim().split("\n")) {
            List<Object> each_row = new ArrayList<Object>();
            for (String item : row.trim().split(",")) {
                each_row.add(item.trim());
            }
            parserd_rows.add(each_row);
        }
        return parserd_rows;
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
            logger.info("Index is invalid: empty index");
            return false;
        }

        if (length < 2) {
            logger.info("Index is invalid: missing keys and ts, {}", index);
            return false;
        }

        if (length > 5) {
            logger.info("Index is invalid: index items {} > 5", index);
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

    public static String buildCreateSQLFromColumnsIndexs(String name, List<String> columns, List<String> indexs,
            int replicaNum) {
        if (CollectionUtils.isEmpty(indexs) || CollectionUtils.isEmpty(columns)) {
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
        sql = builder.toString();
        if (sql.endsWith(",")) {
            sql = sql.substring(0, sql.length() - 1);
        }
        if (replicaNum == 1) {
            sql += ");";
        } else {
            sql = sql + ")replicanum=" + replicaNum + ";";
        }
        return sql;
    }

}
