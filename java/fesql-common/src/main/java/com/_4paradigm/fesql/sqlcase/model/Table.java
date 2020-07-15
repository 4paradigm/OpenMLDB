package com._4paradigm.fesql.sqlcase.model;

import com.google.common.collect.Lists;
import lombok.Data;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Data
public class Table {
    String name = "auto_" + RandomStringUtils.randomAlphabetic(8);
    String index;
    String schema;
    String data;
    List<String> indexs;
    List<String> columns;
    List<List<String>> rows;
    String create;
    String insert;

    public String getCreate() {
        if (!StringUtils.isEmpty(create)) {
            return create;
        }
        return buildCreateSQLFromColumnsIndexs(name, columns, indexs);
    }

    public String getInsert() {
        if (!StringUtils.isEmpty(insert)) {
            return insert;
        }
        return buildInsertSQLFromRows(name, columns, rows);
    }


    public List<String> getIndexs() {
        if (!CollectionUtils.isEmpty(indexs)) {
            return indexs;
        }

        if (StringUtils.isEmpty(index)) {
            return Collections.emptyList();
        }

        List<String> parserd_indexs= new ArrayList<>();

        for (String index : index.trim().split(",|\n")) {
            parserd_indexs.add(index.trim());
        }
        return parserd_indexs;
    }
    /**
     * Return columns list.
     * if columns is empty,  convert schema to columns:
     * <code>col_name:col_type to columns</code>
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
     * Return rows,
     * convert data string to rows if rows is empty
     *
     * @return [
     *  [value, value, ...],
     *  [value, value, ...],
     *  [value, value, ...],
     *  ...]
     */
    public List<List<String>> getRows() {
        if (!CollectionUtils.isEmpty(rows)) {
            return rows;
        }

        if (StringUtils.isEmpty(data)) {
            return Collections.emptyList();
        }

        List<List<String>> parserd_rows = new ArrayList<>();

        for (String row : data.trim().split("\n")) {
            List<String> each_row = new ArrayList<String>();
            for (String item : row.trim().split(",")) {
                each_row.add(item.trim());
            }
            parserd_rows.add(each_row);
        }
        return parserd_rows;
    }

    private String buildInsertSQLFromRows(String name, List<String> columns, List<List<String>> datas) {
        // insert rows
        StringBuilder builder = new StringBuilder("insert into ").append(name).append(" values");
        for (int row_id = 0; row_id < datas.size(); row_id++) {
            List list = datas.get(row_id);
            builder.append("\n(");
            for (int i = 0; i < list.size(); i++) {
                String columnType = getColumnType(columns.get(i));
                Object data = list.get(i);
                String dataStr = data + "";
                if (columnType.equals("string") || columnType.equals("date")) {
                    dataStr = "'" + data + "'";
                } else if (columnType.equals("timestamp")) {
                    dataStr = data + "L";
                }

                if (i < list.size() - 1) {
                    builder.append(dataStr + ",");
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

    private String buildCreateSQLFromColumnsIndexs(String name, List<String> columns, List<String> indexs) {
        String sql;
        StringBuilder builder = new StringBuilder("create table " + name + "(");
//            for(String column:columns){
//                String[] ss = column.split(":");
//                builder.append(ss[0]+" "+ss[1]+" NOT NULL,");
//            }
        for (String column : columns) {
            builder.append(column + ",");
        }
        for (String index : indexs) {
            String[] ss = index.split(":");
            if (ss.length == 3) {
                builder.append(String.format("index(key=(%s),ts=%s),", ss[1], ss[2]));
            } else if (ss.length == 4) {
                builder.append(String.format("index(key=(%s),ts=%s,ttl=%s),", ss[1], ss[2], ss[3]));
            } else if (ss.length == 5) {
                builder.append(String.format("index(key=(%s),ts=%s,ttl=%s,ttl_type=%s),", ss[1], ss[2], ss[3], ss[4]));
            }
        }
        sql = builder.toString();
        if (sql.endsWith(",")) {
            sql = sql.substring(0, sql.length() - 1);
        }
        sql += ");";
        return sql;
    }

    public String getIndexName(String index) {
        String[] splits = index.trim().split(":");
        if (splits.length < 1) {
            return "";
        }
        return splits[0];
    }
    public List<String> getIndexKeys(String index) {
        String[] splits = index.trim().split(":");
        if (splits.length < 2) {
            return Collections.emptyList();
        }
        return Lists.newArrayList(splits[1].trim().split("|"));
    }
    public String getIndexTsCol(String index) {
        String[] splits = index.trim().split(":");
        if (splits.length < 3) {
            return "";
        }
        return splits[2];
    }
    public String getColumnName(String column) {
        int pos = column.trim().lastIndexOf(' ');
        return column.trim().substring(0, pos);
    }
    public String getColumnType(String column) {
        int pos = column.trim().lastIndexOf(' ');
        return column.trim().substring(pos);
    }
}
