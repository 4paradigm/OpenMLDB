package com._4paradigm.fesql.sqlcase.model;

import lombok.Data;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
public class SQLCase implements Serializable{
    int level = 0;
    String id;
    String desc;
    String mode;
    String db;
    String sql;
    List<List<String>> dataProvider;
    List<String> sqls;
    boolean standard_sql;
    boolean standard_sql_compatible;
    List<String> tags;
    List<String> common_column_indices;
    String batch_plan;
    String request_plan;
    String cluster_request_plan;
    List<InputDesc> inputs;
    InputDesc batch_request;
    ExpectDesc expect;
    String spName = genAutoName();
    UnequalExpect unequalExpect;

    private Map<Integer,ExpectDesc> expectProvider;

    public static String formatSql(String sql, int idx, String name) {
        return sql.replaceAll("\\{" + idx + "\\}", name);
    }

    public int getLevel() {
        return this.level;
    }

    public static String formatSql(String sql, String name) {
        return sql.replaceAll("\\{auto\\}", name);
    }

    public String getSql() {
        sql = formatSql(sql, Table.genAutoName());
        if (CollectionUtils.isEmpty(inputs)) {
            return sql;
        }
        for (int idx = 0; idx < inputs.size(); idx++) {
            sql = formatSql(sql, idx, inputs.get(idx).getName());
        }
        return sql;
    }


    public static String genAutoName() {
        return "auto_" + RandomStringUtils.randomAlphabetic(8);
    }

    public String getProcedure(String sql) {
        return buildCreateSpSQLFromColumnsIndexs(spName, sql, inputs.get(0).getColumns());
    }

    public static String buildCreateSpSQLFromColumnsIndexs(String name, String sql, List<String> columns) {
        if (sql == null || sql.isEmpty()) {
            return "";
        }
        StringBuilder builder = new StringBuilder("create procedure " + name + "(\n");
        for (int i = 0; i < columns.size(); i++) {
            builder.append(columns.get(i));
            if (i != columns.size() - 1) {
                builder.append(",");
            }
        }
        builder.append(")\n");
        builder.append("BEGIN\n");
        builder.append(sql);
        builder.append("\n");
        builder.append("END;");
        sql = builder.toString();
        return sql;
    }
}
