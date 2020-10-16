package com._4paradigm.fesql.sqlcase.model;

import lombok.Data;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;

@Data
public class SQLCase {
    String id;
    String desc;
    String mode;
    String db;
    String sql;
    List<String> sqls;
    boolean standard_sql;
    boolean standard_sql_compatible;
    List<String> tags;
    List<String> common_column_indices;
    String batch_plan;
    String request_plan;
    List<InputDesc> inputs;
    ExpectDesc expect;

    public static String formatSql(String sql, int idx, String name) {
        return sql.replaceAll("\\{" + idx + "\\}", name);
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
}
