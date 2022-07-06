package com._4paradigm.openmldb.test_common.util;

import com._4paradigm.openmldb.test_common.model.InputDesc;
import com._4paradigm.openmldb.test_common.openmldb.OpenMLDBGlobalVar;
import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBInfo;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SQLUtil {
    private static String reg = "\\{(\\d+)\\}";
    private static Pattern pattern = Pattern.compile(reg);

    public static String formatSql(String sql, List<String> tableNames, OpenMLDBInfo fedbInfo) {
        Matcher matcher = pattern.matcher(sql);
        while (matcher.find()) {
            int index = Integer.parseInt(matcher.group(1));
            sql = sql.replace("{" + index + "}", tableNames.get(index));
        }
        sql = formatSql(sql,fedbInfo);
        return sql;
    }

    public static String formatSql(String sql, OpenMLDBInfo fedbInfo) {
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
        return formatSql(sql,tableNames, OpenMLDBGlobalVar.mainInfo);
    }
    public static String buildSpSQLWithConstColumns(String spName, String sql, InputDesc input) throws SQLException {
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
}
