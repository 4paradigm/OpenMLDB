package com._4paradigm.openmldb.test_common.util;

import com._4paradigm.openmldb.test_common.model.InputDesc;
import com._4paradigm.openmldb.test_common.openmldb.OpenMLDBGlobalVar;
import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBInfo;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SQLUtil {
    private static String reg = "\\{(\\d+)\\}";
    private static Pattern pattern = Pattern.compile(reg);

    public static String replaceDBNameAndTableName(String dbName,List<String> tableNames,String str){
        Matcher matcher = pattern.matcher(str);
        while (matcher.find()) {
            int index = Integer.parseInt(matcher.group(1));
            str = str.replace("{" + index + "}", tableNames.get(index));
        }
        str = str.replace("{db_name}",dbName);
        return str;
    }
    public static String replaceDBNameAndSpName(String dbName,String spName,String str){
        str = str.replace("{sp_name}",spName);
        str = str.replace("{db_name}",dbName);
        return str;
    }

    public static String getLongWindowDeploySQL(String name,String longWindow,String sql){
        String deploySql = String.format("deploy %s options(long_windows='%s') %s",name,longWindow,sql);
        return deploySql;
    }

    public static String genInsertSQL(String tableName, List<List<Object>> dataList) {
        if (CollectionUtils.isEmpty(dataList)) {
            return "";
        }
        // insert rows
        StringBuilder builder = new StringBuilder("insert into ").append(tableName).append(" values");
        for (int row_id = 0; row_id < dataList.size(); row_id++) {
            List list = dataList.get(row_id);
            builder.append("\n(");
            for (int i = 0; i < list.size(); i++) {
                Object data = list.get(i);
                if(data == null){
                    data = "null";
                }else if(data instanceof String){
                    data = DataUtil.parseRules((String)data);
                }
                if(data instanceof String){
                    data = "'" + data + "'";
                }
                builder.append(data);
                if (i < list.size() - 1) {
                    builder.append(",");
                }
            }
            if (row_id < dataList.size() - 1) {
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
    public static String formatSql(String sql, List<String> tableNames, OpenMLDBInfo openMLDBInfo) {
        Matcher matcher = pattern.matcher(sql);
        while (matcher.find()) {
            int index = Integer.parseInt(matcher.group(1));
            sql = sql.replace("{" + index + "}", tableNames.get(index));
        }
        sql = formatSql(sql,openMLDBInfo);
        return sql;
    }

    public static String formatSql(String sql, OpenMLDBInfo openMLDBInfo) {
        if(sql.contains("{tb_endpoint_0}")){
            sql = sql.replace("{tb_endpoint_0}", openMLDBInfo.getTabletEndpoints().get(0));
        }
        if(sql.contains("{tb_endpoint_1}")){
            sql = sql.replace("{tb_endpoint_1}", openMLDBInfo.getTabletEndpoints().get(1));
        }
        if(sql.contains("{tb_endpoint_2}")){
            sql = sql.replace("{tb_endpoint_2}", openMLDBInfo.getTabletEndpoints().get(2));
        }
        return sql;
    }
    public static String formatSql(String sql) {
        return sql.replace("{root_path}", Tool.openMLDBDir().getAbsolutePath());
    }
    public static void setExecuteMode(String sql){
        sql = sql.toLowerCase();
        if(sql.startsWith("set ")){
            if (sql.contains("online")) {
                OpenMLDBGlobalVar.EXECUTE_MODE="online";
            }else{
                OpenMLDBGlobalVar.EXECUTE_MODE="offline";
            }
        }
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
