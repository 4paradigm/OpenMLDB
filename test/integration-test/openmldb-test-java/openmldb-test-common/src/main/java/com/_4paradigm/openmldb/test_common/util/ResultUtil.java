package com._4paradigm.openmldb.test_common.util;

import com._4paradigm.openmldb.jdbc.SQLResultSet;
import com._4paradigm.openmldb.test_common.bean.OpenMLDBResult;
import com._4paradigm.openmldb.test_common.bean.SQLType;
import com._4paradigm.openmldb.test_common.model.OpenmldbDeployment;
import com.google.common.base.Joiner;
import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@Slf4j
public class ResultUtil {
    public static OpenmldbDeployment parseDeployment(List<String> lines){
        OpenmldbDeployment deployment = new OpenmldbDeployment();
        List<String> inColumns = new ArrayList<>();
        List<String> outColumns = new ArrayList<>();
        String[] db_sp = lines.get(3).split("\\s+");
        deployment.setDbName(db_sp[1]);
        deployment.setName(db_sp[2]);

        String sql = "";
        List<String> list = lines.subList(9, lines.size());
        Iterator<String> it = list.iterator();
        while(it.hasNext()) {
            String line = it.next().trim();
            if (line.contains("row in set")) break;
            if (line.startsWith("#") || line.startsWith("-")) continue;
            sql += line+"\n";
        }
        deployment.setSql(sql);
        while(it.hasNext()){
            String line = it.next().trim();
            if (line.contains("Output Schema")) break;
            if (line.startsWith("#") || line.startsWith("-")|| line.equals("")) continue;
            String[] infos = line.split("\\s+");
            String in = Joiner.on(",").join(infos);
            inColumns.add(in);
        }
        while(it.hasNext()){
            String line = it.next().trim();
            if(line.startsWith("#")||line.startsWith("-"))continue;
            String[] infos = line.split("\\s+");
            String out = Joiner.on(",").join(infos);
            outColumns.add(out);
        }
        deployment.setInColumns(inColumns);
        deployment.setOutColumns(outColumns);
        return deployment;
    }
    public static void setSchema(ResultSetMetaData metaData, OpenMLDBResult openMLDBResult) {
        try {
            int columnCount = metaData.getColumnCount();
            List<String> columnNames = new ArrayList<>();
            List<String> columnTypes = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                String columnLabel = null;
                try {
                    columnLabel = metaData.getColumnLabel(i);
                }catch (SQLException e){
                    columnLabel = metaData.getColumnName(i);
                }
                columnNames.add(columnLabel);
                columnTypes.add(TypeUtil.fromJDBCTypeToString(metaData.getColumnType(i)));
            }
            openMLDBResult.setColumnNames(columnNames);
            openMLDBResult.setColumnTypes(columnTypes);
        }catch (SQLException e){
            e.printStackTrace();
        }
    }

    public static List<List<Object>> toList(SQLResultSet rs) throws SQLException {
        List<List<Object>> result = new ArrayList<>();
        while (rs.next()) {
            List list = new ArrayList();
            int columnCount = rs.getMetaData().getColumnCount();
            for (int i = 0; i < columnCount; i++) {
                list.add(getColumnData(rs, i));
            }
            result.add(list);
        }
        return result;
    }



    public static String convertResultSetToListDeploy(SQLResultSet rs) throws SQLException {
        String string = null;
        while (rs.next()) {
            int columnCount = rs.getMetaData().getColumnCount();
            for (int i = 0; i < columnCount; i++) {
                string=String.valueOf(getColumnData(rs, i));
            }
        }
        return string;
    }

    public static List<String> convertResultSetToListDesc(SQLResultSet rs) throws SQLException {
        List<String> res = new ArrayList<>();
        while (rs.next()) {
            int columnCount = rs.getMetaData().getColumnCount();
            for (int i = 0; i < columnCount; i++) {
                String string=String.valueOf(getColumnData(rs, i));
                res.add(string);
            }
        }
        return res;
    }

    public static Object getColumnData(SQLResultSet rs, int index) throws SQLException {
        Object obj = null;
        int columnType = rs.getMetaData().getColumnType(index + 1);
        if (rs.getNString(index + 1) == null) {
            log.info("rs is null");
            return null;
        }
        if (columnType == Types.BOOLEAN) {
            obj = rs.getBoolean(index + 1);
        } else if (columnType == Types.DATE) {
            try {
//                obj = new Date(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//                        .parse(rs.getNString(index + 1) + " 00:00:00").getTime());
                obj = rs.getDate(index + 1);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        } else if (columnType == Types.DOUBLE) {
            obj = rs.getDouble(index + 1);
        } else if (columnType == Types.FLOAT) {
            obj = rs.getFloat(index + 1);
        } else if (columnType == Types.SMALLINT) {
            obj = rs.getShort(index + 1);
        } else if (columnType == Types.INTEGER) {
            obj = rs.getInt(index + 1);
        } else if (columnType == Types.BIGINT) {
            obj = rs.getLong(index + 1);
        } else if (columnType == Types.VARCHAR) {
            obj = rs.getString(index + 1);
            log.info("conver string data {}", obj);
        } else if (columnType == Types.TIMESTAMP) {
            obj = rs.getTimestamp(index + 1);
        }
        return obj;
    }
}
