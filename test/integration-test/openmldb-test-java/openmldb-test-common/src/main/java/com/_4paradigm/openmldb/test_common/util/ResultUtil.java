package com._4paradigm.openmldb.test_common.util;

import com._4paradigm.openmldb.jdbc.SQLResultSet;
import com._4paradigm.openmldb.test_common.bean.OpenMLDBJob;
import com._4paradigm.openmldb.test_common.bean.OpenMLDBResult;
import com._4paradigm.openmldb.test_common.model.OpenmldbDeployment;
import com.google.common.base.Joiner;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.text.SimpleDateFormat;

@Slf4j
public class ResultUtil {
    public static void parseResultSet(Statement statement,OpenMLDBResult openMLDBResult){
        try {
            ResultSet resultSet = statement.getResultSet();
            if (resultSet == null) {
                openMLDBResult.setOk(false);
                openMLDBResult.setMsg("executeSQL fail, result is null");
            } else if  (resultSet instanceof SQLResultSet){
                SQLResultSet rs = (SQLResultSet)resultSet;
                ResultUtil.setSchema(rs.getMetaData(),openMLDBResult);
                List<List<Object>> result = ResultUtil.toList(rs);
                openMLDBResult.setCount(result.size());
                openMLDBResult.setResult(result);
//                openMLDBResult.setMsg("success");
//                openMLDBResult.setOk(true);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            openMLDBResult.setOk(false);
            openMLDBResult.setMsg(e.getMessage());
        }
    }
    public static OpenMLDBJob parseJob(OpenMLDBResult openMLDBResult){
        OpenMLDBJob openMLDBJob = new OpenMLDBJob();
        List<List<Object>> result = openMLDBResult.getResult();
        String[] ss = String.valueOf(result.get(0)).trim().split(",");
        openMLDBJob.setId(Integer.parseInt(ss[0].substring(1)));
        openMLDBJob.setJobType(ss[1]);
        openMLDBJob.setState(ss[2].substring(1));
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
        Timestamp start_time = new java.sql.Timestamp(0);
        Timestamp end_time = new java.sql.Timestamp(0);
        try {
            java.util.Date parsedDate = dateFormat.parse(ss[3].substring(1));
            Timestamp timestamp = new java.sql.Timestamp(parsedDate.getTime());
            start_time = timestamp;
            parsedDate = dateFormat.parse(ss[4].substring(1));
            timestamp = new java.sql.Timestamp(parsedDate.getTime());
            end_time = timestamp;
        } catch(Exception e) { 
        }
        openMLDBJob.setStartTime(start_time);
        openMLDBJob.setEndTime(end_time);
        openMLDBJob.setParameter(ss[5]);
        openMLDBJob.setCluster(ss[6]);
        openMLDBJob.setApplicationId(ss[7]);
        if(ss.length==9) {
            openMLDBJob.setError(ss[8]);
        }
        return openMLDBJob;
    }
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
//            log.info("convert string data {}", obj);
        } else if (columnType == Types.TIMESTAMP) {
            obj = rs.getTimestamp(index + 1);
        }
        return obj;
    }
}
