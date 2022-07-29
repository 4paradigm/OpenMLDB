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
package com._4paradigm.openmldb.java_sdk_test.util;


import com._4paradigm.openmldb.test_common.bean.OpenMLDBResult;
import com._4paradigm.openmldb.test_common.common.ReportLog;
import com._4paradigm.openmldb.test_common.model.DBType;
import com._4paradigm.openmldb.test_common.util.TypeUtil;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zhaowei
 * @date 2021/3/19 5:41 PM
 */
@Slf4j
public class JDBCUtil {
    public static ReportLog reportLog = ReportLog.of();

    public static int executeUpdate(String sql, DBType dbType){
        log.info("jdbc sql:{}",sql);
        reportLog.info("jdbc sql:{}",sql);
        int n = -1;
        try(Connection connection= ConnectionFactory.of().getConn(dbType)){
            Statement statement=connection.createStatement();
            n = statement.executeUpdate(sql);
        }catch (Exception e){
            e.printStackTrace();
        }
        log.info("jdbc update result:{}",n);
        reportLog.info("jdbc update result:{}",n);
        return n;
    }
    public static OpenMLDBResult executeQuery(String sql, DBType dbType){
        log.info("jdbc sql:{}",sql);
        reportLog.info("jdbc sql:{}",sql);
        OpenMLDBResult fesqlResult = new OpenMLDBResult();
        try(Connection connection= ConnectionFactory.of().getConn(dbType)){
            Statement statement=connection.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            ResultSetMetaData metaData = rs.getMetaData();
            JDBCUtil.setSchema(metaData,fesqlResult);
            List<List<Object>> list = null;
            if(dbType==DBType.SQLITE3){
                list = new ArrayList<>();
                int columnCount = metaData.getColumnCount();
                while (rs.next()) {
                    List<Object> objs = new ArrayList<>();
                    for(int i=1;i<=columnCount;i++){
                        objs.add(rs.getObject(i));
                    }
                    list.add(objs);
                }
            }else {
                list = convertRestultSetToList(rs);
            }
            fesqlResult.setResult(list);
            fesqlResult.setCount(list.size());
            fesqlResult.setOk(true);
        } catch (SQLException e) {
            fesqlResult.setOk(false);
            fesqlResult.setMsg(e.getMessage());
            e.printStackTrace();
        }
        log.info("jdbc query result:{}",fesqlResult);
        reportLog.info("jdbc query result:{}",fesqlResult);
        return fesqlResult;
    }

    private static List<List<Object>> convertRestultSetToList(ResultSet rs) throws SQLException {
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

    public static void setSchema(ResultSetMetaData metaData, OpenMLDBResult fesqlResult) {
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
            fesqlResult.setColumnNames(columnNames);
            fesqlResult.setColumnTypes(columnTypes);
        }catch (SQLException e){
            e.printStackTrace();
        }
    }

    public static Object getColumnData(ResultSet rs, int index) throws SQLException {
        int columnIndex = index + 1;
        int columnType = rs.getMetaData().getColumnType(columnIndex);
        if (rs == null) {
            log.info("rs is null");
            reportLog.info("rs is null");
            return null;
        }
        switch (columnType){
            case Types.BIT:
            case Types.BOOLEAN:
                return rs.getBoolean(columnIndex);
            case Types.VARCHAR:
                return rs.getString(columnIndex);
            case Types.SMALLINT:
                return rs.getShort(columnIndex);
            case Types.INTEGER:
                return rs.getInt(columnIndex);
            case Types.BIGINT:
                return rs.getLong(columnIndex);
            case Types.REAL:
            case Types.FLOAT:
                return rs.getFloat(columnIndex);
            case Types.DOUBLE:
                return rs.getDouble(columnIndex);
            case Types.TIMESTAMP:
                return rs.getTimestamp(columnIndex);
            case Types.DATE:
                return rs.getDate(columnIndex);
            default:
                return null;
        }
    }
}
