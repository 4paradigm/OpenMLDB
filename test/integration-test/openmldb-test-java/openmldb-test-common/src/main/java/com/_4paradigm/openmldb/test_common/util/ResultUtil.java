package com._4paradigm.openmldb.test_common.util;

import com._4paradigm.openmldb.test_common.bean.OpenMLDBResult;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ResultUtil {
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
                columnTypes.add(OpenMLDBUtil.getSQLTypeString(metaData.getColumnType(i)));
            }
            fesqlResult.setColumnNames(columnNames);
            fesqlResult.setColumnTypes(columnTypes);
        }catch (SQLException e){
            e.printStackTrace();
        }
    }
}
