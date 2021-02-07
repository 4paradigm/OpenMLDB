package com._4paradigm.fesql_auto_test.entity;

import com._4paradigm.fesql_auto_test.util.FesqlUtil;
import com._4paradigm.sql.sdk.SqlExecutor;
import com.google.common.base.Joiner;
import com._4paradigm.sql.Schema;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

/**
 * @author zhaowei
 * @date 2020/6/15 11:36 AM
 */
@Data
public class FesqlResult {
    private boolean ok;
    private int count;
    private List<List<Object>> result;
    private Schema resultSchema;
    private ResultSetMetaData metaData;

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("FesqlResult{ok=" + ok + ", count=" + count + "}");
        if (result != null) {
            builder.append("result=" + result.size() + ":\n");
            // builder.append(getColumnInfo());
            if (resultSchema != null) {
                builder.append(getColumnInfo(resultSchema));
            } else {
                try {
                    System.out.println("==========");
                    int columnCount = metaData.getColumnCount();
                    for (int i = 0; i < columnCount; i++) {
                        builder.append(metaData.getColumnName(i + 1))
                                .append(" ")
                                .append(FesqlUtil.getSQLTypeString(metaData.getColumnType(i + 1)))
                                .append(",");
                    }
                    builder.deleteCharAt(builder.length()-1);
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
            builder.append("\n");
            for (int i = 0; i < result.size(); i++) {
                List list = result.get(i);
                builder.append(Joiner.on(",").useForNull("null(obj)").join(list)).append("\n");
            }
        }
        return builder.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FesqlResult that = (FesqlResult) o;
        if (ok != that.ok) return false;
        if (count != that.count) return false;
        if (result != null ? !result.equals(that.result) : that.result != null) return false;
        if (resultSchema != null ? !getColumnInfo(resultSchema).equals(getColumnInfo(that.resultSchema)) : that.resultSchema != null) return false;
        boolean flag = metaData != null ? getColumnInfo(metaData).equals(getColumnInfo(that.metaData)) : that.metaData == null;
        // boolean flag = getColumnInfo().equals(that.getColumnInfo());
        return flag;
    }
    public String getColumnInfo() {
        StringBuilder builder = new StringBuilder();
        try {
            if (resultSchema != null) {
                int columnCount = resultSchema.GetColumnCnt();
                for (int i = 0; i < columnCount; i++) {
                    builder.append(resultSchema.GetColumnName(i))
                            .append(" ")
                            .append(FesqlUtil.getColumnTypeString(resultSchema.GetColumnType(i)))
                            .append(",");
                    // if (i < columnCount - 1) {
                    //     builder.append(",");
                    // }
                }
            } else if(metaData!=null){
                int columnCount = metaData.getColumnCount();
                for (int i = 0; i < columnCount; i++) {
                    builder.append(metaData.getColumnName(i + 1))
                            .append(" ")
                            .append(FesqlUtil.getSQLTypeString(metaData.getColumnType(i + 1)))
                            .append(",");
                }
            }
            if(builder.length()>0) {
                builder.deleteCharAt(builder.length() - 1);
            }
        }catch (SQLException e){
            e.printStackTrace();
        }
        return builder.toString();
    }
    public String getColumnInfo(Schema schema) {
        StringBuilder builder = new StringBuilder();
        int columnCount = schema.GetColumnCnt();
        for (int i = 0; i < columnCount; i++) {
            builder.append(schema.GetColumnName(i))
                    .append(" ")
                    .append(FesqlUtil.getColumnTypeString(schema.GetColumnType(i)))
                    .append(",");
            // if (i < columnCount - 1) {
            //     builder.append(",");
            // }
        }
        builder.deleteCharAt(builder.length() - 1);
        return builder.toString();
    }
    public String getColumnInfo(ResultSetMetaData metaData){
        StringBuilder builder = new StringBuilder();
        try {
            int columnCount = metaData.getColumnCount();
            for (int i = 0; i < columnCount; i++) {
                builder.append(metaData.getColumnName(i + 1))
                        .append(" ")
                        .append(FesqlUtil.getSQLTypeString(metaData.getColumnType(i + 1)))
                        .append(",");
            }
            builder.deleteCharAt(builder.length()-1);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return builder.toString();
    }

    @Override
    public int hashCode() {
        int result1 = super.hashCode();
        result1 = 31 * result1 + (ok ? 1 : 0);
        result1 = 31 * result1 + count;
        result1 = 31 * result1 + (result != null ? result.hashCode() : 0);
        result1 = 31 * result1 + (resultSchema != null ? getColumnInfo(resultSchema).hashCode() : 0);
        result1 = 31 * result1 + (metaData != null ? getColumnInfo(metaData).hashCode() : 0);
        return result1;
    }
}
