package com._4paradigm.fesql_auto_test.entity;

import com._4paradigm.fesql_auto_test.util.FesqlUtil;
import com._4paradigm.sql.sdk.SqlExecutor;
import com.google.common.base.Joiner;
import com._4paradigm.sql.Schema;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
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
    // private ResultSet rs;

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("FesqlResult{ok=" + ok + ", count=" + count + "}");
        if (result != null) {
            builder.append("result=" + result.size() + ":\n");
            if (resultSchema != null) {
                int columnCount = resultSchema.GetColumnCnt();
                for (int i = 0; i < columnCount; i++) {
                    builder.append(resultSchema.GetColumnName(i))
                            .append(" ")
                            .append(FesqlUtil.getColumnTypeString(resultSchema.GetColumnType(i)))
                            .append(",");
                    if (i < columnCount - 1) {
                        builder.append(",");
                    }
                }
            } else {
                try {
                    int columnCount = metaData.getColumnCount();
                    for (int i = 0; i < columnCount; i++) {
                        builder.append(metaData.getColumnName(i + 1))
                                .append(" ")
                                .append(FesqlUtil.getSQLTypeString(metaData.getColumnType(i + 1)))
                                .append(",");
                    }
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
        boolean flag = toString().equals(that.toString());
        return flag;
    }

    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }
}
