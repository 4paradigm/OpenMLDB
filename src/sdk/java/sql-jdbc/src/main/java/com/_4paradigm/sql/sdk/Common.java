package com._4paradigm.sql.sdk;

import com._4paradigm.sql.DataType;

import java.sql.SQLException;
import java.sql.Types;

public class Common {
    public static int type2SqlType(DataType dataType) throws SQLException {
        if (dataType == DataType.kTypeBool) {
            return Types.BOOLEAN;
        } else if (dataType == DataType.kTypeInt16) {
            return Types.SMALLINT;
        } else if (dataType == DataType.kTypeInt32) {
            return Types.INTEGER;
        } else if (dataType == DataType.kTypeInt64) {
            return Types.BIGINT;
        } else if (dataType == DataType.kTypeFloat) {
            return Types.FLOAT;
        } else if (dataType == DataType.kTypeDouble) {
            return Types.DOUBLE;
        } else if (dataType == DataType.kTypeString) {
            return Types.VARCHAR;
        } else if (dataType == DataType.kTypeDate) {
            return Types.DATE;
        } else if (dataType == DataType.kTypeTimestamp) {
            return Types.TIMESTAMP;
        } else {
            throw new SQLException("Unexpected value: " + dataType.toString());
        }
    }
}
