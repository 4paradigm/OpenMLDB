package com._4paradigm.sql.sdk.impl;

import java.sql.SQLException;
import java.sql.Types;

public class Util {
    public static String sqlTypeToString(int sqlType) throws SQLException {
        switch (sqlType) {
            case Types.SMALLINT:
                return "int16";
            case Types.INTEGER:
                return "int32";
            case Types.BIGINT:
                return "int64";
            case Types.FLOAT:
                return "float";
            case Types.DOUBLE:
                return "double";
            case Types.BOOLEAN:
                return "bool";
            case Types.VARCHAR:
                return "string";
            case Types.TIMESTAMP:
                return "timestamp";
            case Types.DATE:
                return "date";
            default:
                throw new SQLException("unsupported type: " + sqlType);
        }
    }
}
