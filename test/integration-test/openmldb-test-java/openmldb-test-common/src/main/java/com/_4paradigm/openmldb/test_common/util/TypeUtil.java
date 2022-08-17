package com._4paradigm.openmldb.test_common.util;

import com._4paradigm.openmldb.DataType;

import java.sql.Types;

public class TypeUtil {
    public static DataType getOpenMLDBColumnType(String type) {
        switch (type) {
            case "smallint":
            case "int16":
                return DataType.kTypeInt16;
            case "int32":
            case "i32":
            case "int":
                return DataType.kTypeInt32;
            case "int64":
            case "bigint":
                return DataType.kTypeInt64;
            case "float":
                return DataType.kTypeFloat;
            case "double":
                return DataType.kTypeDouble;
            case "bool":
                return DataType.kTypeBool;
            case "string":
                return DataType.kTypeString;
            case "timestamp":
                return DataType.kTypeTimestamp;
            case "date":
                return DataType.kTypeDate;
            default:
                return null;
        }
    }

    public static int getJDBCColumnType(String type) {
        switch (type) {
            case "smallint":
            case "int16":
                return Types.SMALLINT;
            case "int32":
            case "i32":
            case "int":
                return Types.INTEGER;
            case "int64":
            case "bigint":
                return Types.BIGINT;
            case "float":
                return Types.FLOAT;
            case "double":
                return Types.DOUBLE;
            case "bool":
                return Types.BOOLEAN;
            case "string":
                return Types.VARCHAR;
            case "timestamp":
                return Types.TIMESTAMP;
            case "date":
                return Types.DATE;
            default:
                return 0;
        }
    }

    public static String fromOpenMLDBTypeToString(DataType dataType) {
        if (dataType.equals(DataType.kTypeBool)) {
            return "bool";
        } else if (dataType.equals(DataType.kTypeString)) {
            return "string";
        } else if (dataType.equals(DataType.kTypeInt16)) {
            return "smallint";
        } else if (dataType.equals(DataType.kTypeInt32)) {
            return "int";
        } else if (dataType.equals(DataType.kTypeInt64)) {
            return "bigint";
        } else if (dataType.equals(DataType.kTypeFloat)) {
            return "float";
        } else if (dataType.equals(DataType.kTypeDouble)) {
            return "double";
        } else if (dataType.equals(DataType.kTypeTimestamp)) {
            return "timestamp";
        } else if (dataType.equals(DataType.kTypeDate)) {
            return "date";
        }
        return null;
    }

    public static String fromJDBCTypeToString(int dataType) {
        switch (dataType){
            case Types.BIT:
            case Types.BOOLEAN:
                return "bool";
            case Types.VARCHAR:
                return "string";
            case Types.SMALLINT:
                return "smallint";
            case Types.INTEGER:
                return "int";
            case Types.BIGINT:
                return "bigint";
            case Types.REAL:
            case Types.FLOAT:
                return "float";
            case Types.DOUBLE:
                return "double";
            case Types.TIMESTAMP:
                return "timestamp";
            case Types.DATE:
                return "date";
            default:
                return null;
        }
    }
}
