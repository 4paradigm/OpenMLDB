package com._4paradigm.openmldb.sdk;

public enum DataType {
  kTypeBool,
  kTypeInt16,
  kTypeInt32,
  kTypeInt64,
  kTypeFloat,
  kTypeDouble,
  kTypeString,
  kTypeDate,
  kTypeTimestamp,
  kTypeUnknow,
  ;

  public static DataType fromSqlType(String sqlType) {
    switch (sqlType) {
      case "smallint":
        return kTypeInt16;
      case "int":
        return kTypeInt32;
      case "bigint":
        return kTypeInt64;
      case "float":
        return kTypeFloat;
      case "double":
        return kTypeDouble;
      case "bool":
        return kTypeBool;
      case "string":
        return kTypeString;
      case "timestamp":
        return kTypeTimestamp;
      case "date":
        return kTypeDate;
      default:
        return kTypeUnknow;
    }
  }

  public static com._4paradigm.openmldb.DataType toOpenMLDBType(String sqlType) {
    return toOpenMLDBType(fromSqlType(sqlType));
  }

  public static com._4paradigm.openmldb.DataType toOpenMLDBType(DataType dataType) {
    switch (dataType) {
      case kTypeInt16:
        return com._4paradigm.openmldb.DataType.kTypeInt16;
      case kTypeInt32:
        return com._4paradigm.openmldb.DataType.kTypeInt32;
      case kTypeInt64:
        return com._4paradigm.openmldb.DataType.kTypeInt64;
      case kTypeFloat:
        return com._4paradigm.openmldb.DataType.kTypeFloat;
      case kTypeDouble:
        return com._4paradigm.openmldb.DataType.kTypeDouble;
      case kTypeBool:
        return com._4paradigm.openmldb.DataType.kTypeBool;
      case kTypeString:
        return com._4paradigm.openmldb.DataType.kTypeString;
      case kTypeTimestamp:
        return com._4paradigm.openmldb.DataType.kTypeTimestamp;
      case kTypeDate:
        return com._4paradigm.openmldb.DataType.kTypeDate;
      default:
        return com._4paradigm.openmldb.DataType.kTypeUnknow;
    }
  }
}
