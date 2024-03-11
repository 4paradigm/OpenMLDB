package com._4paradigm.openmldb.mysql.util;

import com._4paradigm.openmldb.DataType;
import com._4paradigm.openmldb.jdbc.SQLResultSet;
import com._4paradigm.openmldb.sdk.Common;
import java.sql.SQLException;
import java.sql.Types;

public class TypeUtil {
  public static String javaSqlTypeToString(int sqlType) {
    switch (sqlType) {
      case Types.BIT:
        return "BIT";
      case Types.TINYINT:
        return "TINYINT";
      case Types.SMALLINT:
        return "SMALLINT";
      case Types.INTEGER:
        return "INTEGER";
      case Types.BIGINT:
        return "BIGINT";
      case Types.FLOAT:
        return "FLOAT";
      case Types.REAL:
        return "REAL";
      case Types.DOUBLE:
        return "DOUBLE";
      case Types.NUMERIC:
        return "NUMERIC";
      case Types.DECIMAL:
        return "DECIMAL";
      case Types.CHAR:
        return "CHAR";
      case Types.VARCHAR:
        return "VARCHAR";
      case Types.LONGVARCHAR:
        return "LONGVARCHAR";
      case Types.DATE:
        return "DATE";
      case Types.TIME:
        return "TIME";
      case Types.TIMESTAMP:
        return "TIMESTAMP";
      case Types.BINARY:
        return "BINARY";
      case Types.VARBINARY:
        return "VARBINARY";
      case Types.LONGVARBINARY:
        return "LONGVARBINARY";
      case Types.NULL:
        return "NULL";
      case Types.OTHER:
        return "OTHER";
      case Types.JAVA_OBJECT:
        return "JAVA_OBJECT";
      case Types.DISTINCT:
        return "DISTINCT";
      case Types.STRUCT:
        return "STRUCT";
      case Types.ARRAY:
        return "ARRAY";
      case Types.BLOB:
        return "BLOB";
      case Types.CLOB:
        return "CLOB";
      case Types.REF:
        return "REF";
      case Types.DATALINK:
        return "DATALINK";
      case Types.BOOLEAN:
        return "BOOLEAN";
      case Types.ROWID:
        return "ROWID";
      case Types.NCHAR:
        return "NCHAR";
      case Types.NVARCHAR:
        return "NVARCHAR";
      case Types.LONGNVARCHAR:
        return "LONGNVARCHAR";
      case Types.NCLOB:
        return "NCLOB";
      case Types.SQLXML:
        return "SQLXML";
      default:
        return "Unknown";
    }
  }

  public static String javaSqlTypeToDemoData(int sqlType) {
    switch (sqlType) {
      case Types.BIT:
      case Types.TINYINT:
      case Types.SMALLINT:
      case Types.INTEGER:
      case Types.BIGINT:
        return "1";
      case Types.FLOAT:
      case Types.REAL:
      case Types.DOUBLE:
      case Types.NUMERIC:
      case Types.DECIMAL:
        return "1.01";
      case Types.CHAR:
      case Types.VARCHAR:
      case Types.LONGVARCHAR:
        return "\"foo\"";
      case Types.DATE:
        return "\"2021-05-20\"";
      case Types.TIME:
      case Types.TIMESTAMP:
        return "1635247427000";
      case Types.NULL:
        return "NULL";
      case Types.BOOLEAN:
        return "true";
      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
      case Types.OTHER:
      case Types.JAVA_OBJECT:
      case Types.DISTINCT:
      case Types.STRUCT:
      case Types.ARRAY:
      case Types.BLOB:
      case Types.CLOB:
      case Types.REF:
      case Types.DATALINK:
      case Types.ROWID:
      case Types.NCHAR:
      case Types.NVARCHAR:
      case Types.LONGNVARCHAR:
      case Types.NCLOB:
      case Types.SQLXML:
      default:
        return "Unknown";
    }
  }

  public static String getResultSetStringColumn(SQLResultSet resultSet, int i, DataType type)
      throws SQLException {
    if (type.equals(DataType.kTypeBool)) {
      if (resultSet.getBoolean(i)) {
        return "1";
      }
      return "0";
    } else if (type.equals(DataType.kTypeInt16)) {
      return String.valueOf(resultSet.getShort(i));
    } else if (type.equals(DataType.kTypeInt32)) {
      return String.valueOf(resultSet.getInt(i));
    } else if (type.equals(DataType.kTypeInt64)) {
      return String.valueOf(resultSet.getLong(i));
    } else if (type.equals(DataType.kTypeFloat)) {
      return String.valueOf(resultSet.getFloat(i));
    } else if (type.equals(DataType.kTypeDouble)) {
      return String.valueOf(resultSet.getDouble(i));
    } else if (type.equals(DataType.kTypeString)) {
      if (resultSet.getString(i) == null) {
        return null;
      }
      return String.valueOf(resultSet.getString(i));
    } else if (type.equals(DataType.kTypeDate)) {
      if (resultSet.getDate(i) == null) {
        return null;
      }
      return String.valueOf(resultSet.getDate(i));
    } else if (type.equals(DataType.kTypeTimestamp)) {
      if (resultSet.getTimestamp(i) == null) {
        return null;
      }
      return String.valueOf(resultSet.getTimestamp(i));
    } else if (type.equals(DataType.kTypeUnknow)) {
      throw new SQLException("Not support type for " + type);
    } else {
      throw new SQLException("Not support type for " + type);
    }
  }

  public static String getResultSetStringColumn(SQLResultSet resultSet, int i, int type)
      throws SQLException {
    DataType dataType = Common.sqlTypeToDataType(type);
    return getResultSetStringColumn(resultSet, i, dataType);
  }

  // TODO: Need to compare with mysql type and test for all types
  public static String openmldbTypeToMysqlTypeString(int sqlType) throws SQLException {
    switch (sqlType) {
      case Types.BOOLEAN:
        return "TINYINT(1)";
      case Types.SMALLINT:
        return "TINYINT";
      case Types.INTEGER:
        return "INT";
      case Types.BIGINT:
        return "BIGINT";
      case Types.FLOAT:
        return "FLOAT";
      case Types.DOUBLE:
        return "DOUBLE";
      case Types.VARCHAR:
        return "VARCHAR(255)";
      case Types.DATE:
        return "DATE";
      case Types.TIMESTAMP:
        return "TIMESTAMP";
      default:
        throw new SQLException("Unexpected Values: " + sqlType);
    }
  }
}
