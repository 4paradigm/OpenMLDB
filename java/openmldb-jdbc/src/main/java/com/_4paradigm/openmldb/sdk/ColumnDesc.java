package com._4paradigm.openmldb.sdk;

public class ColumnDesc {

  private String columnName;
  private DataType columnType;

  public ColumnDesc() {
  }

  public String getColumnName() {
    return columnName;
  }

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  public DataType getColumnType() {
    return columnType;
  }

  public void setColumnType(DataType columnType) {
    this.columnType = columnType;
  }
}
