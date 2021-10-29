package com._4paradigm.openmldb.sdk;

import java.util.List;

public class TableDesc {

  private String tableName;

  private List<ColumnDesc> columnDescList;

  public TableDesc() {
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public List<ColumnDesc> getColumnDescList() {
    return columnDescList;
  }

  public void setColumnDescList(List<ColumnDesc> columnDescList) {
    this.columnDescList = columnDescList;
  }
}
