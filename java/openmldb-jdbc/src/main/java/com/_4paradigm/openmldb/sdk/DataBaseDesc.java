package com._4paradigm.openmldb.sdk;

import java.util.List;

public class DataBaseDesc {

  private String dbName;
  private List<TableDesc> tableDescList;

  public DataBaseDesc() {

  }

  public String getDbName() {
    return dbName;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public List<TableDesc> getTableDescList() {
    return tableDescList;
  }

  public void setTableDescList(List<TableDesc> tableDescList) {
    this.tableDescList = tableDescList;
  }
}
