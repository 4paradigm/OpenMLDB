package com._4paradigm.openmldb.sdk;

public class CreateTableDesc {

  private String dbName;
  private String[] ddl;

  public CreateTableDesc() {

  }

  public String getDbName() {
    return dbName;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public String[] getDdl() {
    return ddl;
  }

  public void setDdl(String[] ddl) {
    this.ddl = ddl;
  }
}
