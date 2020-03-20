package com._4paradigm.rtidb.client;

import com._4paradigm.rtidb.client.schema.TableDesc;
import com._4paradigm.rtidb.ns.NS.TableInfo;

import java.util.List;

public interface NameServerClient {

    boolean createTable(TableDesc tableDesc) throws TabletException;
    boolean createTable(TableInfo tableInfo);
    boolean dropTable(String tname);
    List<TableInfo> showTable(String tname);
    boolean changeLeader(String tname, int pid);
    boolean recoverEndpoint(String endpoint);
    List<String> showTablet();

    boolean addTableField(String tableName, String columnName, String columnType);
}
