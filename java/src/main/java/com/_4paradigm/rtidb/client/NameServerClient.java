package com._4paradigm.rtidb.client;

import com._4paradigm.rtidb.client.schema.TableDesc;
import com._4paradigm.rtidb.ns.NS.TableInfo;
import com._4paradigm.rtidb.ns.NS.OPStatus;

import java.util.List;
import java.util.Map;

public interface NameServerClient {

    boolean createTable(TableDesc tableDesc);
    boolean createTable(TableInfo tableInfo);
    boolean dropTable(String tname);
    List<TableInfo> showTable(String tname);
    boolean changeLeader(String tname, int pid);
    boolean recoverEndpoint(String endpoint);
    List<String> showTablet();
    List<OPStatus> showOPStatus(String tname);

    boolean addTableField(String tableName, String columnName, String columnType);
    boolean addIndex(String tableName, String indexName, List<String> tss, Map<String, String> cols);
}
