package com._4paradigm.rtidb.client;

import java.util.List;

import com._4paradigm.rtidb.ns.NS.TableInfo;

public interface NameServerClient {
    
    boolean createTable(TableInfo tableInfo);
    boolean dropTable(String tname);
    List<TableInfo> showTable(String tname);
    boolean changeLeader(String tname, int pid);
    boolean recoverEndpoint(String endpoint);
    List<String> showTablet();
}
