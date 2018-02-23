package com._4paradigm.rtidb.client;

import com._4paradigm.rtidb.ns.NS.TableInfo;

public interface NameServerClient {
    
    boolean createTable(TableInfo tableInfo);
    boolean dropTable(String tname);
    
}
