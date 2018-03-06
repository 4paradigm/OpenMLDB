package com._4paradigm.rtidb.client.ha;

public interface RTIDBClient {

    TableHandler getHandler(String name);
    
    TableHandler getHandler(int tid);
    
    RTIDBClientConfig getConfig();
    
    void close();
}
