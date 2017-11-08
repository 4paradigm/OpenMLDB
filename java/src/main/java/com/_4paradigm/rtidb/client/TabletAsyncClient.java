package com._4paradigm.rtidb.client;

public interface TabletAsyncClient {

    PutFuture put(int tid, int pid, String key,
    		      long time, byte[] bytes);
    
    PutFuture put(int tid, int pid, String key,
    		      long time, String value);
    
    GetFuture get(int tid, int pid, String key);
    
    GetFuture get(int tid, int pid, String key, long time);
    
    ScanFuture scan(int tid, int pid, String key,
    				long st, long et);
}
