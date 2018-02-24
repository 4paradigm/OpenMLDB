package com._4paradigm.rtidb.client;

@Deprecated
public interface TabletAsyncClient {

    PutFuture put(int tid, int pid, String key,
            long time, byte[] bytes);
    
    PutFuture put(int tid, int pid, String key,
    		      long time, String value);
    
    GetFuture get(int tid, int pid, String key);
    
    GetFuture get(int tid, int pid, String key, long time);
    
    ScanFuture scan(int tid, int pid, String key,
    				long st, long et);
    
    ScanFuture scan(int tid, int pid, String key,
    				String idxName, long st, long et);
    
    PutFuture put(String name, String key,
            long time, byte[] bytes);
    
}
