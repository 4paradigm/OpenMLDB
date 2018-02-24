package com._4paradigm.rtidb.client;

public interface TableAsyncClient {
    PutFuture put(int tid, int pid, String key, long time, byte[] bytes);

    PutFuture put(int tid, int pid, String key, long time, String value);
    
    PutFuture put(int tid, int pid, long time, Object[] row) throws TabletException;

    GetFuture get(int tid, int pid, String key);

    GetFuture get(int tid, int pid, String key, long time);

    ScanFuture scan(int tid, int pid, String key, long st, long et);

    ScanFuture scan(int tid, int pid, String key, String idxName, long st, long et);

    PutFuture put(String name, String key, long time, byte[] bytes);

    ScanFuture scan(String name, String key, String idxName, long st, long et);

    ScanFuture scan(String name, String key, long st, long et);

    GetFuture get(String name, String key, long time);

    GetFuture get(String name, String key);
}
