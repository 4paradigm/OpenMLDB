package com._4paradigm.rtidb.client;

public interface TableAsyncClient {
    PutFuture put(int tid, int pid, String key, long time, byte[] bytes) throws TabletException;

    PutFuture put(int tid, int pid, String key, long time, String value)throws TabletException;
    
    PutFuture put(int tid, int pid, long time, Object[] row) throws TabletException;

    GetFuture get(int tid, int pid, String key)throws TabletException;

    GetFuture get(int tid, int pid, String key, long time)throws TabletException;

    ScanFuture scan(int tid, int pid, String key, long st, long et)throws TabletException;

    ScanFuture scan(int tid, int pid, String key, String idxName, long st, long et)throws TabletException;

    PutFuture put(String name, String key, long time, byte[] bytes)throws TabletException;
    PutFuture put(String name, String key, long time, String value)throws TabletException;
    PutFuture put(String name, long time, Object[] row) throws TabletException;
    ScanFuture scan(String name, String key, String idxName, long st, long et)throws TabletException;

    ScanFuture scan(String name, String key, long st, long et)throws TabletException;

    GetFuture get(String name, String key, long time)throws TabletException;

    GetFuture get(String name, String key)throws TabletException;
}
