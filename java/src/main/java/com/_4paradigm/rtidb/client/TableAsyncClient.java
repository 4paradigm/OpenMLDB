package com._4paradigm.rtidb.client;

import java.util.Map;
import java.util.concurrent.TimeoutException;

public interface TableAsyncClient {
    
    /**
     * Put a byte[] value to partition of table
     * 
     * @param tid , the id of table
     * @param pid , the id of partition
     * @param key , the key of value
     * @param time, the time of key
     * @param bytes
     * @return
     * @throws TabletException when tid or pid is not found
     */
    PutFuture put(int tid, int pid, String key, long time, byte[] bytes) throws TabletException;

    /**
     * Put a string value which will be encode to byte[] byte UTF-8 to partition of table
     * 
     * @param tid , the id of table
     * @param pid , the id of partition
     * @param key , the key of value
     * @param time, the time of key
     * @param bytes
     * @return
     * @throws TabletException when tid or pid is not found
     */
    PutFuture put(int tid, int pid, String key, long time, String value) throws TabletException;

    PutFuture put(int tid, int pid, long time, Object[] row) throws TabletException;

    GetFuture get(int tid, int pid, String key) throws TabletException;

    GetFuture get(int tid, int pid, String key, long time) throws TabletException;

    ScanFuture scan(int tid, int pid, String key, long st, long et) throws TabletException;

    ScanFuture scan(int tid, int pid, String key, String idxName, long st, long et) throws TabletException;

    PutFuture put(String name, String key, long time, byte[] bytes) throws TabletException;

    PutFuture put(String name, String key, long time, String value) throws TabletException;

    PutFuture put(String name, long time, Object[] row) throws TabletException;

    ScanFuture scan(String name, String key, String idxName, long st, long et) throws TabletException;

    ScanFuture scan(String name, String key, long st, long et) throws TabletException;

    GetFuture get(String name, String key, long time) throws TabletException;

    GetFuture get(String name, String key) throws TabletException;
}
