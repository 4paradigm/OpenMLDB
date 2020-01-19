package com._4paradigm.rtidb.client;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.tablet.Tablet;

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
     * @param value
     * @return
     * @throws TabletException when tid or pid is not found
     */
    PutFuture put(int tid, int pid, String key, long time, String value) throws TabletException;

    PutFuture put(int tid, int pid, long time, Object[] row) throws TabletException;
    PutFuture put(int tid, int pid, long time, Map<String, Object> row) throws TabletException;
    List<ColumnDesc> getSchema(int tid) throws TabletException;
    GetFuture get(int tid, int pid, String key) throws TabletException;
    GetFuture get(int tid, int pid, String key, String idxName) throws TabletException;

    GetFuture get(int tid, int pid, String key, String idxName, String tsName,
                  long st, Tablet.GetType stType, long et, Tablet.GetType etType) throws TabletException;
    GetFuture get(int tid, int pid, String key, long time) throws TabletException;

    GetFuture get(int tid, int pid, String key, String idxName, long time) throws TabletException;

    ScanFuture scan(int tid, int pid, String key, long st, long et) throws TabletException;

    /**
     * Scan
     *
     * @param tid , the id of table
     * @param pid , the id of partition
     * @param key , the key of value
     * @param st, the start time or version
     * @param et, the end time or version
     * @param limit, the max number of return records
     * @return
     * @throws TabletException when tid or pid is not found
     */
    ScanFuture scan(int tid, int pid, String key, long st, long et, int limit) throws TabletException;

    /**
     * Scan the latest N records
     *
     * @param tid , the id of table
     * @param pid , the id of partition
     * @param key , the key of value
     * @param limit, the max number of return records
     * @return
     * @throws TabletException when tid or pid is not found
     */
    ScanFuture scan(int tid, int pid, String key, int limit) throws TabletException;

    ScanFuture scan(int tid, int pid, String key, String idxName, long st, long et) throws TabletException;

    ScanFuture scan(int tid, int pid, String key, String idxName, int limit) throws TabletException;

    ScanFuture scan(int tid, int pid, String key, String idxName, long st, long et, int limit) throws TabletException;

    PutFuture put(String name, String key, long time, byte[] bytes) throws TabletException;

    PutFuture put(String name, String key, long time, String value) throws TabletException;

    PutFuture put(String name, long time, Object[] row) throws TabletException;
    PutFuture put(String name, Object[] row) throws TabletException;

    PutFuture put(String name, long time, Map<String, Object> row) throws TabletException;
    PutFuture put(String name, Map<String, Object> row) throws TabletException;
    List<ColumnDesc> getSchema(String tname) throws TabletException;
    ScanFuture scan(String name, String key, String idxName, long st, long et) throws TabletException;

    ScanFuture scan(String name, String key, String idxName, int limit) throws TabletException;

    ScanFuture scan(String name, String key, String idxName, long st, long et, int limit) throws TabletException;

    ScanFuture scan(String name, String key, long st, long et) throws TabletException;

    ScanFuture scan(String name, String key, int limit) throws TabletException;

    ScanFuture scan(String name, String key, long st, long et, int limit) throws TabletException;

    ScanFuture scan(String tname, Map<String, Object> keyMap,long st, long et,
                    ScanOption option) throws TabletException;
    ScanFuture scan(String tname, String key, long st, long et,
                    ScanOption option) throws TabletException;
    ScanFuture scan(String tname, Object[] keyArr, long st, long et, ScanOption option)
            throws TabletException;

    ScanFuture scan(String name, String key, String idxName, long st, long et, String tsName, int limit) throws TabletException;
    ScanFuture scan(String name, Object[] keyArr, String idxName, long st, long et, String tsName, int limit) throws TabletException;
    ScanFuture scan(String name, Map<String, Object> keyMap, String idxName, long st, long et, String tsName, int limit) throws TabletException;

    GetFuture get(String name, String key, long time) throws TabletException;
    GetFuture get(String name, String key, long time, Tablet.GetType type) throws TabletException;

    GetFuture get(String name, String key, String idxName, long time) throws TabletException;
    GetFuture get(String name, String key, String idxName, long time, Tablet.GetType type) throws TabletException;
    GetFuture get(String name, String key, String idxName, long time, String tsName, Tablet.GetType type) throws TabletException;
    GetFuture get(String name, String key, String idxName, long time, String tsName, Tablet.GetType type,
                  long et, Tablet.GetType etType) throws TabletException;
    GetFuture get(String name, Object[] keyArr, String idxName, long time, String tsName, Tablet.GetType type,
                  long et, Tablet.GetType etType) throws TabletException;
    GetFuture get(String name, Object[] keyArr, String idxName, long time, String tsName, Tablet.GetType type) throws TabletException;
    GetFuture get(String name, Map<String, Object> keyMap, String idxName, long time, String tsName, Tablet.GetType type,
                  long et, Tablet.GetType etType) throws TabletException;
    GetFuture get(String name, Map<String, Object> keyMap, String idxName, long time, String tsName, Tablet.GetType type) throws TabletException;
    GetFuture get(String name, String key) throws TabletException;

    GetFuture get(String name, String key, String idxName) throws TabletException;
}