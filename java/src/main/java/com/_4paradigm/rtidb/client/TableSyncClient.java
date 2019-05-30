package com._4paradigm.rtidb.client;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.tablet.Tablet;
import com.google.protobuf.ByteString;

public interface TableSyncClient {

    // for single node
    boolean put(int tid, int pid, String key, long time, byte[] bytes) throws TimeoutException, TabletException;

    boolean put(int tid, int pid, String key, long time, String value) throws TimeoutException, TabletException;

    boolean put(int tid, int pid, long time, Object[] row) throws TimeoutException, TabletException;
    boolean put(int tid, int pid, long time, Map<String, Object> row) throws TimeoutException, TabletException;
    List<ColumnDesc> getSchema(int tid) throws TabletException;
    ByteString get(int tid, int pid, String key) throws TimeoutException, TabletException;

    ByteString get(int tid, int pid, String key, long time) throws TimeoutException, TabletException;

    Object[] getRow(int tid, int pid, String key, long time) throws TimeoutException, TabletException;

    Object[] getRow(int tid, int pid, String key, String idxName, long time) throws TimeoutException, TabletException;

    Object[] getRow(int tid, int pid, String key, String idxName) throws TimeoutException, TabletException;

    KvIterator scan(int tid, int pid, String key, long st, long et) throws TimeoutException, TabletException;

    KvIterator scan(int tid, int pid, String key, int limit) throws TimeoutException, TabletException;

    KvIterator scan(int tid, int pid, String key, long st, long et, int limit) throws TimeoutException, TabletException;

    KvIterator scan(int tid, int pid, String key, String idxName, long st, long et)
            throws TimeoutException, TabletException;

    KvIterator scan(int tid, int pid, String key, String idxName, long st, long et, int limit)
            throws TimeoutException, TabletException;

    KvIterator scan(int tid, int pid, String key, String idxName, int limit)
            throws TimeoutException, TabletException;

    KvIterator scan(int tid, int pid, Object[] row, String idxName, long st, long et, String tsName)
            throws TimeoutException, TabletException;

    KvIterator scan(int tid, int pid, Map<String, Object> keyMap, String idxName, long st, long et, String tsName, int limit)
            throws TimeoutException, TabletException;

    // for cluster
    boolean put(String tname, String key, long time, byte[] bytes) throws TimeoutException, TabletException;

    boolean put(String tname, String key, long time, String value) throws TimeoutException, TabletException;

    boolean put(String tname, long time, Object[] row) throws TimeoutException, TabletException;
    boolean put(String tname, Object[] row) throws TimeoutException, TabletException;
    boolean put(String tname, long time, Map<String, Object> row) throws TimeoutException, TabletException;
    boolean put(String tname, Map<String, Object> row) throws TimeoutException, TabletException;
    List<ColumnDesc> getSchema(String tname) throws TabletException;
    ByteString get(String tname, String key) throws TimeoutException, TabletException;

    ByteString get(String tname, String key, long time) throws TimeoutException, TabletException;

    Object[] getRow(String tname, String key, long time) throws TimeoutException, TabletException;
    Object[] getRow(String tname, String key, long time, Tablet.GetType type) throws TimeoutException, TabletException;

    Object[] getRow(String tname, String key, String idxName) throws TimeoutException, TabletException;

    Object[] getRow(String tname, String key, String idxName, long time) throws TimeoutException, TabletException;

    Object[] getRow(String tname, String key, String idxName, long time, Tablet.GetType type) throws TimeoutException, TabletException;

    Object[] getRow(String tname, String key, String idxName, long time, String tsName, Tablet.GetType type) throws TimeoutException, TabletException;

    Object[] getRow(String tname, Object[] keyArr, String idxName, long time, String tsName, Tablet.GetType type) throws TimeoutException, TabletException;
    Object[] getRow(String tname, Map<String, Object> keyMap, String idxName, long time, String tsName, Tablet.GetType type) throws TimeoutException, TabletException;

    KvIterator scan(String tname, String key, long st, long et) throws TimeoutException, TabletException;

    KvIterator scan(String tname, String key, int limit) throws TimeoutException, TabletException;

    KvIterator scan(String tname, String key, long st, long et, int limit) throws TimeoutException, TabletException;

    KvIterator scan(String tname, String key, String idxName, long st, long et)
            throws TimeoutException, TabletException;

    KvIterator scan(String tname, String key, String idxName, long st, long et, int limit)
            throws TimeoutException, TabletException;

    KvIterator scan(String tname, String key, String idxName, int limit)
            throws TimeoutException, TabletException;

    KvIterator scan(String tname, String key, String idxName, long st, long et, String tsName, int limit)
            throws TimeoutException, TabletException;

    KvIterator scan(String tname, Object[] keyArr, String idxName, long st, long et, String tsName)
            throws TimeoutException, TabletException;

    KvIterator scan(String tname, Map<String, Object> keyMap, String idxName, long st, long et, String tsName)
            throws TimeoutException, TabletException;

    KvIterator scan(String tname, Map<String, Object> keyMap, String idxName, long st, long et, String tsName, int limit)
            throws TimeoutException, TabletException;

    KvIterator traverse(String tname, String idxName) throws TimeoutException, TabletException;
    KvIterator traverse(String tname) throws TimeoutException, TabletException;
    KvIterator traverse(String tname, String idxName, String tsName) throws TimeoutException, TabletException;

    int count(String tname, String key) throws TimeoutException, TabletException;
    int count(String tname, String key, boolean filter_expired_data) throws TimeoutException, TabletException;
    int count(String tname, String key, String idxName) throws TimeoutException, TabletException;
    int count(String tname, String key, String idxName, boolean filter_expired_data) throws TimeoutException, TabletException;
    int count(String tname, String key, String idxName, String tsName, boolean filter_expired_data) throws TimeoutException, TabletException;
    int count(String tname, Map<String, Object> keyMap, String idxName, String tsName, boolean filter_expired_data) throws TimeoutException, TabletException;
    int count(String tname, Object[] keyArr, String idxName, String tsName, boolean filter_expired_data) throws TimeoutException, TabletException;

    int count(int tid, int pid, String key, boolean filter_expired_data) throws TimeoutException, TabletException;
    int count(int tid, int pid, String key, String idxName, boolean filter_expired_data) throws TimeoutException, TabletException;

    boolean delete(String tname, String key) throws TimeoutException, TabletException;
    boolean delete(String tname, String key, String idxName) throws TimeoutException, TabletException;
    boolean delete(int tid, int pid, String key) throws TimeoutException, TabletException;
    boolean delete(int tid, int pid, String key, String idxName) throws TimeoutException, TabletException;

}
