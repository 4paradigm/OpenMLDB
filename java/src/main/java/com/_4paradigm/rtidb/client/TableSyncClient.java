package com._4paradigm.rtidb.client;

import java.util.Map;
import java.util.concurrent.TimeoutException;

import com.google.protobuf.ByteString;

public interface TableSyncClient {

    // for single node
    boolean put(int tid, int pid, String key, long time, byte[] bytes) throws TimeoutException, TabletException;

    boolean put(int tid, int pid, String key, long time, String value) throws TimeoutException, TabletException;

    boolean put(int tid, int pid, long time, Object[] row) throws TimeoutException, TabletException;

    ByteString get(int tid, int pid, String key) throws TimeoutException, TabletException;

    ByteString get(int tid, int pid, String key, long time) throws TimeoutException, TabletException;

    Object[] getRow(int tid, int pid, String key, long time) throws TimeoutException, TabletException;

    KvIterator scan(int tid, int pid, String key, long st, long et) throws TimeoutException, TabletException;

    KvIterator scan(int tid, int pid, String key, String idxName, long st, long et)
            throws TimeoutException, TabletException;

    // for cluster
    boolean put(String tname, String key, long time, byte[] bytes) throws TimeoutException, TabletException;

    boolean put(String tname, String key, long time, String value) throws TimeoutException, TabletException;

    boolean put(String tname, long time, Object[] row) throws TimeoutException, TabletException;


    ByteString get(String tname, String key) throws TimeoutException, TabletException;

    ByteString get(String tname, String key, long time) throws TimeoutException, TabletException;

    Object[] getRow(String tname, String key, long time) throws TimeoutException, TabletException;

    KvIterator scan(String tname, String key, long st, long et) throws TimeoutException, TabletException;

    KvIterator scan(String tname, String key, String idxName, long st, long et)
            throws TimeoutException, TabletException;

}
