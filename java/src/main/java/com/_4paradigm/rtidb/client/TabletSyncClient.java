package com._4paradigm.rtidb.client;

import java.util.concurrent.TimeoutException;

import com.google.protobuf.ByteString;

public interface TabletSyncClient {

	boolean put(int tid, int pid, String key,
		        long time, byte[] bytes) throws TimeoutException;

	boolean put(int tid, int pid, String key,
		        long time, String value) throws TimeoutException;

	ByteString get(int tid, int pid, String key) throws TimeoutException;

	ByteString get(int tid, int pid, String key, long time) throws TimeoutException;

    KvIterator scan(int tid, int pid, String key,
				    long st, long et) throws TimeoutException;
    
    boolean createTable(String name, 
    			        int tid, 
    			        int pid, 
    			        long ttl, 
    			        int segCnt);
    boolean dropTable(int tid, int pid);
    
}
