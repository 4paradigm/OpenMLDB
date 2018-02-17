package com._4paradigm.rtidb.client;

import java.util.List;
import java.util.concurrent.TimeoutException;

import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.tablet.Tablet.TTLType;
import com.google.protobuf.ByteString;

public interface TabletSyncClient {

	boolean put(int tid, int pid, String key,
		        long time, byte[] bytes) throws TimeoutException;

	boolean put(int tid, int pid, String key,
		        long time, String value) throws TimeoutException;
	
	boolean put(int tid, int pid, long time, Object[] row) throws TimeoutException,TabletException;

	ByteString get(int tid, int pid, String key) throws TimeoutException;

	ByteString get(int tid, int pid, String key, long time) throws TimeoutException;
	Object[] getRow(int tid, int pid, String key, long time) throws TimeoutException, TabletException;
	
    KvIterator scan(int tid, int pid, String key,
				    long st, long et) throws TimeoutException;
    
    KvIterator scan(int tid, int pid, String key,
		    		String idxName,
		    		long st, long et) throws TimeoutException;

    boolean createTable(String name, 
    			        int tid, 
    			        int pid, 
    			        long ttl, 
    			        int segCnt);
    
    boolean createTable(String name, 
	        int tid, 
	        int pid, 
	        long ttl, 
	        TTLType type,
	        int segCnt);
    
    boolean createTable(String name, 
	        int tid, 
	        int pid, 
	        long ttl, 
	        int segCnt,
	        List<ColumnDesc> schema);
    boolean createTable(String name, 
    		int tid, int pid, long ttl, TTLType type,
    		int segCnt, List<ColumnDesc> schema);
    boolean dropTable(int tid, int pid);
    
    
    // cluster 
    boolean put(String name, String key,
                long time, byte[] bytes) throws TimeoutException,TabletException;
    boolean put(String name, long time, Object[] row) throws TimeoutException,TabletException;
    
}
