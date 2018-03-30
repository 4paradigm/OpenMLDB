package com._4paradigm.rtidb.client;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.metrics.TabletMetrics;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.RowCodec;
import com.google.protobuf.ByteString;

public interface KvIterator {

    
    int getCount();
    List<ColumnDesc> getSchema();
	boolean valid();

    long getKey();

    // no copy
    ByteBuffer getValue();
    
    Object[] getDecodedValue() throws TabletException;

    void next(); 
}
