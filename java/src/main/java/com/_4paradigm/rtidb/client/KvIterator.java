package com._4paradigm.rtidb.client;

import java.nio.ByteBuffer;
import java.util.List;

import com._4paradigm.rtidb.client.schema.ColumnDesc;

public interface KvIterator {

    
    int getCount();
    List<ColumnDesc> getSchema();
	boolean valid();

    long getKey();

    String getPK();

    // no copy
    ByteBuffer getValue();
    
    Object[] getDecodedValue() throws TabletException;
    
    void getDecodedValue(Object[] row, int start, int length) throws TabletException;

    void next();
}
