package com._4paradigm.openmldb.sdk.impl;

import com._4paradigm.openmldb.common.codec.CodecMetaData;
import com._4paradigm.openmldb.common.codec.RowView;
import com._4paradigm.openmldb.jdbc.DirectResultSet;
import com._4paradigm.openmldb.sdk.Schema;

import java.nio.ByteBuffer;
import java.sql.SQLException;

public class CallableDirectResultSet extends DirectResultSet {
    private int position = 0;

    public CallableDirectResultSet(ByteBuffer buf, int totalRows, Schema schema, CodecMetaData metaData) {
        super(buf, totalRows, schema);
        rowView = new RowView(metaData);
    }

    @Override
    public boolean next() throws SQLException {
        if (closed) {
            return false;
        }
        if (rowNum < totalRows && position < buf.capacity()) {
            buf.position(position);
            int rowLength = buf.getInt(position + 2);
            position += rowLength;
            if (position > buf.capacity()) {
                return false;
            }
            rowNum++;
            return rowView.reset(buf.slice(), rowLength);
        }
        return false;
    }
}
