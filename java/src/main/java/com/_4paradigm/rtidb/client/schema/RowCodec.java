package com._4paradigm.rtidb.client.schema;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import com._4paradigm.rtidb.client.TabletException;

public class RowCodec {

	public static ByteBuffer encode(Object[] row, List<ColumnDesc> schema) throws TabletException {
		if (row.length  != schema.size()) {
			throw new TabletException("row length mismatch schema");
		}
		List<byte[]> cache = new ArrayList<byte[]>(row.length);
		//TODO limit the max size
		int size = getSize(row, schema, cache);
		ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
		buffer.put((byte)row.length);
		for (int i = 0; i < row.length; i++) {
			buffer.put((byte)schema.get(i).getType().getValue());
			if (row[i] == null) {
				buffer.put((byte)0);
				continue;
			}
			switch (schema.get(i).getType()) {
			case kString:
				byte[] bytes = cache.get(i);
				buffer.put((byte)bytes.length);
				buffer.put(bytes);
				break;
			case kInt32:
				buffer.put((byte)4);
				buffer.putInt((Integer)row[i]);
				break;
			case kUInt32:
				throw new TabletException("kUInt32 is not support on jvm platform");
			case kFloat:
				buffer.put((byte)4);
				buffer.putFloat((Float)row[i]);
				break;
			case kInt64:
				buffer.put((byte)8);
				buffer.putLong((Long)row[i]);
				break;
			case kUInt64:
				throw new TabletException("kUInt64 is not support on jvm platform");
			case kDouble:
				buffer.put((byte)8);
				buffer.putDouble((Double)row[i]);
				break;
			default:
				break;
			}
		}
		return buffer;
	}
	
	public static Object[] decode(ByteBuffer buffer, List<ColumnDesc> schema) throws TabletException {
		if (buffer.order() == ByteOrder.BIG_ENDIAN) {
            buffer = buffer.order(ByteOrder.LITTLE_ENDIAN);
        }
		byte length = buffer.get();
        Object[] row = new Object[(int)length]; 
        int index = 0;
        while (buffer.position() < buffer.limit()
        		&& index < row.length) {
        	byte type = buffer.get();
            byte size = buffer.get();
            if (size == 0) {
            	row[index] = null;
            	index ++;
            	continue;
            }
            ColumnType ctype = ColumnType.valueOf((int)type);
            switch (ctype) {
			case kString:
				byte[] inner = new byte[size];
                buffer.get(inner);
                String val = new String(inner, Charset.forName("utf-8"));
                row[index] = val;
				break;
			case kInt32:
				row[index] = buffer.getInt();
				break;
			case kInt64:
				row[index] = buffer.getLong();
				break;
			case kDouble:
				row[index] = buffer.getDouble();
				break;
			case kFloat:
				row[index] = buffer.getFloat();
				break;
			default:
				throw new TabletException(ctype.toString() + " is not support on jvm platform");
			}
            index ++;
            
        }
        return row;
	}
	
	
	private static int getSize(Object[] row, List<ColumnDesc> schema, List<byte[]> cache) {
		int totalSize = 1;
		for (int i = 0; i < row.length; i++) {
			totalSize += 2;
			if (row[i] == null) {
				continue;
			}
			switch (schema.get(i).getType()) {
			case kString:
				byte[] bytes = ((String)row[i]).getBytes(Charset.forName("utf-8"));
				cache.add(i, bytes);
				totalSize += bytes.length;
				break;
			case kInt32:
			case kUInt32:
			case kFloat:
				totalSize += 4;
				break;
			case kInt64:
			case kUInt64:
			case kDouble:
				totalSize += 8;
				break;
			default:
				break;
			}
		}
		return totalSize;
	} 
}
