package com._4paradigm.rtidb.client.schema;

import java.nio.ByteBuffer;
import java.util.List;

public class SchemaCodec {
	
	public static ByteBuffer encode(List<ColumnDesc> schema) {
		ByteBuffer buffer = ByteBuffer.allocate(getSize(schema));
		for (ColumnDesc col : schema) {
			buffer.put((byte)col.getType().getValue());
			if (col.isAddTsIndex()) {
				buffer.put((byte)1);
			}else {
				buffer.put((byte)0);
			}
			buffer.put((byte)(col.getName().getBytes().length));
			buffer.put(col.getName().getBytes());
		}
		return buffer;
	}

	private static int getSize(List<ColumnDesc> schema) {
		int totalSize = 0;
		for (int i = 0; i < schema.size(); i++) {
			totalSize += 3 + schema.get(i).getName().getBytes().length;
		}
		return totalSize;
	}
}
