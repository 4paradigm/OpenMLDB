package com._4paradigm.rtidb.client.schema;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com._4paradigm.rtidb.client.TabletException;
import com._4paradigm.rtidb.client.type.DataType;

public class SchemaCodec {

	private static Map<String, DataType> TYPE_MAPING = new HashMap<>();
	static {
		TYPE_MAPING.put("int16", DataType.SmallInt);
		TYPE_MAPING.put("int32", DataType.Int);
		TYPE_MAPING.put("int64", DataType.BigInt);
		TYPE_MAPING.put("float", DataType.Float);
		TYPE_MAPING.put("double", DataType.Double);
		TYPE_MAPING.put("string", DataType.Varchar);
		TYPE_MAPING.put("timestamp", DataType.Timestamp);
		TYPE_MAPING.put("bool", DataType.Bool);
		TYPE_MAPING.put("date", DataType.Date);
	}
	public static ByteBuffer encode(List<ColumnDesc> schema) throws TabletException{
		ByteBuffer buffer = ByteBuffer.allocate(getSize(schema)).order(ByteOrder.LITTLE_ENDIAN);
		for (ColumnDesc col : schema) {
			buffer.put((byte)col.getType().getValue());
			if (col.isAddTsIndex()) {
				buffer.put((byte)1);
			}else {
				buffer.put((byte)0);
			}
            if (col.getName().getBytes().length >128) {
                throw new TabletException("col name size is too big, which should be less than or equal 128");
            }
			buffer.put((byte)(col.getName().getBytes().length));
			buffer.put(col.getName().getBytes());
		}
		return buffer;
	}
	
	public static List<ColumnDesc> decode(ByteBuffer buffer) {
		List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
		if (buffer.order() == ByteOrder.BIG_ENDIAN) {
            buffer = buffer.order(ByteOrder.LITTLE_ENDIAN);
        }
		while (buffer.position() < buffer.limit()) {
			ColumnDesc desc = new ColumnDesc();
			ColumnType type = ColumnType.valueOf((byte)buffer.get());
			desc.setType(type);
			desc.setAddTsIndex(false);
			desc.setDataType(TYPE_MAPING.get(type));
			if ((int)buffer.get() == 1) {
				desc.setAddTsIndex(true);
			} 
			int size = buffer.get() & 0xFF ;
			byte[] nameBytes = new byte[size];
			buffer.get(nameBytes);
			desc.setName(new String(nameBytes, Charset.forName("utf-8")));
			schema.add(desc);
		}
		return schema;
	}

	private static int getSize(List<ColumnDesc> schema) {
		int totalSize = 0;
		for (int i = 0; i < schema.size(); i++) {
			totalSize += 3 + schema.get(i).getName().getBytes(Charset.forName("utf-8")).length;
		}
		return totalSize;
	}
}
