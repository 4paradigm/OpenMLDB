package com._4paradigm.rtidb.client.impl;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.SchemaCodec;
import com._4paradigm.rtidb.client.schema.Table;

import rtidb.api.Tablet;
import rtidb.api.TabletServer;

public class GTableSchema {

	public final static ConcurrentHashMap<Integer, Table> tableSchema = new ConcurrentHashMap<Integer, Table>();
	
	public static Table getTable(int tid, int pid) {
	    Table table = tableSchema.get(tid);
	    return table;
	}
	
	public static Table getTable(int tid, int pid, TabletServer tablet) {
		Table table = tableSchema.get(tid);
		if (table != null) {
			return table;
		}
		Tablet.GetTableSchemaRequest request = Tablet.GetTableSchemaRequest.newBuilder().setTid(tid).setPid(pid)
				.build();
		Tablet.GetTableSchemaResponse response = tablet.getTableSchema(request);
		if (response.getCode() == 0) {
			List<ColumnDesc> schema = SchemaCodec.decode(response.getSchema().asReadOnlyByteBuffer());
			table = new Table(schema);
			tableSchema.put(tid, table);
			return table;

		} else if (response.getCode() == -1) {
			// some table maybe have no schema, eg kv table
			table = new Table();
			table.setTid(tid);
			tableSchema.put(tid, table);
			return table;
		}
		return null;
	}
}
