package com._4paradigm.rtidb.client.impl;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com._4paradigm.rtidb.client.KvIterator;
import com._4paradigm.rtidb.client.TabletException;
import com._4paradigm.rtidb.client.TabletSyncClient;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.RowCodec;
import com._4paradigm.rtidb.client.schema.SchemaCodec;
import com._4paradigm.rtidb.client.schema.Table;
import com.google.protobuf.ByteString;

import rtidb.api.Tablet;
import rtidb.api.Tablet.TTLType;
import rtidb.api.TabletServer;

public class TabletSyncClientImpl implements TabletSyncClient {
	private final static Logger logger = LoggerFactory.getLogger(TabletSyncClientImpl.class);
	private TabletServer tabletServer;
    private final static int KEEP_LATEST_MAX_NUM = 1000;
	public TabletSyncClientImpl(TabletServer tabletServer) {
		this.tabletServer = tabletServer;
	}

	@Override
	public boolean put(int tid, int pid, String key, long time, byte[] bytes) throws TimeoutException {
		Tablet.PutRequest request = Tablet.PutRequest.newBuilder().setPid(pid).setPk(key).setTid(tid).setTime(time)
				.setValue(ByteString.copyFrom(bytes)).build();
		Tablet.PutResponse response = tabletServer.put(request);
		if (response != null && response.getCode() == 0) {
			return true;
		}
		return false;
	}

	@Override
	public boolean put(int tid, int pid, String key, long time, String value) throws TimeoutException {
		return put(tid, pid, key, time, value.getBytes(Charset.forName("utf-8")));
	}

	@Override
	public ByteString get(int tid, int pid, String key) throws TimeoutException {
		return get(tid, pid, key, 0l);
	}

	@Override
	public ByteString get(int tid, int pid, String key, long time) throws TimeoutException {
		Tablet.GetRequest request = Tablet.GetRequest.newBuilder().setPid(pid).setTid(tid).setKey(key).setTs(time)
				.build();
		Tablet.GetResponse response = tabletServer.get(request);
		if (response != null && response.getCode() == 0) {
			return response.getValue();
		}
		return null;
	}

	@Override
	public KvIterator scan(int tid, int pid, String key, long st, long et) throws TimeoutException {
		return scan(tid, pid, key, null, st, et);
	}

	@Override
	public boolean createTable(String name, int tid, int pid, long ttl, int segCnt) {
		return createTable(name, tid, pid, ttl, segCnt, null);
	}

	@Override
	public boolean dropTable(int tid, int pid) {
		Tablet.DropTableRequest.Builder builder = Tablet.DropTableRequest.newBuilder();
		builder.setTid(tid);
		builder.setPid(pid);
		Tablet.DropTableRequest request = builder.build();
		Tablet.DropTableResponse response = tabletServer.dropTable(request);
		if (response != null && response.getCode() == 0) {
			return true;
		}
		return false;
	}

	@Override
	public boolean createTable(String name, int tid, int pid, long ttl, 
			                   int segCnt, List<ColumnDesc> schema) {
		return createTable(name, tid, pid, ttl, null, segCnt, schema);
	}

	@Override
	public boolean put(int tid, int pid, long ts, Object[] row) throws TimeoutException, TabletException {
		Table table = getTable(tid, pid);
		Tablet.PutRequest.Builder builder = Tablet.PutRequest.newBuilder();
		ByteBuffer buffer = RowCodec.encode(row, table.getSchema());
		int index = 0;
		for (int i = 0; i < table.getSchema().size(); i++) {
			if (table.getSchema().get(i).isAddTsIndex()) {
				if (row[i] == null) {
					throw new TabletException("index " + index + "column is empty");
				}
				String value = row[i].toString();
				if (value.isEmpty()) {
					throw new TabletException("index" + index + " column is empty");
				}
				Tablet.Dimension dim = Tablet.Dimension.newBuilder().setIdx(index).setKey(value).build();
				builder.addDimensions(dim);
				index++;
			}
		}
		builder.setPid(pid);
		builder.setTid(tid);
		builder.setTime(ts);
		//TODO reduce memory copy
		builder.setValue(ByteString.copyFrom(buffer.array()));
		Tablet.PutRequest request = builder.build();
		Tablet.PutResponse response = tabletServer.put(request);
		if (response != null && response.getCode() == 0) {
			return true;
		}
		return false;
	}

	@Override
	public Table getTable(int tid, int pid) {
		Table table = GTableSchema.tableSchema.get(tid);
		if (table != null) {
			return table;
		}
		Tablet.GetTableSchemaRequest request = Tablet.GetTableSchemaRequest.newBuilder().setTid(tid).setPid(pid).build();
		Tablet.GetTableSchemaResponse response = tabletServer.getTableSchema(request);
		if (response.getCode() == 0) {
			List<ColumnDesc> schema = SchemaCodec.decode(response.getSchema().asReadOnlyByteBuffer());
			table = new Table(schema);
			GTableSchema.tableSchema.put(tid, table);
			return table;
		}
		return null;
	}
	
	
	@Override
	public KvIterator scan(int tid, int pid, String key, String idxName, long st, long et) throws TimeoutException {
		Table table = getTable(tid, pid);
		Tablet.ScanRequest.Builder builder = Tablet.ScanRequest.newBuilder();
		builder.setPk(key);
		builder.setTid(tid);
		builder.setEt(et);
		builder.setSt(st);
		builder.setPid(pid);
		if (idxName != null && !idxName.isEmpty()) {
			builder.setIdxName(idxName);
		}
		Tablet.ScanRequest request = builder.build();
		Tablet.ScanResponse response = tabletServer.scan(request);
		if (response != null && response.getCode() == 0) {
			return new KvIterator(response.getPairs(), table.getSchema());
		}
		return null;
	}

	@Override
	public boolean createTable(String name, int tid, int pid, long ttl, TTLType type, int segCnt) {
		
		return createTable(name, tid, pid, ttl, type, segCnt, null);
	}

	@Override
	public boolean createTable(String name, int tid, int pid, long ttl, TTLType type, int segCnt,
			List<ColumnDesc> schema) {
		if (null == name || "".equals(name.trim())) {
			return false;
		}
        if (type == TTLType.kLatestTime && (ttl > KEEP_LATEST_MAX_NUM || ttl <= 0)) {
            return false;
        }
		Tablet.TableMeta.Builder builder = Tablet.TableMeta.newBuilder();
        Set<String> usedColumnName = new HashSet<String>();
		if (schema != null && schema.size() > 0) {
			for (ColumnDesc desc : schema) {
				if (null == desc.getName() 
					||"".equals(desc.getName().trim())) {
					return false;
				}
                if (usedColumnName.contains(desc.getName())) {
                    return false;
                }
                usedColumnName.add(desc.getName());
				if (desc.isAddTsIndex()) {
					builder.addDimensions(desc.getName());
				}
			}
			ByteBuffer buffer = SchemaCodec.encode(schema);
			builder.setSchema(ByteString.copyFrom(buffer.array()));
			
		}
		if (type != null) {
			builder.setTtlType(type);
		}
		builder.setName(name).setTid(tid).setPid(pid).setTtl(ttl).setSegCnt(segCnt);
		Tablet.TableMeta meta = builder.build();
		Tablet.CreateTableRequest request = Tablet.CreateTableRequest.newBuilder().setTableMeta(meta).build();
		Tablet.CreateTableResponse response = tabletServer.createTable(request);
		if (response != null && response.getCode() == 0) {
			if (schema != null) {
				Table table = new Table(schema);
				GTableSchema.tableSchema.put(tid, table);
			}
			return true;
		}
		return false;
	}
}
