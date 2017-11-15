package com._4paradigm.rtidb.client.impl;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com._4paradigm.rtidb.client.KvIterator;
import com._4paradigm.rtidb.client.TabletException;
import com._4paradigm.rtidb.client.TabletSyncClient;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.RowCodec;
import com._4paradigm.rtidb.client.schema.SchemaCodec;
import com.google.protobuf.ByteString;

import rtidb.api.Tablet;
import rtidb.api.TabletServer;

public class TabletSyncClientImpl implements TabletSyncClient {
	private final static Logger logger = LoggerFactory.getLogger(TabletSyncClientImpl.class);
	private TabletServer tabletServer;
	private ConcurrentHashMap<Integer, List<ColumnDesc>> tableSchema = new ConcurrentHashMap<Integer, List<ColumnDesc>>();

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
		Tablet.ScanRequest.Builder builder = Tablet.ScanRequest.newBuilder();
		builder.setPk(key);
		builder.setTid(tid);
		builder.setEt(et);
		builder.setSt(st);
		builder.setPid(pid);
		Tablet.ScanRequest request = builder.build();
		Tablet.ScanResponse response = tabletServer.scan(request);
		if (response != null && response.getCode() == 0) {
			return new KvIterator(response.getPairs());
		}
		return null;
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
	public boolean createTable(String name, int tid, int pid, long ttl, int segCnt, List<ColumnDesc> schema) {

		Tablet.TableMeta.Builder builder = Tablet.TableMeta.newBuilder();
		if (schema != null && schema.size() > 0) {
			ByteBuffer buffer = SchemaCodec.encode(schema);
			builder.setSchema(ByteString.copyFrom(buffer.array()));
			for (ColumnDesc desc : schema) {
				if (desc.isAddTsIndex()) {
					builder.addDimensions(desc.getName());
				}
			}
		}
		builder.setName(name).setTid(tid).setPid(pid).setTtl(ttl).setSegCnt(segCnt);
		Tablet.TableMeta meta = builder.build();
		Tablet.CreateTableRequest request = Tablet.CreateTableRequest.newBuilder().setTableMeta(meta).build();
		Tablet.CreateTableResponse response = tabletServer.createTable(request);
		if (response != null && response.getCode() == 0) {
			tableSchema.putIfAbsent(tid, schema);
			return true;
		}
		return false;

	}

	@Override
	public boolean put(int tid, int pid, long ts, Object[] row) throws TimeoutException, TabletException {
		List<ColumnDesc> schema = tableSchema.get(tid);
		Tablet.PutRequest.Builder builder = Tablet.PutRequest.newBuilder();
		ByteBuffer buffer = RowCodec.encode(row, schema);
		int index = 0;
		for (int i = 0; i < schema.size(); i++) {
			if (schema.get(i).isAddTsIndex()) {
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
}
