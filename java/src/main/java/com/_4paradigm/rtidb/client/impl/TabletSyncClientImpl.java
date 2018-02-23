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
import com._4paradigm.rtidb.client.TabletClientConfig;
import com._4paradigm.rtidb.client.TabletException;
import com._4paradigm.rtidb.client.TabletSyncClient;
import com._4paradigm.rtidb.client.metrics.TabletMetrics;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.RowCodec;
import com._4paradigm.rtidb.client.schema.SchemaCodec;
import com._4paradigm.rtidb.client.schema.Table;
import com.google.protobuf.ByteBufferNoCopy;
import com.google.protobuf.ByteString;

import io.brpc.client.RpcClient;
import io.brpc.client.RpcProxy;
import rtidb.api.Tablet;
import rtidb.api.Tablet.TTLType;
import rtidb.api.TabletServer;

public class TabletSyncClientImpl implements TabletSyncClient {
    private final static Logger logger = LoggerFactory.getLogger(TabletSyncClientImpl.class);
    private TabletServer tabletServer;
    private RpcClient client;
    private final static int KEEP_LATEST_MAX_NUM = 1000;

    public TabletSyncClientImpl(RpcClient rpcClient) {
        this.client = rpcClient;
    }

    public void init() {
        tabletServer = RpcProxy.getProxy(client, TabletServer.class);
    }

    public TabletSyncClientImpl() {
    }

    @Override
    public boolean put(int tid, int pid, String key, long time, byte[] bytes) throws TimeoutException {
        Long consumed = System.nanoTime();
        Tablet.PutRequest request = Tablet.PutRequest.newBuilder().setPid(pid).setPk(key).setTid(tid).setTime(time)
                .setValue(ByteString.copyFrom(bytes)).build();
        Long encode = System.nanoTime() - consumed;
        consumed = System.nanoTime();
        Tablet.PutResponse response = tabletServer.put(request);
        Long network = System.nanoTime() - consumed;
        if (TabletClientConfig.isMetricsEnabled()) {
            TabletMetrics.getInstance().addPut(encode, network);
        }
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
            logger.info("drop table tid {} pid{} ok", tid, pid);
            return true;
        }
        return false;
    }

    @Override
    public boolean createTable(String name, int tid, int pid, long ttl, int segCnt, List<ColumnDesc> schema) {
        return createTable(name, tid, pid, ttl, null, segCnt, schema);
    }

    @Override
    public boolean put(int tid, int pid, long ts, Object[] row) throws TimeoutException, TabletException {
        Long consumed = System.nanoTime();
        Table table = GTableSchema.getTable(tid, pid, tabletServer);
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
        buffer.rewind();
        builder.setValue(ByteBufferNoCopy.wrap(buffer.asReadOnlyBuffer()));
        Tablet.PutRequest request = builder.build();
        Long encode = System.nanoTime() - consumed;
        consumed = System.nanoTime();
        Tablet.PutResponse response = tabletServer.put(request);
        if (TabletClientConfig.isMetricsEnabled()) {
            Long network = System.nanoTime() - consumed;
            TabletMetrics.getInstance().addPut(encode, network);
        }
        if (response != null && response.getCode() == 0) {
            return true;
        }
        return false;
    }

    @Override
    public KvIterator scan(int tid, int pid, String key, String idxName, long st, long et) throws TimeoutException {
        Table table = GTableSchema.getTable(tid, pid, tabletServer);
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
        Long consuemd = System.nanoTime();
        Tablet.ScanResponse response = tabletServer.scan(request);
        Long network = System.nanoTime() - consuemd;
        if (response != null && response.getCode() == 0) {
            KvIterator it = new KvIterator(response.getPairs(), table.getSchema(), network);
            it.setCount(response.getCount());
            return it;
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
        if (ttl < 0) {
            return false;
        }
        if (null == name || "".equals(name.trim())) {
            return false;
        }
        if (type == TTLType.kLatestTime && (ttl > KEEP_LATEST_MAX_NUM || ttl < 0)) {
            return false;
        }
        Tablet.TableMeta.Builder builder = Tablet.TableMeta.newBuilder();
        Set<String> usedColumnName = new HashSet<String>();
        if (schema != null && schema.size() > 0) {
            for (ColumnDesc desc : schema) {
                if (null == desc.getName() || "".equals(desc.getName().trim())) {
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
            try {
                ByteBuffer buffer = SchemaCodec.encode(schema);
                builder.setSchema(ByteString.copyFrom(buffer.array()));
            } catch (TabletException e) {
                logger.error("fail to decode schema");
                return false;
            }

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

    @Override
    public Object[] getRow(int tid, int pid, String key, long time) throws TimeoutException, TabletException {
        Table table = GTableSchema.getTable(tid, pid, tabletServer);
        long consumed = System.nanoTime();
        ByteString response = get(tid, pid, key, time);
        if (response == null) {
            return new Object[table.getSchema().size()];
        }
        long network = System.nanoTime() - consumed;
        consumed = System.nanoTime();
        Object[] row = RowCodec.decode(response.asReadOnlyByteBuffer(), table.getSchema());
        if (TabletClientConfig.isMetricsEnabled()) {
            long decode = System.nanoTime() - consumed;
            TabletMetrics.getInstance().addGet(decode, network);
        }
        return row;
    }

}
