package com._4paradigm.rtidb.client.impl;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com._4paradigm.rtidb.client.KvIterator;
import com._4paradigm.rtidb.client.TabletException;
import com._4paradigm.rtidb.client.TabletSyncClient;
import com._4paradigm.rtidb.client.ha.PartitionHandler;
import com._4paradigm.rtidb.client.ha.RTIDBClient;
import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.ha.TableHandler;
import com._4paradigm.rtidb.client.metrics.TabletMetrics;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.RowCodec;
import com._4paradigm.rtidb.client.schema.SchemaCodec;
import com._4paradigm.rtidb.client.schema.Table;
import com._4paradigm.rtidb.tablet.Tablet;
import com._4paradigm.rtidb.tablet.Tablet.TTLType;
import com._4paradigm.utils.MurmurHash;
import com.google.protobuf.ByteBufferNoCopy;
import com.google.protobuf.ByteString;

import rtidb.api.TabletServer;

public class TabletSyncClientImpl implements TabletSyncClient {
    private final static Logger logger = LoggerFactory.getLogger(TabletSyncClientImpl.class);
    private RTIDBClient client;
    private final static int KEEP_LATEST_MAX_NUM = 1000;

    public TabletSyncClientImpl(RTIDBClient client) {
        this.client = client;
    }

    public TabletSyncClientImpl() {
    }

    @Override
    public boolean put(int tid, int pid, String key, long time, byte[] bytes) throws TimeoutException {
        PartitionHandler ph = client.getHandler(tid).getHandler(pid);
        return put(tid, pid, key, time, bytes, ph);
    }

    private boolean put(int tid, int pid, String key, long time, byte[] bytes, PartitionHandler ph)
            throws TimeoutException {
        Long consumed = System.nanoTime();
        Tablet.PutRequest request = Tablet.PutRequest.newBuilder().setPid(pid).setPk(key).setTid(tid).setTime(time)
                .setValue(ByteString.copyFrom(bytes)).build();
        Long encode = System.nanoTime() - consumed;
        consumed = System.nanoTime();
        Tablet.PutResponse response = ph.getLeader().put(request);
        Long network = System.nanoTime() - consumed;
        if (RTIDBClientConfig.isMetricsEnabled()) {
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
        Tablet.GetResponse response = client.getHandler(tid).getHandler(pid).getLeader().get(request);
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
        Tablet.DropTableResponse response = client.getHandler(0).getHandler(0).getLeader().dropTable(request);
        if (response != null && response.getCode() == 0) {
            logger.info("drop table tid {} pid {} ok", tid, pid);
            return true;
        }
        return false;
    }

    @Override
    public boolean createTable(String name, int tid, int pid, long ttl, int segCnt, List<ColumnDesc> schema) {
        return createTable(name, tid, pid, ttl, null, segCnt, schema);
    }

    private boolean put(int tid, int pid, long ts, List<Tablet.Dimension> ds, ByteBuffer row, TableHandler th)
            throws TimeoutException, TabletException {
        Long consumed = System.nanoTime();
        TabletServer tablet = th.getHandler(pid).getLeader();
        Tablet.PutRequest.Builder builder = Tablet.PutRequest.newBuilder();
        for (Tablet.Dimension dim : ds) {
            builder.addDimensions(dim);
        }
        builder.setPid(pid);
        builder.setTid(tid);
        builder.setTime(ts);
        row.rewind();
        builder.setValue(ByteBufferNoCopy.wrap(row.asReadOnlyBuffer()));
        Tablet.PutRequest request = builder.build();
        Long encode = System.nanoTime() - consumed;
        consumed = System.nanoTime();
        Tablet.PutResponse response = tablet.put(request);
        if (RTIDBClientConfig.isMetricsEnabled()) {
            Long network = System.nanoTime() - consumed;
            TabletMetrics.getInstance().addPut(encode, network);
        }
        if (response != null && response.getCode() == 0) {
            return true;
        }
        return false;
    }

    @Override
    public boolean put(int tid, int pid, long ts, Object[] row) throws TimeoutException, TabletException {
        TableHandler tableHandler = client.getHandler(tid);
        ByteBuffer buffer = RowCodec.encode(row, tableHandler.getSchema());
        List<Tablet.Dimension> dimList = new ArrayList<Tablet.Dimension>();
        int index = 0;
        for (int i = 0; i < tableHandler.getSchema().size(); i++) {
            if (tableHandler.getSchema().get(i).isAddTsIndex()) {
                if (row[i] == null) {
                    throw new TabletException("index " + index + "column is empty");
                }
                String value = row[i].toString();
                if (value.isEmpty()) {
                    throw new TabletException("index" + index + " column is empty");
                }
                Tablet.Dimension dim = Tablet.Dimension.newBuilder().setIdx(index).setKey(value).build();
                dimList.add(dim);
                index++;
            }
        }
        return put(tid, pid, ts, dimList, buffer, tableHandler);
    }

    @Override
    public KvIterator scan(int tid, int pid, String key, String idxName, long st, long et) throws TimeoutException {
        TabletServer tabletServer = client.getHandler(tid).getHandler(pid).getLeader();
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
        TabletServer tabletServer = client.getHandler(0).getHandler(0).getLeader();
        builder.setName(name).setTid(tid).setPid(pid).setTtl(ttl).setSegCnt(segCnt);
        Tablet.TableMeta meta = builder.build();
        Tablet.CreateTableRequest request = Tablet.CreateTableRequest.newBuilder().setTableMeta(meta).build();
        Tablet.CreateTableResponse response = tabletServer.createTable(request);
        if (response != null && response.getCode() == 0) {
            return true;
        }
        return false;
    }

    @Override
    public Object[] getRow(int tid, int pid, String key, long time) throws TimeoutException, TabletException {
        TabletServer tabletServer = client.getHandler(tid).getHandler(pid).getLeader();
        Table table = GTableSchema.getTable(tid, pid, tabletServer);
        long consumed = System.nanoTime();
        ByteString response = get(tid, pid, key, time);
        if (response == null) {
            return new Object[table.getSchema().size()];
        }
        long network = System.nanoTime() - consumed;
        consumed = System.nanoTime();
        Object[] row = RowCodec.decode(response.asReadOnlyByteBuffer(), table.getSchema());
        if (RTIDBClientConfig.isMetricsEnabled()) {
            long decode = System.nanoTime() - consumed;
            TabletMetrics.getInstance().addGet(decode, network);
        }
        return row;
    }

    @Override
    public boolean put(String name, String key, long time, byte[] bytes) throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(name);
        if (th == null) {
            throw new TabletException("no table with name " + name);
        }
        int pid = (int) (MurmurHash.hash64(key) % th.getPartitions().length);
        if (pid < 0) {
            pid = pid * -1;
        }
        PartitionHandler ph = th.getHandler(pid);
        return put(th.getTableInfo().getTid(), pid, key, time, bytes, ph);
    }

    @Override
    public boolean put(String name, long time, Object[] row) throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(name);
        if (th == null) {
            throw new TabletException("no table with name " + name);
        }
        ByteBuffer buffer = RowCodec.encode(row, th.getSchema());
        Map<Integer, List<Tablet.Dimension>> mapping = new HashMap<Integer, List<Tablet.Dimension>>();
        int index = 0;
        for (int i = 0; i < th.getSchema().size(); i++) {
            if (th.getSchema().get(i).isAddTsIndex()) {
                if (row[i] == null) {
                    throw new TabletException("index " + index + "column is empty");
                }
                String value = row[i].toString();
                if (value.isEmpty()) {
                    throw new TabletException("index" + index + " column is empty");
                }
                int pid = (int) (MurmurHash.hash64(value) % th.getPartitions().length);
                if (pid < 0) {
                    pid = pid * -1;
                }
                Tablet.Dimension dim = Tablet.Dimension.newBuilder().setIdx(index).setKey(value).build();
                List<Tablet.Dimension> dimList = mapping.get(pid);
                if (dimList == null) {
                    dimList = new ArrayList<Tablet.Dimension>();
                    mapping.put(pid, dimList);
                }
                dimList.add(dim);
                index++;
            }
        }
        Iterator<Map.Entry<Integer, List<Tablet.Dimension>>> it = mapping.entrySet().iterator();
        boolean ret = true;
        while (it.hasNext()) {
            Map.Entry<Integer, List<Tablet.Dimension>> entry = it.next();
            // TODO(wangtaize) support retry
            ret = ret && put(th.getTableInfo().getTid(), entry.getKey(), time, entry.getValue(), buffer, th);
        }
        return ret;
    }
}
