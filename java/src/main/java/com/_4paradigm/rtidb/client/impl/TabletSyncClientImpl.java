package com._4paradigm.rtidb.client.impl;

import com._4paradigm.rtidb.client.KvIterator;
import com._4paradigm.rtidb.client.TabletException;
import com._4paradigm.rtidb.client.TabletSyncClient;
import com._4paradigm.rtidb.client.ha.PartitionHandler;
import com._4paradigm.rtidb.client.ha.RTIDBClient;
import com._4paradigm.rtidb.client.ha.TableHandler;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.RowCodec;
import com._4paradigm.rtidb.client.schema.SchemaCodec;
import com._4paradigm.rtidb.tablet.Tablet;
import com._4paradigm.rtidb.tablet.Tablet.TTLType;
import com._4paradigm.rtidb.utils.MurmurHash;
import com.google.protobuf.ByteBufferNoCopy;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rtidb.api.TabletServer;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.TimeoutException;

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
        Tablet.PutRequest request = Tablet.PutRequest.newBuilder().setPid(pid).setPk(key).setTid(tid).setTime(time)
                .setValue(ByteString.copyFrom(bytes)).build();
        Tablet.PutResponse response = ph.getLeader().put(request);
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
        TableHandler th = client.getHandler(tid);
        if (th == null) {
            return null;
        }
        PartitionHandler ph = th.getHandler(pid);
        if (ph == null) {
            return null;
        }
        Tablet.GetResponse response = ph.getLeader().get(request);
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
        Tablet.PutResponse response = tablet.put(request);
        if (response != null && response.getCode() == 0) {
            return true;
        }
        return false;
    }

    @Override
    public boolean put(int tid, int pid, long ts, Object[] row) throws TimeoutException, TabletException {
        TableHandler tableHandler = client.getHandler(tid);
        if (tableHandler == null) {
            throw new TabletException("no table with id " + tid);
        }
        if (row == null) {
            throw new TabletException("putting data is null");
        }
        ByteBuffer buffer = null;
        if (row.length == tableHandler.getSchema().size()) {
            buffer = RowCodec.encode(row, tableHandler.getSchema());
        } else {
            List<ColumnDesc> columnDescs = tableHandler.getSchemaMap().get(row.length);
            if (columnDescs == null) {
                throw new TabletException("no schema for column count " + row.length);
            }
            int modifyTimes = row.length - tableHandler.getSchema().size();
            if (row.length > tableHandler.getSchema().size() + tableHandler.getSchemaMap().size()) {
                modifyTimes = tableHandler.getSchemaMap().size();
            }
            buffer = RowCodec.encode(row, columnDescs, modifyTimes);
        }
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
        TableHandler th = client.getHandler(tid);
        TabletServer tabletServer = th.getHandler(pid).getLeader();
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
            DefaultKvIterator it = null;
            if (th.getSchemaMap().size() > 0) {
                it = new DefaultKvIterator(response.getPairs(), th);
            } else {
                it = new DefaultKvIterator(response.getPairs(), th.getSchema(), network);
            }
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
        TableHandler th = client.getHandler(tid);
        ByteString response = get(tid, pid, key, time);
        if (response == null) {
            return new Object[th.getSchema().size()];
        }
        Object[] row = null;
        if (th.getSchemaMap().size() > 0) {
            row = RowCodec.decode(response.asReadOnlyByteBuffer(), th.getSchema(), th.getSchemaMap().size());
        } else {
            row = RowCodec.decode(response.asReadOnlyByteBuffer(), th.getSchema());
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
        if (row == null) {
            throw new TabletException("putting data is null");
        }
        ByteBuffer buffer = null;
        if (row.length == th.getSchema().size()) {
            buffer = RowCodec.encode(row, th.getSchema());
        } else {
            List<ColumnDesc> columnDescs = th.getSchemaMap().get(row.length);
            if (columnDescs == null) {
                throw new TabletException("no schema for column count " + row.length);
            }
            int modifyTimes = row.length - th.getSchema().size();
            if (row.length > th.getSchema().size() + th.getSchemaMap().size()) {
                modifyTimes = th.getSchemaMap().size();
            }
            buffer = RowCodec.encode(row, columnDescs, modifyTimes);
        }
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
