package com._4paradigm.rtidb.client.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import com._4paradigm.rtidb.client.KvIterator;
import com._4paradigm.rtidb.client.TableSyncClient;
import com._4paradigm.rtidb.client.TabletException;
import com._4paradigm.rtidb.client.ha.PartitionHandler;
import com._4paradigm.rtidb.client.ha.RTIDBClient;
import com._4paradigm.rtidb.client.ha.TableHandler;
import com._4paradigm.rtidb.client.metrics.TabletMetrics;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.RowCodec;
import com._4paradigm.rtidb.ns.NS;
import com._4paradigm.rtidb.tablet.Tablet;
import com._4paradigm.rtidb.utils.Compress;
import com._4paradigm.rtidb.utils.MurmurHash;
import com.google.common.base.Charsets;
import com.google.protobuf.ByteBufferNoCopy;
import com.google.protobuf.ByteString;

import rtidb.api.TabletServer;

public class TableSyncClientImpl implements TableSyncClient {
    private RTIDBClient client;
    public TableSyncClientImpl(RTIDBClient client) {
        this.client = client;
    }
    @Override
    public boolean put(int tid, int pid, String key, long time, byte[] bytes) throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tid);
        if (th == null) {
            throw new TabletException("fail to find table with id " + tid);
        }
        if (th.getPartitions().length <= pid) {
            throw new TabletException("fail to find partition with pid "+ pid +" from table " +th.getTableInfo().getName());
        }
        PartitionHandler ph = th.getHandler(pid);
        return put(tid, pid, key, time, bytes, th);
    }

    @Override
    public boolean put(int tid, int pid, String key, long time, String value) throws TimeoutException, TabletException {
        return put(tid, pid, key, time, value.getBytes(Charsets.UTF_8));
    }
    
    @Override
    public boolean put(String tname, String key, long time, String value) throws TimeoutException, TabletException {
        return put(tname, key, time, value.getBytes(Charsets.UTF_8));
    }

    @Override
    public boolean put(int tid, int pid, long time, Object[] row) throws TimeoutException, TabletException {
        TableHandler tableHandler = client.getHandler(tid);
        if (tableHandler == null) {
            throw new TabletException("fail to find table with id " + tid);
        }
        ByteBuffer buffer = RowCodec.encode(row, tableHandler.getSchema());
        List<Tablet.Dimension> dimList = new ArrayList<Tablet.Dimension>();
        int index = 0;
        int count = 0;
        for (int i = 0; i < tableHandler.getSchema().size(); i++) {
            if (tableHandler.getSchema().get(i).isAddTsIndex()) {
                if (row[i] == null) {
                    index ++;
                    continue;
                }
                String value = row[i].toString();
                if (value.isEmpty()) {
                    index ++;
                    continue;
                }
                Tablet.Dimension dim = Tablet.Dimension.newBuilder().setIdx(index).setKey(value).build();
                dimList.add(dim);
                index ++;
                count ++;
            }
        }
        if (count == 0) {
            throw new TabletException("no dimension in this row");
        }
        return put(tid, pid, null, time, dimList, buffer, tableHandler);
    }

    @Override
    public ByteString get(int tid, int pid, String key) throws TimeoutException, TabletException {
        return get(tid, pid, key, 0l);
    }

    @Override
    public ByteString get(int tid, int pid, String key, long time) throws TimeoutException, TabletException {
        return get(tid, pid, key, null, time, null, client.getHandler(tid));
    }

    @Override
    public Object[] getRow(int tid, int pid, String key, long time) throws TimeoutException, TabletException {
        return getRow(tid, pid, key, null, time);
    }

    @Override
    public Object[] getRow(int tid, int pid, String key, String idxName, long time) throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tid);
        if (th == null) {
            throw new TabletException("fail to find table with id " + tid);
        }
        long consumed = 0l;
        if (client.getConfig().isMetricsEnabled()) {
            consumed = System.nanoTime();
        }
        ByteString response = get(tid, pid, key, idxName, time,null, th);
        if (response == null) {
            return new Object[th.getSchema().size()];
        }
        long network = 0;
        if (client.getConfig().isMetricsEnabled()) {
            network = System.nanoTime() - consumed;
            consumed = System.nanoTime();
        }
        Object[] row = RowCodec.decode(response.asReadOnlyByteBuffer(), th.getSchema());
        if (client.getConfig().isMetricsEnabled()) {
            long decode = System.nanoTime() - consumed;
            TabletMetrics.getInstance().addGet(decode, network);
        }
        return row;
    }

    @Override
    public Object[] getRow(int tid, int pid, String key, String idxName) throws TimeoutException, TabletException {
        return getRow(tid, pid, key, idxName, 0);
    }

    @Override
    public ByteString get(String tname, String key) throws TimeoutException, TabletException {
        return get(tname, key, 0l);
    }
    
    @Override
    public Object[] getRow(String tname, String key, long time) throws TimeoutException, TabletException {
        return getRow(tname, key, null, time, null);
    }

    @Override
    public Object[] getRow(String tname, String key, String idxName) throws TimeoutException, TabletException {
        return getRow(tname, key, idxName, 0);
    }

    @Override
    public Object[] getRow(String tname, String key, String idxName, long time) throws TimeoutException, TabletException {
        return getRow(tname, key, idxName, time, null);
    }

    @Override
    public ByteString get(String tname, String key, long time) throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tname);
        if (th == null) {
            throw new TabletException("no table with name " + tname);
        }
        int pid = (int) (MurmurHash.hash64(key) % th.getPartitions().length);
        if (pid < 0) {
            pid = pid * -1;
        }
        ByteString response = get(th.getTableInfo().getTid(), pid, key, null, time, null, th);
        return response;
    }
    
    private ByteString get(int tid, int pid, String key, String idxName, long time, Tablet.GetType type, TableHandler th) throws TabletException {
        if (key == null || key.isEmpty()) {
            throw new TabletException("key is null or empty");
        }
        PartitionHandler ph = th.getHandler(pid);
        TabletServer ts = ph.getReadHandler(th.getReadStrategy());
        if (ts == null) {
            throw new TabletException("Cannot find available tabletServer with tid " + tid);
        }
        Tablet.GetRequest.Builder builder = Tablet.GetRequest.newBuilder();
        builder.setTid(tid);
        builder.setPid(pid);
        builder.setKey(key);
        builder.setTs(time);
        if (type != null) builder.setType(type);
        if (idxName != null && !idxName.isEmpty()) {
            builder.setIdxName(idxName);
        }
        Tablet.GetRequest request = builder.build();
        Tablet.GetResponse response = ts.get(request);
        if (response != null && response.getCode() == 0) {
            if (th.getTableInfo().hasCompressType() && th.getTableInfo().getCompressType() == NS.CompressType.kSnappy) {
                byte[] uncompressed = Compress.snappyUnCompress(response.getValue().toByteArray());
                return ByteString.copyFrom(uncompressed);
            } else {
                return response.getValue();
            }
        }
        return null;
    }

    @Override
    public int count(String tname, String key) throws TimeoutException, TabletException {
        return count(tname, key, null, false);
    }

    @Override
    public int count(String tname, String key, boolean filter_expired_data) throws TimeoutException, TabletException {
        return count(tname, key, null, filter_expired_data);
    }

    @Override
    public int count(String tname, String key, String idxName) throws TimeoutException, TabletException {
        return count(tname, key, idxName, false);
    }

    @Override
    public int count(String tname, String key, String idxName, boolean filter_expired_data) throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tname);
        if (th == null) {
            throw new TabletException("no table with name " + tname);
        }
        int pid = (int) (MurmurHash.hash64(key) % th.getPartitions().length);
        if (pid < 0) {
            pid = pid * -1;
        }
        return count(th.getTableInfo().getTid(), pid, key, idxName, filter_expired_data, th);
    }

    private int count(int tid, int pid, String key, String idxName, boolean filter_expired_data, TableHandler th) throws TabletException {
        if (key == null || key.isEmpty()) {
            throw new TabletException("key is null or empty");
        }
        PartitionHandler ph = th.getHandler(pid);
        TabletServer ts = ph.getReadHandler(th.getReadStrategy());
        if (ts == null) {
            throw new TabletException("Cannot find available tabletServer with tid " + tid);
        }
        Tablet.CountRequest.Builder builder = Tablet.CountRequest.newBuilder();
        builder.setTid(tid);
        builder.setPid(pid);
        builder.setKey(key);
        builder.setFilterExpiredData(filter_expired_data);
        if (idxName != null && !idxName.isEmpty()) {
            builder.setIdxName(idxName);
        }
        Tablet.CountRequest request = builder.build();
        Tablet.CountResponse response = ts.count(request);
        if (response != null && response.getCode() == 0) {
            return response.getCount();
        } else {
            throw new TabletException(response.getMsg());
        }
    }

    @Override
    public KvIterator scan(int tid, int pid, String key, long st, long et) throws TimeoutException, TabletException {
        return scan(tid, pid, key, null, st, et, 0);
    }

    @Override
    public KvIterator scan(int tid, int pid, String key, int limit) throws TimeoutException, TabletException {
        return scan(tid, pid, key, null, 0, 0, limit);
    }

    @Override
    public KvIterator scan(int tid, int pid, String key, long st, long et, int limit) throws TimeoutException, TabletException {
        return scan(tid, pid, key, null, st, et, limit);
    }

    @Override
    public KvIterator scan(int tid, int pid, String key, String idxName, long st, long et) throws TimeoutException, TabletException {
        return scan(tid, pid, key, idxName, st, et, 0);
    }

    @Override
    public KvIterator scan(int tid, int pid, String key, String idxName, long st, long et, int limit) throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tid);
        if (th == null) {
            throw new TabletException("no table with tid" + tid);
        }
        return scan(tid, pid, key, idxName, st, et, limit, th);
    }

    @Override
    public KvIterator scan(int tid, int pid, String key, String idxName, int limit) throws TimeoutException, TabletException {
        return scan(tid, pid, key, idxName, 0, 0, limit);
    }

    @Override
    public KvIterator traverse(int tid) throws TimeoutException, TabletException {
        return traverse(tid, null);
    }

    @Override
    public KvIterator traverse(int tid, String idxName) throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tid);
        TraverseKvIterator it = new TraverseKvIterator(client, th, idxName);
        it.next();
        return it;
    }

    @Override
    public KvIterator scan(String tname, String key, long st, long et) throws TimeoutException, TabletException {

        return scan(tname, key, null, st, et, 0);
    }

    @Override
    public KvIterator scan(String tname, String key, int limit) throws TimeoutException, TabletException {
        return scan(tname, key, null, 0, 0, limit);
    }

    @Override
    public KvIterator scan(String tname, String key, long st, long et, int limit) throws TimeoutException, TabletException {
        return scan(tname, key, null, st, et, limit);
    }

    @Override
    public KvIterator scan(String tname, String key, String idxName, long st, long et)
            throws TimeoutException, TabletException {
        return scan(tname, key, idxName, st, et, 0);
    }

    @Override
    public KvIterator scan(String tname, String key, String idxName, long st, long et, int limit) throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tname);
        if (th == null) {
            throw new TabletException("no table with name " + tname);
        }
        int pid = (int) (MurmurHash.hash64(key) % th.getPartitions().length);
        if (pid < 0) {
            pid = pid * -1;
        }
        return scan(th.getTableInfo().getTid(), pid, key, idxName, st, et, limit, th);
    }

    @Override
    public KvIterator scan(String tname, String key, String idxName, int limit) throws TimeoutException, TabletException {
        return scan(tname, key, idxName, 0, 0, limit);
    }

    @Override
    public KvIterator traverse(String tname, String idxName) throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tname);
        if (th == null) {
            throw new TabletException("no table with name " + tname);
        }
        TraverseKvIterator it = new TraverseKvIterator(client, th, idxName);
        it.next();
        return it;
    }

    @Override
    public KvIterator traverse(String tname) throws TimeoutException, TabletException {
        return traverse(tname, null);
    }

    private KvIterator scan(int tid, int pid, String key, String idxName, long st, long et, int limit, TableHandler th)throws TimeoutException, TabletException  {
        if (key == null || key.isEmpty()) {
            throw new TabletException("key is null or empty");
        }
        PartitionHandler ph = th.getHandler(pid);
        TabletServer ts = ph.getReadHandler(th.getReadStrategy());
        if (ts == null) {
            throw new TabletException("Cannot find available tabletServer with tid " + tid);
        }
        Tablet.ScanRequest.Builder builder = Tablet.ScanRequest.newBuilder();
        builder.setPk(key);
        builder.setTid(tid);
        builder.setEt(et);
        builder.setSt(st);
        builder.setPid(pid);
        builder.setLimit(limit);
        if (idxName != null && !idxName.isEmpty()) {
            builder.setIdxName(idxName);
        }
        if (client.getConfig().isRemoveDuplicateByTime()) {
            builder.setEnableRemoveDuplicatedRecord(true);
        }
        Tablet.ScanRequest request = builder.build();
        long consuemd = 0l;
        if (client.getConfig().isMetricsEnabled()) {
            consuemd = System.nanoTime();
        }
        Tablet.ScanResponse response = ts.scan(request);
        if (response != null && response.getCode() == 0) {
            Long network = null;
            if (client.getConfig().isMetricsEnabled()) {
                network = System.nanoTime() - consuemd;
            }
            DefaultKvIterator it = new DefaultKvIterator(response.getPairs(), th.getSchema(), network);
            it.setCount(response.getCount());
            if (th.getTableInfo().hasCompressType()) {
                it.setCompressType(th.getTableInfo().getCompressType());
            }
            return it;
        }
        if (response != null) {
            throw new TabletException(response.getMsg());
        }
        throw new TabletException("rtidb internal server error");
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
        return put(th.getTableInfo().getTid(), pid, key, time, bytes, th);
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
        int count = 0;
        for (int i = 0; i < th.getSchema().size(); i++) {
            if (th.getSchema().get(i).isAddTsIndex()) {
                if (row[i] == null) {
                    index ++;
                    continue;
                }
                String value = row[i].toString();
                if (value.isEmpty()) {
                    index ++;
                    continue;
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
                index ++;
                count ++;
            }
        }
        if (count == 0) {
            throw new TabletException("no dimension in this row");
        }
        Iterator<Map.Entry<Integer, List<Tablet.Dimension>>> it = mapping.entrySet().iterator();
        boolean ret = true;
        while (it.hasNext()) {
            Map.Entry<Integer, List<Tablet.Dimension>> entry = it.next();
            ret = ret && put(th.getTableInfo().getTid(), entry.getKey(), null, 
                             time, entry.getValue(), buffer, th);
        }
        return ret;
    }
    
    
    private boolean put(int tid, int pid, String key, long time, byte[] bytes, TableHandler th) throws TabletException{
        return put(tid, pid, key, time, null, ByteBuffer.wrap(bytes), th);
    }
    
    private boolean put(int tid, int pid, 
            String key, long time, 
            List<Tablet.Dimension> ds, 
            ByteBuffer row, TableHandler th) throws TabletException {
        if ((ds == null || ds.isEmpty()) && (key == null || key.isEmpty())) {
            throw new TabletException("key is null or empty");
        }
        PartitionHandler ph = th.getHandler(pid);
        if (th.getTableInfo().hasCompressType() && th.getTableInfo().getCompressType() == NS.CompressType.kSnappy) {
            byte[] data = row.array();
            byte[] compressed = Compress.snappyCompress(data);
            if (compressed == null) {
                throw new TabletException("snappy compress error");
            }
            ByteBuffer buffer = ByteBuffer.wrap(compressed);
            row = buffer;
        }
        long consumed = 0l;
        if (client.getConfig().isMetricsEnabled()) {
            consumed = System.nanoTime();
        }
        TabletServer tablet = ph.getLeader();
        if (tablet == null) {
            throw new TabletException("Cannot find available tabletServer with tid " + tid);
        }
        Tablet.PutRequest.Builder builder = Tablet.PutRequest.newBuilder();
        if (ds != null) {
            for (Tablet.Dimension dim : ds) {
                builder.addDimensions(dim);
            }
        }
        builder.setPid(pid);
        builder.setTid(tid);
        builder.setTime(time);
        if (key != null) {
            builder.setPk(key);
        }
        row.rewind();
        builder.setValue(ByteBufferNoCopy.wrap(row.asReadOnlyBuffer()));
        Tablet.PutRequest request = builder.build();
        long encode = 0l;
        if (client.getConfig().isMetricsEnabled()) {
            encode = System.nanoTime() - consumed;
            consumed = System.nanoTime();
        }
        Tablet.PutResponse response = tablet.put(request);
        if (client.getConfig().isMetricsEnabled()) {
            Long network = System.nanoTime() - consumed;
            TabletMetrics.getInstance().addPut(encode, network);
        }
        if (response != null && response.getCode() == 0) {
            return true;
        }
        return false;
    }
    
    @Override
    public boolean put(String tname, long time, Map<String, Object> row) throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tname);
        if (th == null) {
            throw new TabletException("no table with name " + tname);
        }
        Object[] arrayRow = new Object[th.getSchema().size()];
        for(int i = 0; i < th.getSchema().size(); i++) {
            arrayRow[i] = row.get(th.getSchema().get(i).getName());
        }
        return put(tname, time, arrayRow);
    }
    
    @Override
    public List<ColumnDesc> getSchema(String tname) throws TabletException {
        TableHandler th = client.getHandler(tname);
        if (th == null) {
            throw new TabletException("no table with name " + tname);
        }
        return th.getSchema();
    }
    @Override
    public boolean put(int tid, int pid, long time, Map<String, Object> row) throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tid);
        if (th == null) {
            throw new TabletException("fail to find table with id " + tid);
        }
        Object[] arrayRow = new Object[th.getSchema().size()];
        for(int i = 0; i < th.getSchema().size(); i++) {
            arrayRow[i] = row.get(th.getSchema().get(i).getName());
        }
        return put(tid, pid, time, arrayRow);
    }
    
    @Override
    public List<ColumnDesc> getSchema(int tid) throws TabletException {
        TableHandler th = client.getHandler(tid);
        if (th == null) {
            throw new TabletException("fail to find table with id " + tid);
        }
        return th.getSchema();
    }

    @Override
    public Object[] getRow(String tname, String key, String idxName, long time, Tablet.GetType type) throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tname);
        if (th == null) {
            throw new TabletException("no table with name " + tname);
        }
        int pid = (int) (MurmurHash.hash64(key) % th.getPartitions().length);
        if (pid < 0) {
            pid = pid * -1;
        }
        long consumed = 0l;
        if (client.getConfig().isMetricsEnabled()) {
            consumed = System.nanoTime();
        }
        ByteString response = get(th.getTableInfo().getTid(), pid, key, idxName, time,type, th);
        if (response == null) {
            return new Object[th.getSchema().size()];
        }
        long network = 0l;
        if (client.getConfig().isMetricsEnabled()) {
            network = System.nanoTime() - consumed;
            consumed = System.nanoTime();
        }
        Object[] row = RowCodec.decode(response.asReadOnlyByteBuffer(), th.getSchema());
        if (client.getConfig().isMetricsEnabled()) {
            long decode = System.nanoTime() - consumed;
            TabletMetrics.getInstance().addGet(decode, network);
        }
        return row;
    }

    @Override
    public Object[] getRow(String tname, String key, long time, Tablet.GetType type) throws TimeoutException, TabletException {
        return getRow(tname, key, null, time, type);
    }
}
