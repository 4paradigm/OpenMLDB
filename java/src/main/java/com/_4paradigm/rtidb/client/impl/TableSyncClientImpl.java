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
import com._4paradigm.rtidb.client.schema.RowCodec;
import com._4paradigm.rtidb.tablet.Tablet;
import com._4paradigm.utils.MurmurHash;
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
        return put(tid, pid, key, time, bytes, ph);
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
        return put(tid, pid, null, time, dimList, buffer, tableHandler.getHandler(pid));
    }

    @Override
    public ByteString get(int tid, int pid, String key) throws TimeoutException, TabletException {
        return get(tid, pid, key, 0l);
    }

    @Override
    public ByteString get(int tid, int pid, String key, long time) throws TimeoutException, TabletException {
        return get(tid, pid, key, time, client.getHandler(tid));
    }

    @Override
    public Object[] getRow(int tid, int pid, String key, long time) throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tid);
        long consumed = System.nanoTime();
        ByteString response = get(tid, pid, key, time, th);
        if (response == null) {
            return new Object[th.getSchema().size()];
        }
        long network = System.nanoTime() - consumed;
        consumed = System.nanoTime();
        Object[] row = RowCodec.decode(response.asReadOnlyByteBuffer(), th.getSchema());
        if (client.getConfig().isMetricsEnabled()) {
            long decode = System.nanoTime() - consumed;
            TabletMetrics.getInstance().addGet(decode, network);
        }
        return row;
    }
    
    @Override
    public ByteString get(String tname, String key) throws TimeoutException, TabletException {
        return get(tname, key, 0l);
    }
    
    @Override
    public Object[] getRow(String tname, String key, long time) throws TimeoutException, TabletException {
        if (key == null || key.isEmpty()) {
            throw new TabletException("key is null or empty");
        }
        TableHandler th = client.getHandler(tname);
        if (th == null) {
            throw new TabletException("no table with name " + tname);
        }
        int pid = (int) (MurmurHash.hash64(key) % th.getPartitions().length);
        if (pid < 0) {
            pid = pid * -1;
        }
        long consumed = System.nanoTime();
        ByteString response = get(th.getTableInfo().getTid(), pid, key, time, th);
        if (response == null) {
            return new Object[th.getSchema().size()];
        }
        long network = System.nanoTime() - consumed;
        consumed = System.nanoTime();
        Object[] row = RowCodec.decode(response.asReadOnlyByteBuffer(), th.getSchema());
        if (client.getConfig().isMetricsEnabled()) {
            long decode = System.nanoTime() - consumed;
            TabletMetrics.getInstance().addGet(decode, network);
        }
        return row;
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
        ByteString response = get(th.getTableInfo().getTid(), pid, key, time, th);
        return response;
    }
    
    private ByteString get(int tid, int pid, String key, long time, TableHandler th) throws TabletException {
        if (key == null || key.isEmpty()) {
            throw new TabletException("key is null or empty");
        }
        PartitionHandler ph = th.getHandler(pid);
        TabletServer ts = ph.getReadHandler(th.getReadStrategy());
        Tablet.GetRequest request = Tablet.GetRequest.newBuilder().setPid(pid).setTid(tid).setKey(key).setTs(time)
                .build();
        Tablet.GetResponse response = ts.get(request);
        if (response != null && response.getCode() == 0) {
            return response.getValue();
        }
        return null;
    }
    
    @Override
    public KvIterator scan(int tid, int pid, String key, long st, long et) throws TimeoutException, TabletException {
        return scan(tid, pid, key, null, st, et);
    }
    
    @Override
    public KvIterator scan(int tid, int pid, String key, String idxName, long st, long et) throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tid);
        return scan(tid, pid, key, idxName, st, et, th);
    }
    
    @Override
    public KvIterator scan(String tname, String key, long st, long et) throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tname);
        if (th == null) {
            throw new TabletException("no table with name " + tname);
        }
        int pid = (int) (MurmurHash.hash64(key) % th.getPartitions().length);
        if (pid < 0) {
            pid = pid * -1;
        }
        return scan(th.getTableInfo().getTid(), pid, key, null, st, et, th);
    }

    @Override
    public KvIterator scan(String tname, String key, String idxName, long st, long et)
            throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tname);
        if (th == null) {
            throw new TabletException("no table with name " + tname);
        }
        int pid = (int) (MurmurHash.hash64(key) % th.getPartitions().length);
        if (pid < 0) {
            pid = pid * -1;
        }
        return scan(th.getTableInfo().getTid(), pid, key, idxName, st, et, th);
    }

    private KvIterator scan(int tid, int pid, String key, String idxName, long st, long et, TableHandler th)throws TimeoutException, TabletException  {
        if (key == null || key.isEmpty()) {
            throw new TabletException("key is null or empty");
        }
        PartitionHandler ph = th.getHandler(pid);
        TabletServer ts = ph.getReadHandler(th.getReadStrategy());
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
        Tablet.ScanResponse response = ts.scan(request);
        Long network = System.nanoTime() - consuemd;
        if (response != null && response.getCode() == 0) {
            KvIterator it = new KvIterator(response.getPairs(), th.getSchema(), network);
            it.setCount(response.getCount());
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
        return put(th.getTableInfo().getTid(), pid, key, time, bytes, th.getHandler(pid));
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
            ret = ret && put(th.getTableInfo().getTid(), entry.getKey(), null, 
                             time, entry.getValue(), buffer, 
                             th.getHandler(entry.getKey()));
        }
        return ret;
    }
    
    
    private boolean put(int tid, int pid, String key, long time, byte[] bytes, PartitionHandler ph) throws TabletException{
        return put(tid, pid, key, time, null, ByteBuffer.wrap(bytes), ph);
    }
    
    private boolean put(int tid, int pid, 
            String key, long time, 
            List<Tablet.Dimension> ds, 
            ByteBuffer row, PartitionHandler ph) throws TabletException {
        if ((ds == null || ds.isEmpty()) && (key == null || key.isEmpty())) {
            throw new TabletException("key is null or empty");
        }
        Long consumed = System.nanoTime();
        TabletServer tablet = ph.getLeader();
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
        Long encode = System.nanoTime() - consumed;
        consumed = System.nanoTime();
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

    
    
}
