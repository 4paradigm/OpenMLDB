package com._4paradigm.rtidb.client.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.sql.Timestamp;

import com._4paradigm.rtidb.client.KvIterator;
import com._4paradigm.rtidb.client.TableSyncClient;
import com._4paradigm.rtidb.client.TabletException;
import com._4paradigm.rtidb.client.ha.PartitionHandler;
import com._4paradigm.rtidb.client.ha.RTIDBClient;
import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.ha.TableHandler;
import com._4paradigm.rtidb.client.metrics.TabletMetrics;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.ColumnType;
import com._4paradigm.rtidb.client.schema.RowCodec;
import com._4paradigm.rtidb.common.Common;
import com._4paradigm.rtidb.ns.NS;
import com._4paradigm.rtidb.tablet.Tablet;
import com._4paradigm.rtidb.utils.Compress;
import com._4paradigm.rtidb.utils.MurmurHash;
import com.google.common.base.Charsets;
import com.google.protobuf.ByteBufferNoCopy;
import com.google.protobuf.ByteString;

import org.joda.time.DateTime;
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
            throw new TabletException("fail to find partition with pid " + pid + " from table " + th.getTableInfo().getName());
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
        boolean handleNull = client.getConfig().isHandleNull();
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
                String value = null;
                if (row[i] == null) {
                    if (handleNull) {
                        value = RTIDBClientConfig.NULL_STRING;
                    } else {
                        index++;
                        continue;
                    }
                }
                if (row[i] != null) {
                    value = row[i].toString();
                }

                if (value.isEmpty()) {
                    if (handleNull) {
                        value = RTIDBClientConfig.EMPTY_STRING;
                    } else {
                        index++;
                        continue;
                    }
                }

                Tablet.Dimension dim = Tablet.Dimension.newBuilder().setIdx(index).setKey(value).build();
                dimList.add(dim);
                index++;
                count++;
            }
        }
        if (count == 0) {
            throw new TabletException("no dimension in this row");
        }
        return put(tid, pid, null, time, dimList, null, buffer, tableHandler);
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
        ByteString response = get(tid, pid, key, idxName, time, null, th);
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

        key = validateKey(key);
        int pid = (int) (MurmurHash.hash64(key) % th.getPartitions().length);
        if (pid < 0) {
            pid = pid * -1;
        }
        ByteString response = get(th.getTableInfo().getTid(), pid, key, null, time, null, th);
        return response;
    }

    /**
     * validate key
     * rewrite key if rtidb client config set isHandleNull {@code true}
     *
     * @param key
     * @return
     * @throws TabletException if key is null or empty
     */
    private String validateKey(String key) throws TabletException {
        if (key == null || key.isEmpty()) {
            if (client.getConfig().isHandleNull()) {
                key = key == null ? RTIDBClientConfig.NULL_STRING : key.isEmpty() ? RTIDBClientConfig.EMPTY_STRING : key;
            } else {
                throw new TabletException("key is null or empty");
            }
        }
        return key;
    }

    private ByteString get(int tid, int pid, String key, String idxName, long time, Tablet.GetType type, TableHandler th) throws TabletException {
        key = validateKey(key);
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
        if (response != null) {
            throw new TabletException(response.getCode(), response.getMsg());
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
        key = validateKey(key);
        int pid = (int) (MurmurHash.hash64(key) % th.getPartitions().length);
        if (pid < 0) {
            pid = pid * -1;
        }
        return count(th.getTableInfo().getTid(), pid, key, idxName, filter_expired_data, th);
    }

    @Override
    public int count(int tid, int pid, String key, boolean filter_expired_data) throws TimeoutException, TabletException {
        return count(tid, pid, key, null, filter_expired_data);
    }

    @Override
    public int count(int tid, int pid, String key, String idxName, boolean filter_expired_data) throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tid);
        if (th == null) {
            throw new TabletException("no table with tid" + tid);
        }
        return count(tid, pid, key, idxName, filter_expired_data, th);
    }

    private int count(int tid, int pid, String key, String idxName, boolean filter_expired_data, TableHandler th) throws TimeoutException, TabletException {
        key = validateKey(key);
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
        }
        if (response != null) {
            throw new TabletException(response.getCode(), response.getMsg());
        }
        throw new TabletException("rtidb internal server error");
    }

    @Override
    public boolean delete(String tname, String key) throws TimeoutException, TabletException {
        return delete(tname, key, null);
    }

    @Override
    public boolean delete(String tname, String key, String idxName) throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tname);
        if (th == null) {
            throw new TabletException("no table with name " + tname);
        }
        key = validateKey(key);
        int pid = (int) (MurmurHash.hash64(key) % th.getPartitions().length);
        if (pid < 0) {
            pid = pid * -1;
        }
        return delete(th.getTableInfo().getTid(), pid, key, idxName, th);

    }

    @Override
    public boolean delete(int tid, int pid, String key) throws TimeoutException, TabletException {
        return delete(tid, pid, key, null);
    }

    @Override
    public boolean delete(int tid, int pid, String key, String idxName) throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tid);
        if (th == null) {
            throw new TabletException("no table with tid" + tid);
        }
        return delete(tid, pid, key, idxName, th);
    }

    private boolean delete(int tid, int pid, String key, String idxName, TableHandler th) throws TimeoutException, TabletException {
        key = validateKey(key);
        PartitionHandler ph = th.getHandler(pid);
        TabletServer ts = ph.getLeader();
        if (ts == null) {
            throw new TabletException("Cannot find available tabletServer with tid " + tid);
        }
        Tablet.DeleteRequest.Builder builder = Tablet.DeleteRequest.newBuilder();
        builder.setTid(tid);
        builder.setPid(pid);
        builder.setKey(key);
        if (idxName != null && !idxName.isEmpty()) {
            builder.setIdxName(idxName);
        }
        Tablet.DeleteRequest request = builder.build();
        Tablet.GeneralResponse response = ts.delete(request);
        if (response != null && response.getCode() == 0) {
            return true;
        }
        if (response != null) {
            throw new TabletException(response.getCode(), response.getMsg());
        }
        return false;
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
        return scan(tid, pid, key, idxName, st, et, null, limit, th);
    }

    @Override
    public KvIterator scan(int tid, int pid, String key, String idxName, int limit) throws TimeoutException, TabletException {
        return scan(tid, pid, key, idxName, 0, 0, limit);
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
        key = validateKey(key);
        int pid = (int) (MurmurHash.hash64(key) % th.getPartitions().length);
        if (pid < 0) {
            pid = pid * -1;
        }
        return scan(th.getTableInfo().getTid(), pid, key, idxName, st, et, null, limit, th);
    }

    @Override
    public KvIterator scan(String tname, String key, String idxName, int limit) throws TimeoutException, TabletException {
        return scan(tname, key, idxName, 0, 0, limit);
    }

    @Override
    public KvIterator scan(String tname, String key, String idxName, long st, long et, String tsName, int limit)
            throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tname);
        if (th == null) {
            throw new TabletException("no table with name " + tname);
        }
        key = validateKey(key);
        int pid = (int) (MurmurHash.hash64(key) % th.getPartitions().length);
        if (pid < 0) {
            pid = pid * -1;
        }
        return scan(th.getTableInfo().getTid(), pid, key, idxName, st, et, tsName, limit, th);
    }

    @Override
    public KvIterator scan(String tname, Map<String, Object> key_map, String idxName, long st, long et, String tsName)
            throws TimeoutException, TabletException {
        return scan(tname, key_map, idxName, st, et, tsName, 0);
    }

    @Override
    public KvIterator scan(String tname, Object[] row, String idxName, long st, long et, String tsName)
            throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tname);
        if (th == null) {
            throw new TabletException("no table with name " + tname);
        }
        List<String> list = th.getKeyMap().get(idxName);
        if (list == null) {
            throw new TabletException("no index name in table" + idxName);
        }
        if (row.length != list.size()) {
            throw new TabletException("check key number failed");
        }
        boolean handleNull = client.getConfig().isHandleNull();
        String combinedKey = "";
        for (Object obj : row) {
            if (!combinedKey.isEmpty()) {
                combinedKey += "|";
            }
            if (obj == null) {
                if (handleNull) {
                    combinedKey += RTIDBClientConfig.NULL_STRING;
                } else {
                    throw new TabletException("has null key");
                }
            } else {
                String value = obj.toString();
                if (value.isEmpty()) {
                    if (handleNull) {
                        value = RTIDBClientConfig.EMPTY_STRING;
                    } else {
                        throw new TabletException("has empty key");
                    }
                }
                combinedKey += value;
            }
        }
        int pid = (int) (MurmurHash.hash64(combinedKey) % th.getPartitions().length);
        if (pid < 0) {
            pid = pid * -1;
        }
        return scan(th.getTableInfo().getTid(), pid, combinedKey, idxName, st, et, tsName, 0, th);

    }

    @Override
    public KvIterator scan(String tname, Map<String, Object> keyMap, String idxName, long st, long et, String tsName, int limit)
            throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tname);
        if (th == null) {
            throw new TabletException("no table with name " + tname);
        }
        List<String> list = th.getKeyMap().get(idxName);
        if (list == null) {
            throw new TabletException("no index name in table" + idxName);
        }
        boolean handleNull = client.getConfig().isHandleNull();
        String combinedKey = "";
        for (String key : list) {
            if (!combinedKey.isEmpty()) {
                combinedKey += "|";
            }
            Object obj = keyMap.get(key);
            if (obj == null) {
                if (handleNull) {
                    combinedKey += RTIDBClientConfig.NULL_STRING;
                } else {
                    throw new TabletException(key + " value is null");
                }
            } else {
                String value = obj.toString();
                if (value.isEmpty()) {
                    if (handleNull) {
                        value = RTIDBClientConfig.EMPTY_STRING;
                    } else {
                        throw new TabletException(key + " value is empty");
                    }
                }
                combinedKey += value;
            }
        }
        int pid = (int) (MurmurHash.hash64(combinedKey) % th.getPartitions().length);
        if (pid < 0) {
            pid = pid * -1;
        }
        return scan(th.getTableInfo().getTid(), pid, combinedKey, idxName, st, et, tsName, limit, th);
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

    @Override
    public KvIterator scan(int tid, int pid, Object[] row, String idxName, long st, long et, String tsName)
            throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tid);
        if (th == null) {
            throw new TabletException("no table with tid" + tid);
        }
        boolean handleNull = client.getConfig().isHandleNull();
        String combinedKey = "";
        for (Object obj : row) {
            if (!combinedKey.isEmpty()) {
                combinedKey += "|";
            }
            if (obj == null) {
                if (handleNull) {
                    combinedKey += RTIDBClientConfig.NULL_STRING;
                } else {
                    throw new TabletException("has null key");
                }
            } else {
                String value = obj.toString();
                if (value.isEmpty()) {
                    if (handleNull) {
                        value = RTIDBClientConfig.EMPTY_STRING;
                    } else {
                        throw new TabletException("has empty key");
                    }
                }
                combinedKey += value;
            }
        }
        return scan(tid, pid, combinedKey, idxName, st, et, tsName, 0, th);
    }

    @Override
    public KvIterator scan(int tid, int pid, Map<String, Object> keyMap, String idxName,
                           long st, long et, String tsName, int limit) throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tid);
        if (th == null) {
            throw new TabletException("no table with tid" + tid);
        }
        List<String> list = th.getKeyMap().get(idxName);
        if (list == null) {
            throw new TabletException("no index name in table" + idxName);
        }
        boolean handleNull = client.getConfig().isHandleNull();
        String combinedKey = "";
        for (String key : list) {
            if (!combinedKey.isEmpty()) {
                combinedKey += "|";
            }
            Object obj = keyMap.get(key);
            if (obj == null) {
                if (handleNull) {
                    combinedKey += RTIDBClientConfig.NULL_STRING;
                } else {
                    throw new TabletException(key + " value is null");
                }
            } else {
                String value = obj.toString();
                if (value.isEmpty()) {
                    if (handleNull) {
                        value = RTIDBClientConfig.EMPTY_STRING;
                    } else {
                        throw new TabletException(key + " value is empty");
                    }
                }
                combinedKey += value;
            }
        }
        return scan(tid, pid, combinedKey, idxName, st, et, tsName, limit, th);
    }

    private KvIterator scan(int tid, int pid, String key, String idxName,
                            long st, long et, String tsName, int limit, TableHandler th) throws TimeoutException, TabletException {
        key = validateKey(key);
        PartitionHandler ph = th.getHandler(pid);
        TabletServer ts = ph.getReadHandler(th.getReadStrategy());
        if (ts == null) {
            throw new TabletException("Cannot find available tabletServer with tid " + tid);
        }
        Tablet.ScanRequest.Builder builder = Tablet.ScanRequest.newBuilder();
        if (key != null) {
            builder.setPk(key);
        }
        builder.setTid(tid);
        builder.setEt(et);
        builder.setSt(st);
        builder.setPid(pid);
        builder.setLimit(limit);
        if (idxName != null && !idxName.isEmpty()) {
            builder.setIdxName(idxName);
        }
        if (tsName != null && !tsName.isEmpty()) {
            builder.setTsName(tsName);
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
            throw new TabletException(response.getCode(), response.getMsg());
        }
        throw new TabletException("rtidb internal server error");
    }

    @Override
    public boolean put(String name, String key, long time, byte[] bytes) throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(name);
        if (th == null) {
            throw new TabletException("no table with name " + name);
        }
        key = validateKey(key);
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
        if (th.hasTsCol()) {
            throw new TabletException("has ts column. should not set time");
        }
        return put(name, time, row, null);
    }

    private boolean put(String name, long time, Object[] row, List<Tablet.TSDimension> ts) throws TimeoutException, TabletException {
        boolean handleNull = client.getConfig().isHandleNull();
        TableHandler th = client.getHandler(name);
        if (th == null) {
            throw new TabletException("no table with name " + name);
        }
        Map<Integer, List<Integer>> indexs = th.getIndexes();
        if (indexs.isEmpty()) {
            throw new TabletException("no dimension in this row");
        }
        ByteBuffer buffer = RowCodec.encode(row, th.getSchema());
        Map<Integer, List<Tablet.Dimension>> mapping = new HashMap<Integer, List<Tablet.Dimension>>();
        int count = 0;
        for (Map.Entry<Integer, List<Integer>> entry : indexs.entrySet()) {
            int index = entry.getKey();
            String value = null;
            for (Integer pos : entry.getValue()) {
                String cur_value = null;
                if (pos >= row.length) {
                    throw new TabletException("index is greater than row length");
                }
                if (row[pos] == null) {
                    if (handleNull) {
                        cur_value = RTIDBClientConfig.NULL_STRING;
                    } else {
                        value = null;
                        break;
                    }
                } else {
                    cur_value = row[pos].toString();
                }
                if (cur_value.isEmpty()) {
                    if (handleNull) {
                        cur_value = RTIDBClientConfig.EMPTY_STRING;
                    } else {
                        value = null;
                        break;
                    }
                }
                if (value == null) {
                    value = cur_value;
                }  else {
                    value = value + "|" + cur_value;
                }
            }
            if (value == null || value.isEmpty()) {
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
            count++;
        }
        if (count == 0) {
            throw new TabletException("no dimension in this row");
        }
        Iterator<Map.Entry<Integer, List<Tablet.Dimension>>> it = mapping.entrySet().iterator();
        boolean ret = true;
        while (it.hasNext()) {
            Map.Entry<Integer, List<Tablet.Dimension>> entry = it.next();
            ret = ret && put(th.getTableInfo().getTid(), entry.getKey(), null,
                    time, entry.getValue(), ts, buffer, th);
        }
        return ret;
    }


    private boolean put(int tid, int pid, String key, long time, byte[] bytes, TableHandler th) throws TabletException {
        return put(tid, pid, key, time, null, null, ByteBuffer.wrap(bytes), th);
    }

    private boolean put(int tid, int pid,
                        String key, long time,
                        List<Tablet.Dimension> ds,
                        List<Tablet.TSDimension> ts,
                        ByteBuffer row, TableHandler th) throws TabletException {
        if ((ds == null || ds.isEmpty()) && (key == null || key.isEmpty())) {
            throw new TabletException("key is null or empty");
        }
        if (time == 0 && (ts == null || ts.isEmpty())) {
            throw new TabletException("ts is null or empty");
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
        }
        builder.setPid(pid);
        builder.setTid(tid);
        if (time != 0) {
            builder.setTime(time);
        } else {
            for (Tablet.TSDimension tsDim : ts) {
                builder.addTsDimensions(tsDim);
            }
        }
        if (key != null) {
            builder.setPk(key);
        } else {
            for (Tablet.Dimension dim : ds) {
                builder.addDimensions(dim);
            }
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
        if (response != null) {
            throw new TabletException(response.getCode(), response.getMsg());
        }
        return false;
    }

    @Override
    public boolean put(String tname, Map<String, Object> row) throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tname);
        if (th == null) {
            throw new TabletException("no table with name " + tname);
        }
        Object[] arrayRow = new Object[th.getSchema().size()];
        List<Tablet.TSDimension> tsDimensions = new ArrayList<Tablet.TSDimension>();
        int tsIndex = 0;
        for (int i = 0; i < th.getSchema().size(); i++) {
            ColumnDesc columnDesc = th.getSchema().get(i);
            Object colValue = row.get(columnDesc.getName());
            arrayRow[i] = colValue;
            if (columnDesc.isTsCol()) {
                if (columnDesc.getType() == ColumnType.kInt64) {
                    tsDimensions.add(Tablet.TSDimension.newBuilder().setIdx(tsIndex).setTs((Long)colValue).build());
                } else if (columnDesc.getType() == ColumnType.kTimestamp) {
                    if (colValue instanceof Timestamp) {
                        tsDimensions.add(Tablet.TSDimension.newBuilder().setIdx(tsIndex).
                                setTs(((Timestamp)colValue).getTime()).build());
                    } else if (colValue instanceof DateTime) {
                        tsDimensions.add(Tablet.TSDimension.newBuilder().setIdx(tsIndex).
                                setTs(((DateTime)colValue).getMillis()).build());
                    } else {
                        throw new TabletException("invalid ts column with name" + tname);
                    }
                } else {
                    throw new TabletException("invalid ts column with name" + tname);
                }
                tsIndex++;
            }
        }
        if (tsDimensions.isEmpty()) {
            throw new TabletException("no ts column with name" + tname);
        }
        return put(tname, 0, arrayRow, tsDimensions);
    }

    @Override
    public boolean put(String tname, long time, Map<String, Object> row) throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tname);
        if (th == null) {
            throw new TabletException("no table with name " + tname);
        }
        if (th.hasTsCol()) {
            throw new TabletException("has ts column. should not set time");
        }
        Object[] arrayRow = new Object[th.getSchema().size()];
        for (int i = 0; i < th.getSchema().size(); i++) {
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
        for (int i = 0; i < th.getSchema().size(); i++) {
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
        key = validateKey(key);
        int pid = (int) (MurmurHash.hash64(key) % th.getPartitions().length);
        if (pid < 0) {
            pid = pid * -1;
        }
        long consumed = 0l;
        if (client.getConfig().isMetricsEnabled()) {
            consumed = System.nanoTime();
        }
        ByteString response = get(th.getTableInfo().getTid(), pid, key, idxName, time, type, th);
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
