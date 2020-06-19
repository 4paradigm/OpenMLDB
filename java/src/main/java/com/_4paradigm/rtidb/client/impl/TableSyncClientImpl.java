package com._4paradigm.rtidb.client.impl;

import com._4paradigm.rtidb.blobserver.OSS;
import com._4paradigm.rtidb.client.*;
import com._4paradigm.rtidb.client.ha.PartitionHandler;
import com._4paradigm.rtidb.client.ha.RTIDBClient;
import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.ha.TableHandler;
import com._4paradigm.rtidb.client.schema.*;
import com._4paradigm.rtidb.client.type.DataType;
import com._4paradigm.rtidb.ns.NS;
import com._4paradigm.rtidb.tablet.Tablet;
import com._4paradigm.rtidb.utils.Compress;
import com.google.common.base.Charsets;
import com.google.protobuf.ByteBufferNoCopy;
import com.google.protobuf.ByteString;
import rtidb.api.TabletServer;
import rtidb.blobserver.BlobServer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class TableSyncClientImpl implements TableSyncClient {
    private RTIDBClient client;

    public TableSyncClientImpl(RTIDBClient client) {
        this.client = client;
    }

    @Override
    public Object[] getRow(String tname, String key, long time, GetOption getOption) throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tname);
        if (th == null) {
            throw new TabletException("no table with name " + tname);
        }
        key = validateKey(key);
        int pid = TableClientCommon.computePidByKey(key, th.getPartitions().length);
        Object[] row = get(pid, key, time, getOption, th);
        return row;
    }

    @Override
    public Object[] getRow(String tname, Object[] keyArr, long time, GetOption option) throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tname);
        if (th == null) {
            throw new TabletException("no table with name " + tname);
        }
        if (option.getIdxName() == null) {
            throw new TabletException("index name is required");
        }
        List<String> list = th.getKeyMap().get(option.getIdxName());
        if (list == null) {
            throw new TabletException("no index name in table" + option.getIdxName());
        }
        if (keyArr.length != list.size()) {
            throw new TabletException("check key number failed");
        }
        String combinedKey = TableClientCommon.getCombinedKey(keyArr, client.getConfig().isHandleNull());
        int pid = TableClientCommon.computePidByKey(combinedKey, th.getPartitions().length);
        Object[] row = get(pid, combinedKey, time, option, th);
        return row;
    }

    @Override
    public Object[] getRow(String tname, Map<String, Object> keyMap, long time, GetOption option) throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tname);
        if (th == null) {
            throw new TabletException("no table with name " + tname);
        }
        if (option.getIdxName() == null) {
            throw new TabletException("index name is required");
        }
        List<String> list = th.getKeyMap().get(option.getIdxName());
        if (list == null) {
            throw new TabletException("no index name in table" + option.getIdxName());
        }
        String combinedKey = TableClientCommon.getCombinedKey(keyMap, list, client.getConfig().isHandleNull());
        int pid = TableClientCommon.computePidByKey(combinedKey, th.getPartitions().length);
        Object[] row = get(pid, combinedKey, time, option, th);
        return row;
    }

    @Override
    public Object[] getRow(String tname, String key, long time, Object type) throws TimeoutException, TabletException {
        GetOption getOption = new GetOption();
        getOption.setEtType((Tablet.GetType) type);
        return getRow(tname, key, time, getOption);
    }

    @Override
    public KvIterator scan(String tname, Map<String, Object> keyMap, long st, long et, ScanOption option) throws TimeoutException, TabletException {
        if (option.getIdxName() == null) throw new TabletException("idx name is required ");
        TableHandler th = client.getHandler(tname);
        if (th == null) {
            throw new TabletException("no table with name " + tname);
        }
        List<String> list = th.getKeyMap().get(option.getIdxName());
        if (list == null) {
            throw new TabletException("no index name in table" + option.getIdxName());
        }
        String combinedKey = TableClientCommon.getCombinedKey(keyMap, list, client.getConfig().isHandleNull());
        int pid = TableClientCommon.computePidByKey(combinedKey, th.getPartitions().length);
        return scan(th.getTableInfo().getTid(), pid, combinedKey, st, et, th, option);
    }

    @Override
    public KvIterator scan(String tname, String key, long st, long et, ScanOption option) throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tname);
        if (th == null) {
            throw new TabletException("no table with name " + tname);
        }
        key = validateKey(key);
        if (th.GetPartitionKeyList().isEmpty() || TableClientCommon.isQueryByPartitionKey(option.getIdxName(), th)) {
            int pid = TableClientCommon.computePidByKey(key, th.getPartitions().length);
            return scan(th.getTableInfo().getTid(), pid, key, st, et, th, option);
        }

        List<ScanFuture> futureList = TableClientCommon.scanInternal(th.getTableInfo().getTid(), key, st, et, th, option);
        List<ByteString> byteStrings = new ArrayList<>();
        int count = 0;
        ProjectionInfo projectionInfo = null;
        try {
            for (ScanFuture scanFuture : futureList) {
                Tablet.ScanResponse response = scanFuture.getResponse();
                if (response == null) {
                    throw new TabletException("Connection error");
                }
                if (response.getCode() != 0) {
                    throw new TabletException(response.getCode(), response.getMsg());
                } else if (response.getCount() > 0) {
                    count += response.getCount();
                    byteStrings.add(response.getPairs());
                    if (projectionInfo == null) {
                        projectionInfo = scanFuture.getProjectionInfo();
                    }
                }
            }
        } catch (Exception e) {
            throw new TabletException("rtidb internal server error");
        }
        if (option.getLimit() != 0) {
            count = Math.min(option.getLimit(), count);
        }
        if (th.getTableInfo().getFormatVersion() == 1) {
            if (projectionInfo != null) {
                return new RowKvIterator(byteStrings, projectionInfo.getProjectionSchema(), count);
            } else {
                return new RowKvIterator(byteStrings, th.getSchema(), count);
            }
        } else {
            DefaultKvIterator iter = new DefaultKvIterator(byteStrings, th, projectionInfo);
            iter.setCount(count);
            return iter;
        }
    }

    @Override
    public KvIterator scan(String tname, Object[] keyArr, long st, long et, ScanOption option) throws TimeoutException, TabletException {
        if (option.getIdxName() == null) throw new TabletException("idx name is required ");
        TableHandler th = client.getHandler(tname);
        if (th == null) {
            throw new TabletException("no table with name " + tname);
        }
        List<String> list = th.getKeyMap().get(option.getIdxName());
        if (list == null) {
            throw new TabletException("no index name in table" + option.getIdxName());
        }
        if (keyArr.length != list.size()) {
            throw new TabletException("check key number failed");
        }
        String combinedKey = TableClientCommon.getCombinedKey(keyArr, client.getConfig().isHandleNull());
        int pid = TableClientCommon.computePidByKey(combinedKey, th.getPartitions().length);
        return scan(th.getTableInfo().getTid(), pid, combinedKey, st, et, th, option);
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
        TableHandler th = client.getHandler(tid);
        if (th == null) {
            throw new TabletException("fail to find table with id " + tid);
        }
        if (row == null) {
            throw new TabletException("putting data is null");
        }
        ByteBuffer buffer = null;
        if (row.length == th.getSchema().size()) {
            switch (th.getFormatVersion()) {
                case 1:
                    buffer = RowBuilder.encode(row, th.getSchema());
                    break;
                default:
                    buffer = RowCodec.encode(row, th.getSchema());
            }
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
        List<Tablet.Dimension> dimList = TableClientCommon.fillTabletDimension(row, th, client.getConfig().isHandleNull());
        return put(tid, pid, null, time, dimList, null, buffer, th);
    }

    @Override
    public ByteString get(int tid, int pid, String key) throws TimeoutException, TabletException {
        return get(tid, pid, key, 0l);
    }

    @Override
    public ByteString get(int tid, int pid, String key, long time) throws TimeoutException, TabletException {

        return get(tid, pid, key, null, time, null, null, client.getHandler(tid), 0l, null);
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
        ByteString response = get(tid, pid, key, idxName, time, null, null, th, 0l, null);
        if (response == null || response.isEmpty()) {
            return null;
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
    public Object[] getRow(String tname, Object[] keyArr, String idxName, long time, String tsName, Tablet.GetType type) throws TimeoutException, TabletException {
        return getRow(tname, keyArr, idxName, time, tsName, type, 0l, null);
    }

    @Override
    public Object[] getRow(String tname, Map<String, Object> keyMap, String idxName, long time, String tsName, Tablet.GetType type) throws TimeoutException, TabletException {
        return getRow(tname, keyMap, idxName, time, tsName, type, 0l, null);
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
        return getRow(tname, key, idxName, time, null, null);
    }

    @Override
    public Object[] getRow(String tname, String key, String idxName, long time, Tablet.GetType type) throws TimeoutException, TabletException {
        return getRow(tname, key, idxName, time, null, type);
    }

    @Override
    public Object[] getRow(String tname, String key, String idxName, long time, String tsName, Tablet.GetType type) throws TimeoutException, TabletException {
        return getRow(tname, key, idxName, time, tsName, type, 0l, null);
    }

    @Override
    public Object[] getRow(String tname, String key, String idxName, long time, String tsName, Tablet.GetType type,
                           long et, Tablet.GetType etType) throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tname);
        if (th == null) {
            throw new TabletException("no table with name " + tname);
        }
        key = validateKey(key);
        GetOption getOption = new GetOption();
        getOption.setEt(et);
        getOption.setEtType(etType);
        getOption.setStType(type);
        getOption.setTsName(tsName);
        getOption.setIdxName(idxName);
        return getRow(key, time, getOption, th);
    }

    @Override
    public Object[] getRow(String tname, Object[] keyArr, String idxName, long time, String tsName, Tablet.GetType type,
                           long et, Tablet.GetType etType) throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tname);
        if (th == null) {
            throw new TabletException("no table with name " + tname);
        }
        List<String> list = th.getKeyMap().get(idxName);
        if (list == null) {
            throw new TabletException("no index name in table" + idxName);
        }
        if (keyArr.length != list.size()) {
            throw new TabletException("check key number failed");
        }
        String combinedKey = TableClientCommon.getCombinedKey(keyArr, client.getConfig().isHandleNull());
        GetOption option = new GetOption();
        option.setStType(type);
        option.setEtType(etType);
        option.setIdxName(idxName);
        option.setTsName(tsName);
        option.setEt(et);
        return getRow(combinedKey, time, option, th);
    }

    @Override
    public Object[] getRow(String tname, Map<String, Object> keyMap, String idxName, long time, String tsName,
                           Tablet.GetType type, long et, Tablet.GetType etType) throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tname);
        if (th == null) {
            throw new TabletException("no table with name " + tname);
        }
        List<String> list = th.getKeyMap().get(idxName);
        if (list == null) {
            throw new TabletException("no index name in table" + idxName);
        }
        String combinedKey = TableClientCommon.getCombinedKey(keyMap, list, client.getConfig().isHandleNull());
        GetOption option = new GetOption();
        option.setStType(type);
        option.setEtType(etType);
        option.setIdxName(idxName);
        option.setTsName(tsName);
        option.setEt(et);
        return getRow(combinedKey, time, option, th);
    }

    private Object[] getRow(String key, long time, GetOption option, TableHandler th) throws TabletException {
        if (th.GetPartitionKeyList().isEmpty() || TableClientCommon.isQueryByPartitionKey(option.getIdxName(), th)) {
            int pid = TableClientCommon.computePidByKey(key, th.getPartitions().length);
            return get(pid, key, time, option, th);
        }

        List<GetFuture> futureList = TableClientCommon.getInternal(key, time, option, th);
        GetFuture realFuture = null;
        long maxTS = 0;
        int notFountCnt = 0;
        try {
            for (GetFuture getFuture : futureList) {
                Tablet.GetResponse response = getFuture.getResponse();
                if (response == null) {
                    throw new TabletException("response is null");
                }
                if (response.getCode() == 0) {
                    if (response.getTs() > maxTS && !response.getValue().isEmpty()) {
                        realFuture = getFuture;
                        maxTS = response.getTs();
                    }
                } else if (response.getCode() == 109) {
                    notFountCnt++;
                }

            }
            if (realFuture == null) {
                if (notFountCnt == futureList.size()) {
                    return null;
                } else {
                    throw new TabletException("bad request error");
                }
            } else {
                return realFuture.getRowWithNoWait();
            }
        } catch (ExecutionException e) {
            throw new TabletException("rtidb internal server error");
        } catch (InterruptedException e) {
            throw new TabletException("rtidb internal server error");
        }
    }

    @Override
    public RelationalIterator traverse(String tableName, ReadOption ro) throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tableName);
        if (th == null) {
            throw new TabletException("no table with name " + tableName);
        }
        return new RelationalIterator(client, th, ro);
    }

    @Override
    public RelationalIterator batchQuery(String tableName, List<ReadOption> ros) throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tableName);
        if (th == null) {
            throw new TabletException("no table with name " + tableName);
        }
        if (ros == null || ros.size() < 1) {
            throw new TabletException("read option list size is 0");
        }

        return new RelationalIterator(client, th, ros);
    }

    @Override
    public RelationalIterator query(String tableName, ReadOption ro) throws TimeoutException, TabletException {
        if (ro == null || ro.getIndex().isEmpty()) {
            throw new TabletException("ro should not be null with name" + tableName);
        }
        TableHandler th = client.getHandler(tableName);
        if (th == null) {
            throw new TabletException("no table with name " + tableName);
        }

        List<ReadOption> ros = new ArrayList<>();
        ros.add(ro);
        return new RelationalIterator(client, th, ros);
    }

    @Override
    public Object[] getRow(String tname, String key, long time, Tablet.GetType type) throws TimeoutException, TabletException {
        return getRow(tname, key, null, time, null, type);
    }

    @Override
    public ByteString get(String tname, String key, long time) throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tname);
        if (th == null) {
            throw new TabletException("no table with name " + tname);
        }

        key = validateKey(key);
        int pid = TableClientCommon.computePidByKey(key, th.getPartitions().length);
        ByteString response = get(th.getTableInfo().getTid(), pid, key, null, time, null, null, th, 0l, null);
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

    private Object[] get(int pid, String key, long time, GetOption getOption, TableHandler th) throws TabletException {
        key = validateKey(key);
        PartitionHandler ph = th.getHandler(pid);
        TabletServer ts = ph.getReadHandler(th.getReadStrategy());
        if (ts == null) {
            throw new TabletException("Cannot find available tabletServer with tid " + th.getTableInfo().getTid());
        }
        List<ColumnDesc> schema = th.getSchema();
        Tablet.GetRequest.Builder builder = Tablet.GetRequest.newBuilder();
        builder.setTid(th.getTableInfo().getTid());
        builder.setPid(pid);
        builder.setKey(key);
        builder.setTs(time);
        builder.setEt(getOption.getEt());
        boolean isNewFormat = false;
        if (th.getFormatVersion() == 1) {
            if (getOption.getProjection().size() > 0) {
                schema = new ArrayList<>();
                for (String name : getOption.getProjection()) {
                    Integer idx = th.getSchemaPos().get(name);
                    if (idx == null) {
                        throw new TabletException("Cannot find column " + name);
                    }
                    builder.addProjection(idx);
                    schema.add(th.getSchema().get(idx));
                }
            }
            isNewFormat = true;
        }
        if (getOption.getStType() != null) builder.setType(getOption.getStType());
        if (getOption.getEtType() != null) builder.setEtType(getOption.getEtType());
        if (getOption.getIdxName() != null && !getOption.getIdxName().isEmpty()) {
            builder.setIdxName(getOption.getIdxName());
        }
        if (getOption.getTsName() != null && !getOption.getTsName().isEmpty()) {
            builder.setTsName(getOption.getTsName());
        }
        Tablet.GetRequest request = builder.build();
        Tablet.GetResponse response = ts.get(request);
        if (response != null && response.getCode() == 0) {
            ByteString bs = null;
            if (th.getTableInfo().hasCompressType() && th.getTableInfo().getCompressType() == NS.CompressType.kSnappy) {
                byte[] uncompressed = Compress.snappyUnCompress(response.getValue().toByteArray());
                bs = ByteString.copyFrom(uncompressed);
            } else {
                bs = response.getValue();
            }
            if (isNewFormat) {

                RowView rv = new RowView(schema);
                return rv.read(bs.asReadOnlyByteBuffer().order(ByteOrder.LITTLE_ENDIAN));
            } else {
                if (getOption.getProjection().size() > 0) {
                    BitSet bset = new BitSet(th.getSchema().size());
                    List<Integer> pschema = new ArrayList<>();
                    int maxIndex = -1;
                    for (String name : getOption.getProjection()) {
                        Integer idx = th.getSchemaPos().get(name);
                        if (idx == null) {
                            throw new TabletException("Cannot find column " + name);
                        }
                        bset.set(idx, true);
                        if (idx > maxIndex) {
                            maxIndex = idx;
                        }
                        pschema.add(idx);
                    }
                    ProjectionInfo projectionInfo = new ProjectionInfo(pschema, bset, maxIndex);
                    return RowCodec.decode(bs.asReadOnlyByteBuffer().order(ByteOrder.LITTLE_ENDIAN), schema, projectionInfo);
                } else {
                    if (th.getSchemaMap().size() > 0) {
                        return RowCodec.decode(bs.asReadOnlyByteBuffer().order(ByteOrder.LITTLE_ENDIAN), schema, th.getSchemaMap().size());
                    } else {
                        return RowCodec.decode(bs.asReadOnlyByteBuffer().order(ByteOrder.LITTLE_ENDIAN), schema);
                    }
                }
            }
        }
        if (response != null) {
            if (response.getCode() == 109) {
                return null;
            }
            throw new TabletException(response.getCode(), response.getMsg());
        }
        return null;
    }

    private ByteString get(int tid, int pid, String key, String idxName, long time, String tsName, Tablet.GetType type,
                           TableHandler th,
                           long et, Tablet.GetType etType) throws TabletException {
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
        builder.setEt(et);
        if (type != null) builder.setType(type);
        if (etType != null) builder.setEtType(etType);
        if (idxName != null && !idxName.isEmpty()) {
            builder.setIdxName(idxName);
        }
        if (tsName != null && !tsName.isEmpty()) {
            builder.setTsName(tsName);
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
            if (response.getCode() == 109) {
                return null;
            }
            throw new TabletException(response.getCode(), response.getMsg());
        }
        return null;
    }

    @Override
    public int count(String tname, String key) throws TimeoutException, TabletException {
        return count(tname, key, null, null, false);
    }

    @Override
    public int count(String tname, String key, boolean filter_expired_data) throws TimeoutException, TabletException {
        return count(tname, key, null, null, filter_expired_data);
    }

    @Override
    public int count(String tname, String key, String idxName) throws TimeoutException, TabletException {
        return count(tname, key, idxName, null, false);
    }

    @Override
    public int count(String tname, String key, String idxName, boolean filter_expired_data) throws TimeoutException, TabletException {
        return count(tname, key, idxName, null, filter_expired_data);
    }

    @Override
    public int count(String tname, String key, long st, long et) throws TimeoutException, TabletException {
        return count(tname, key, null, st, et);
    }

    @Override
    public int count(String tname, String key, String idxName, long st, long et) throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tname);
        if (th == null) {
            throw new TabletException("no table with name " + tname);
        }
        key = validateKey(key);
        return count(key, idxName, null, true, st, et, th);
    }

    @Override
    public int count(String tname, String key, String idxName, String tsName, long st, long et) throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tname);
        if (th == null) {
            throw new TabletException("no table with name " + tname);
        }
        key = validateKey(key);
        return count(key, idxName, tsName, true, st, et, th);
    }


    @Override
    public int count(String tname, String key, String idxName, String tsName, boolean filter_expired_data)
            throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tname);
        if (th == null) {
            throw new TabletException("no table with name " + tname);
        }
        key = validateKey(key);
        return count(key, idxName, tsName, filter_expired_data, 0, 0, th);
    }

    @Override
    public int count(String tname, Map<String, Object> keyMap, String idxName, String tsName, boolean filter_expired_data)
            throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tname);
        if (th == null) {
            throw new TabletException("no table with name " + tname);
        }
        List<String> list = th.getKeyMap().get(idxName);
        if (list == null) {
            throw new TabletException("no index name in table" + idxName);
        }
        String combinedKey = TableClientCommon.getCombinedKey(keyMap, list, client.getConfig().isHandleNull());
        return count(combinedKey, idxName, tsName, filter_expired_data, 0 ,0, th);
    }

    @Override
    public int count(String tname, Map<String, Object> keyMap, String idxName, String tsName, long st, long et)
            throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tname);
        if (th == null) {
            throw new TabletException("no table with name " + tname);
        }
        List<String> list = th.getKeyMap().get(idxName);
        if (list == null) {
            throw new TabletException("no index name in table" + idxName);
        }
        String combinedKey = TableClientCommon.getCombinedKey(keyMap, list, client.getConfig().isHandleNull());
        return count(combinedKey, idxName, tsName,  true, st, et, th);
    }

    @Override
    public int count(String tname, Object[] keyArr, String idxName, String tsName, boolean filter_expired_data)
            throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tname);
        if (th == null) {
            throw new TabletException("no table with name " + tname);
        }
        List<String> list = th.getKeyMap().get(idxName);
        if (list == null) {
            throw new TabletException("no index name in table" + idxName);
        }
        if (keyArr.length != list.size()) {
            throw new TabletException("check key number failed");
        }
        String combinedKey = TableClientCommon.getCombinedKey(keyArr, client.getConfig().isHandleNull());
        return count(combinedKey, idxName, tsName, filter_expired_data, 0, 0, th);
    }

    private int count(String key, String idxName, String tsName, boolean filter_expired_data, long st, long et, TableHandler th)
        throws TabletException, TimeoutException {
        if (th.GetPartitionKeyList().isEmpty()) {
            int pid = TableClientCommon.computePidByKey(key, th.getPartitions().length);
            return count(th.getTableInfo().getTid(), pid, key, idxName, tsName, filter_expired_data, th, st, et);
        } else {
            int num = 0;
            for (int pid = 0; pid < th.getPartitions().length; pid++) {
                num += count(th.getTableInfo().getTid(), pid, key, idxName, tsName, filter_expired_data, th, st, et);
            }
            return num;
        }
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
        return count(tid, pid, key, idxName, null, filter_expired_data, th, 0, 0);
    }

    private int count(int tid, int pid, String key, String idxName, String tsName, boolean filter_expired_data,
                      TableHandler th, long st, long et) throws TimeoutException, TabletException {
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
        if (client.getConfig().isRemoveDuplicateByTime()) {
            builder.setEnableRemoveDuplicatedRecord(true);
        }
        builder.setEt(et);
        builder.setSt(st);
        if (idxName != null && !idxName.isEmpty()) {
            builder.setIdxName(idxName);
        }
        if (tsName != null && !tsName.isEmpty()) {
            builder.setTsName(tsName);
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
    public UpdateResult delete(String tableName, Map<String, Object> conditionColumns) throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tableName);
        if (th == null) {
            throw new TabletException("no table with name " + tableName);
        }

        int tid = th.getTableInfo().getTid();
        int pid = 0;
        PartitionHandler ph = th.getHandler(pid);
        TabletServer ts = ph.getLeader();
        if (ts == null) {
            throw new TabletException("Cannot find available tabletServer with tid " + tid);
        }
        Tablet.DeleteRequest.Builder builder = Tablet.DeleteRequest.newBuilder();
        builder.setTid(tid);
        builder.setPid(pid);
        Map<String, DataType> nameTypeMap = th.getNameTypeMap();
        {
            String colName = "";
            Object colValue = "";
            Iterator<Map.Entry<String, Object>> iter = conditionColumns.entrySet().iterator();
            while (iter.hasNext()) {
                Tablet.Columns.Builder conditionBuilder = Tablet.Columns.newBuilder();
                Map.Entry<String, Object> entry = iter.next();
                colName = entry.getKey();
                colValue = entry.getValue();
                if (!nameTypeMap.containsKey(colName)) {
                    throw new TabletException("index name not found with tid " + tid);
                }
                conditionBuilder.addName(colName);
                DataType dataType = nameTypeMap.get(colName);
                ByteBuffer buffer = FieldCodec.convert(dataType, colValue);
                if (buffer != null) {
                    conditionBuilder.setValue(ByteBufferNoCopy.wrap(buffer));
                }
                builder.addConditionColumns(conditionBuilder.build());
            }
        }
        BlobServer bs = th.getBlobServer();
        if (bs != null) {
            builder.setReceiveBlobs(true);
        }
        Tablet.DeleteRequest request = builder.build();
        Tablet.GeneralResponse response = ts.delete(request);
        if (response != null && response.getCode() == 0) {
            deleteBlobByList(th, response.getAdditionalIdsList());
            return new UpdateResult(true, response.getCount());
        }
        if (response != null) {
            throw new TabletException(response.getCode(), response.getMsg());
        }
        return new UpdateResult(false);
    }

    boolean deleteBlobByMap(TableHandler th, Map<String, Long> blobKeys) {
        if (blobKeys.isEmpty()) {
            return true;
        }
        List<Long> keys = new ArrayList<Long>();
        for (String key : blobKeys.keySet()) {
            Long blobKey = blobKeys.get(key);
            keys.add(blobKey);
        }
        return deleteBlobByList(th, keys);
    }

    boolean deleteBlobByList(TableHandler th, List<Long> keys) {
        int tid = th.getTableInfo().getTid();
        BlobServer bs = th.getBlobServer();
        if (bs == null) {
            return false;
        }
        for (Long key : keys) {
            OSS.DeleteRequest.Builder builder = OSS.DeleteRequest.newBuilder();
            builder.setTid(tid);
            builder.setPid(0);
            builder.setKey(key);

            OSS.DeleteRequest request = builder.build();
            bs.delete(request);
        }
        return true;
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
        if (th.GetPartitionKeyList().isEmpty()) {
            int pid = TableClientCommon.computePidByKey(key, th.getPartitions().length);
            return delete(th.getTableInfo().getTid(), pid, key, idxName, th);
        } else {
            for (int pid = 0; pid < th.getPartitions().length; pid++) {
                if (!delete(th.getTableInfo().getTid(), pid, key, idxName, th)) {
                    return  false;
                }
            }
            return true;
        }

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
        ScanOption scanOption = new ScanOption();
        scanOption.setLimit(limit);
        scanOption.setIdxName(idxName);
        scanOption.setRemoveDuplicateRecordByTime(client.getConfig().isRemoveDuplicateByTime());
        return scan(tid, pid, key, st, et, th, scanOption);
    }

    @Override
    public KvIterator scan(int tid, int pid, String key, String idxName, int limit) throws TimeoutException, TabletException {
        return scan(tid, pid, key, idxName, 0, 0,limit);
    }

    @Override
    public KvIterator scan(String tname, String key, long st, long et) throws TimeoutException, TabletException {

        return scan(tname, key, null, st, et, null,0);
    }

    @Override
    public KvIterator scan(String tname, String key, int limit) throws TimeoutException, TabletException {
        return scan(tname, key, null, 0, 0, null, limit);
    }

    @Override
    public KvIterator scan(String tname, String key, long st, long et, int limit) throws TimeoutException, TabletException {
        return scan(tname, key, null, st, et, null, limit);
    }

    @Override
    public KvIterator scan(String tname, String key, String idxName, long st, long et)
            throws TimeoutException, TabletException {
        return scan(tname, key, idxName, st, et, null,0);
    }

    @Override
    public KvIterator scan(String tname, String key, String idxName, long st, long et, int limit) throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tname);
        if (th == null) {
            throw new TabletException("no table with name " + tname);
        }
        return scan(tname, key, idxName, st, et, null, limit);
    }

    @Override
    public KvIterator scan(String tname, String key, String idxName, int limit) throws TimeoutException, TabletException {
        return scan(tname, key, idxName, 0, 0, null, limit);
    }

    @Override
    public KvIterator scan(String tname, String key, String idxName, long st, long et, String tsName, int limit)
            throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tname);
        if (th == null) {
            throw new TabletException("no table with name " + tname);
        }
        ScanOption scanOption = new ScanOption();
        scanOption.setLimit(limit);
        scanOption.setIdxName(idxName);
        scanOption.setTsName(tsName);
        scanOption.setRemoveDuplicateRecordByTime(client.getConfig().isRemoveDuplicateByTime());
        return scan(tname, key, st, et, scanOption);
    }

    @Override
    public KvIterator scan(String tname, Object[] keyArr, String idxName, long st, long et, String tsName, int limit)
            throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tname);
        if (th == null) {
            throw new TabletException("no table with name " + tname);
        }
        List<String> list = th.getKeyMap().get(idxName);
        if (list == null) {
            throw new TabletException("no index name in table" + idxName);
        }
        if (keyArr.length != list.size()) {
            throw new TabletException("check key number failed");
        }
        String combinedKey = TableClientCommon.getCombinedKey(keyArr, client.getConfig().isHandleNull());
        return scan(tname, combinedKey, idxName, st, et, tsName, limit);
    }

    @Override
    public KvIterator scan(String tname, Map<String, Object> keyMap,
                           String idxName, long st, long et, String tsName, int limit)
            throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tname);
        if (th == null) {
            throw new TabletException("no table with name " + tname);
        }
        List<String> list = th.getKeyMap().get(idxName);
        if (list == null) {
            throw new TabletException("no index name in table" + idxName);
        }
        String combinedKey = TableClientCommon.getCombinedKey(keyMap, list, client.getConfig().isHandleNull());
        return scan(tname, combinedKey, idxName, st, et, tsName, limit);
    }

    @Override
    public KvIterator traverse(String tname, String idxName, String tsName) throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tname);
        if (th == null) {
            throw new TabletException("no table with name " + tname);
        }
        TraverseKvIterator it = new TraverseKvIterator(client, th, idxName, tsName);
        it.next();
        return it;
    }

    @Override
    public KvIterator traverse(String tname, String idxName) throws TimeoutException, TabletException {
        return traverse(tname, idxName, null);
    }

    @Override
    public KvIterator traverse(String tname) throws TimeoutException, TabletException {
        return traverse(tname, null, null);
    }

    @Override
    public KvIterator scan(int tid, int pid, Object[] keyArr, String idxName, long st, long et, String tsName)
            throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tid);
        if (th == null) {
            throw new TabletException("no table with tid" + tid);
        }
        List<String> list = th.getKeyMap().get(idxName);
        if (list == null) {
            throw new TabletException("no index name in table" + idxName);
        }
        if (keyArr.length != list.size()) {
            throw new TabletException("check key number failed");
        }
        String combinedKey = TableClientCommon.getCombinedKey(keyArr, client.getConfig().isHandleNull());
        ScanOption scanOption = new ScanOption();
        scanOption.setIdxName(idxName);
        scanOption.setTsName(tsName);
        scanOption.setRemoveDuplicateRecordByTime(client.getConfig().isRemoveDuplicateByTime());
        return scan(tid, pid, combinedKey, st, et, th, scanOption);
    }

    @Override
    public KvIterator scan(int tid, int pid, Map<String, Object> keyMap, String idxName,
                           long st, long et, String tsName, int limit) throws TimeoutException, TabletException {
        return scan(tid, pid, keyMap, idxName, st, et, tsName, limit, 0);
    }

    @Override
    public KvIterator scan(int tid, int pid, Map<String, Object> keyMap, String idxName,
                           long st, long et, String tsName, int limit, int atLeast) throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tid);
        if (th == null) {
            throw new TabletException("no table with tid" + tid);
        }
        List<String> list = th.getKeyMap().get(idxName);
        if (list == null) {
            throw new TabletException("no index name in table" + idxName);
        }
        String combinedKey = TableClientCommon.getCombinedKey(keyMap, list, client.getConfig().isHandleNull());
        ScanOption scanOption = new ScanOption();
        scanOption.setIdxName(idxName);
        scanOption.setTsName(tsName);
        scanOption.setLimit(limit);
        scanOption.setAtLeast(atLeast);
        scanOption.setRemoveDuplicateRecordByTime(client.getConfig().isRemoveDuplicateByTime());
        return scan(tid, pid, combinedKey, st, et, th, scanOption);
    }

    private KvIterator scan(int tid, int pid, String key, long st, long et, TableHandler th, ScanOption option) throws TimeoutException, TabletException {
        key = validateKey(key);
        PartitionHandler ph = th.getHandler(pid);
        TabletServer ts = ph.getReadHandler(th.getReadStrategy());
        if (ts == null) {
            throw new TabletException("Cannot find available tabletServer with tid " + tid);
        }
        Tablet.ScanRequest.Builder builder = Tablet.ScanRequest.newBuilder();
        builder.setPk(key);
        List<ColumnDesc> schema = th.getSchema();
        boolean isNewFormat = false;
        // the new format version
        if (th.getFormatVersion() == 1) {
            if (option.getProjection().size() > 0) {
                schema = new ArrayList<>();
                for (String name : option.getProjection()) {
                    Integer idx = th.getSchemaPos().get(name);
                    if (idx == null) {
                        throw new TabletException("Cannot find column " + name);
                    }
                    builder.addProjection(idx);
                    schema.add(th.getSchema().get(idx));
                }
            }
            isNewFormat = true;
        }
        builder.setTid(tid);
        builder.setEt(et);
        builder.setSt(st);
        builder.setPid(pid);
        builder.setLimit(option.getLimit());
        builder.setAtleast(option.getAtLeast());
        if (option.getIdxName() != null)
            builder.setIdxName(option.getIdxName());
        if (option.getTsName() != null)
            builder.setTsName(option.getTsName());
        builder.setEnableRemoveDuplicatedRecord(option.isRemoveDuplicateRecordByTime());
        Tablet.ScanRequest request = builder.build();
        Tablet.ScanResponse response = ts.scan(request);
        if (response != null && response.getCode() == 0) {
            if (isNewFormat) {
                RowKvIterator rit = new RowKvIterator(response.getPairs(), schema, response.getCount());
                if (th.getTableInfo().hasCompressType()) {
                    rit.setCompressType(th.getTableInfo().getCompressType());
                }
                return rit;
            } else {
                if (option.getProjection().size() > 0) {
                    BitSet bset = new BitSet(th.getSchema().size());
                    List<Integer> pschema = new ArrayList<>();
                    int maxIndex = -1;
                    for (String name : option.getProjection()) {
                        Integer idx = th.getSchemaPos().get(name);
                        if (idx == null) {
                            throw new TabletException("Cannot find column " + name);
                        }
                        bset.set(idx, true);
                        if (idx > maxIndex) {
                            maxIndex = idx;
                        }
                        pschema.add(idx);
                    }
                    ProjectionInfo projectionInfo = new ProjectionInfo(pschema, bset, maxIndex);
                    DefaultKvIterator it = new DefaultKvIterator(response.getPairs(), schema, projectionInfo);
                    it.setCount(response.getCount());
                    if (th.getTableInfo().hasCompressType()) {
                        it.setCompressType(th.getTableInfo().getCompressType());
                    }
                    return it;
                } else {
                    DefaultKvIterator it = new DefaultKvIterator(response.getPairs(), th);
                    it.setCount(response.getCount());
                    if (th.getTableInfo().hasCompressType()) {
                        it.setCompressType(th.getTableInfo().getCompressType());
                    }
                    return it;
                }
            }

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
        if (!th.getSchema().isEmpty()) {
            throw new TabletException("fail to put the schema table " + th.getTableInfo().getName() + " in the way of putting kv table");
        }
        key = validateKey(key);
        int pid = TableClientCommon.computePidByKey(key, th.getPartitions().length);
        return put(th.getTableInfo().getTid(), pid, key, time, bytes, th);
    }

    @Override
    public boolean put(String name, long time, Object[] row) throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(name);
        if (th == null) {
            throw new TabletException("no table with name " + name);
        }
        if (th.hasTsCol()) {
//            throw new TabletException("has ts column. should not set time");
            return put(name, row);
        }
        return put(name, time, row, null);
    }

    @Override
    public boolean put(String name, Object[] row) throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(name);
        if (th == null) {
            throw new TabletException("no table with name " + name);
        }
        if (row.length > th.getSchema().size() + th.getSchemaMap().size()) {
            row = Arrays.copyOf(row, th.getSchema().size() + th.getSchemaMap().size());
        }
        long ts = 0;
        List<Tablet.TSDimension> tsDimensions = TableClientCommon.parseArrayInput(row, th);
        if (tsDimensions.size() <= 0) {
            ts = System.currentTimeMillis();
        }
        return put(name, ts, row, tsDimensions);
    }

    private boolean put(String name, long time, Object[] row, List<Tablet.TSDimension> ts) throws TimeoutException, TabletException {
        boolean handleNull = client.getConfig().isHandleNull();
        TableHandler th = client.getHandler(name);
        if (th == null) {
            throw new TabletException("no table with name " + name);
        }
        if (row == null) {
            throw new TabletException("putting data is null");
        }
        ByteBuffer buffer = null;
        if (row.length == th.getSchema().size()) {
            switch (th.getFormatVersion()) {
                case 1:
                    buffer = RowBuilder.encode(row, th.getSchema());
                    break;
                default:
                    buffer = RowCodec.encode(row, th.getSchema());
            }
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
        if (!th.GetPartitionKeyList().isEmpty()) {
            List<Tablet.Dimension> dims = TableClientCommon.fillTabletDimension(row, th, handleNull);
            int pid = TableClientCommon.computePidByKey(TableClientCommon.combinePartitionKey(row, th.GetPartitionKeyList(),
                    th.getPartitions().length), th.getPartitions().length);
            return put(th.getTableInfo().getTid(), pid, null, time, dims,ts, buffer, th);
        } else {
            Map<Integer, List<Tablet.Dimension>> mapping = TableClientCommon.fillPartitionTabletDimension(row, th, handleNull);
            Iterator<Map.Entry<Integer, List<Tablet.Dimension>>> it = mapping.entrySet().iterator();
            boolean ret = true;
            while (it.hasNext()) {
                Map.Entry<Integer, List<Tablet.Dimension>> entry = it.next();
                ret = ret && put(th.getTableInfo().getTid(), entry.getKey(), null,
                        time, entry.getValue(), ts, buffer, th);
            }
            return ret;
        }
    }

    private boolean put(int tid, int pid, String key, long time, byte[] bytes, TableHandler th) throws TabletException {
        return put(tid, pid, key, time, null, null, ByteBuffer.wrap(bytes), th);
    }

    private PutResult putRelationTable(int tid, int pid, ByteBuffer row, TableHandler th, WriteOption wo) throws TabletException {
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
        TabletServer tablet = ph.getLeader();
        if (tablet == null) {
            throw new TabletException("Cannot find available tabletServer with tid " + tid);
        }
        Tablet.PutRequest.Builder builder = Tablet.PutRequest.newBuilder();
        builder.setPid(pid);
        builder.setTid(tid);
        row.rewind();
        builder.setValue(ByteBufferNoCopy.wrap(row.asReadOnlyBuffer()));
        if (wo != null) {
            Tablet.WriteOption.Builder woBuilder = Tablet.WriteOption.newBuilder();
            woBuilder.setUpdateIfExist(wo.isUpdateIfExist());
            builder.setWo(woBuilder.build());
        }
        Tablet.PutRequest request = builder.build();
        Tablet.PutResponse response = tablet.put(request);
        if (response != null) {
            if (response.getCode() == 0) {
                if (!th.getAutoGenPkName().isEmpty() && response.hasAutoGenPk()) {
                    return new PutResult(true, response.getAutoGenPk());
                } else {
                    return new PutResult(true);
                }
            } else {
                throw new TabletException(response.getCode(), response.getMsg());
            }
        } else {
            return new PutResult(false);
        }
    }

    private boolean putObjectStore(int tid, ByteBuffer row, long[] autoKey, TableHandler th) throws TabletException {
        if (autoKey.length < 1) {
            throw new TabletException("auto gen key array size must greather 1");
        }
        BlobServer bs = th.getBlobServer();
        if (bs == null) {
            throw new TabletException("can not found available blobserver with tid " + tid);
        }
        OSS.PutRequest.Builder builder = OSS.PutRequest.newBuilder();
        builder.setTid(tid);
        builder.setPid(0);
        row.rewind();
        builder.setData(ByteBufferNoCopy.wrap(row.asReadOnlyBuffer()));

        OSS.PutRequest request = builder.build();
        OSS.PutResponse response = bs.put(request);
        if (response != null && response.getCode() == 0) {
            autoKey[0] = response.getKey();
            return true;
        }

        return false;

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
        TabletServer tablet = ph.getLeader();
        if (tablet == null) {
            throw new TabletException("Cannot find available tabletServer with tid " + tid);
        }
        Tablet.PutRequest.Builder builder = Tablet.PutRequest.newBuilder();
        builder.setPid(pid);
        builder.setTid(tid);
        builder.setFormatVersion(th.getFormatVersion());
        if (time != 0) {
            builder.setTime(time);
        }
        if (ts != null) {
            for (Tablet.TSDimension tsDim : ts) {
                builder.addTsDimensions(tsDim);
            }
        }
        if (key != null) {
            builder.setPk(key);
        }
        if (ds != null) {
            for (Tablet.Dimension dim : ds) {
                builder.addDimensions(dim);
            }
        }
        row.rewind();
        builder.setValue(ByteBufferNoCopy.wrap(row.asReadOnlyBuffer()));
        Tablet.PutRequest request = builder.build();
        Tablet.PutResponse response = tablet.put(request);
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
        if (row == null) {
            throw new TabletException("putting data is null");
        }
        List<Tablet.TSDimension> tsDimensions = new ArrayList<Tablet.TSDimension>();
        Object[] arrayRow = null;
        if (row.size() > th.getSchema().size() && th.getSchemaMap().size() > 0) {
            if (row.size() > th.getSchema().size() + th.getSchemaMap().size()) {
                arrayRow = new Object[th.getSchema().size() + th.getSchemaMap().size()];
            } else {
                arrayRow = new Object[row.size()];
            }
        } else {
            arrayRow = new Object[th.getSchema().size()];
        }
        long ts = 0;
        TableClientCommon.parseMapInput(row, th, arrayRow, tsDimensions);
        if (tsDimensions.size() == 0) {
            ts = System.currentTimeMillis();
        }
        return put(tname, ts, arrayRow, tsDimensions);
    }

    @Override
    public PutResult put(String tname, Map<String, Object> row, WriteOption wo) throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tname);
        if (th == null) {
            throw new TabletException("no table with name " + tname);
        }
        if (row == null) {
            throw new TabletException("putting data is null");
        }
        //TODO: resolve wo

        String indexName = th.getAutoGenPkName();

        if (!indexName.isEmpty() &&
                (row.size() == th.getSchema().size() || row.containsKey(indexName))) {
            throw new TabletException("should not input autoGenPk column");
        }
        if (!indexName.isEmpty()) {
            row.put(indexName, RowCodecCommon.DEFAULT_LONG);
        }

        ByteBuffer buffer;
        List<ColumnDesc> schema;
        if (row.size() == th.getSchema().size()) {
            schema = th.getSchema();
        } else {
            schema = th.getSchemaMap().get(row.size());
            if (schema == null || schema.isEmpty()) {
                throw new TabletException("no schema for column count " + row.size());
            }
        }
        Map<String, Long> blobKeys = new HashMap<String, Long>();
        if (th.getBlobServer() != null) {
            if (!th.IsObjectTable()) {
                putObjectStore(row, th, blobKeys);
            }
        }
        if (blobKeys.isEmpty()) {
            buffer = RowBuilder.encode(row, schema);
        } else {
            buffer = RowBuilder.encode(row, schema, blobKeys);
        }

        int pid = 0;
        /*
        TODO: for distributed
        if (indexName.isEmpty()) {
            String pk = RowCodecCommon.getPrimaryKey(row, th.getTableInfo().getColumnKeyList(), schema);
            pid = TableClientCommon.computePidByKey(pk, th.getPartitions().length);
        } else {
            pid = new Random().nextInt() % th.getPartitions().length;
        }
        */
        try {
            return putRelationTable(th.getTableInfo().getTid(), pid, buffer, th, wo);
        } catch (Exception e) {
            deleteBlobByMap(th, blobKeys);
            throw e;
        }


    }

    private UpdateResult updateRequest(TableHandler th, int pid, Map<String, Object> conditionColumns, List<ColumnDesc> newValueSchema,
                                       ByteBuffer valueBuffer) throws TabletException {
        PartitionHandler ph = th.getHandler(pid);
        if (th.getTableInfo().hasCompressType() && th.getTableInfo().getCompressType() == NS.CompressType.kSnappy) {
            byte[] data = valueBuffer.array();
            byte[] compressed = Compress.snappyCompress(data);
            if (compressed == null) {
                throw new TabletException("snappy compress error");
            }
            valueBuffer = ByteBuffer.wrap(compressed);
        }
        TabletServer tablet = ph.getLeader();
        int tid = th.getTableInfo().getTid();
        if (tablet == null) {
            throw new TabletException("Cannot find available tabletServer with tid " + tid);
        }
        Tablet.UpdateRequest.Builder builder = Tablet.UpdateRequest.newBuilder();
        builder.setTid(tid);
        builder.setPid(pid);
        Map<String, DataType> nameTypeMap = th.getNameTypeMap();
        {
            String colName = "";
            Object colValue = "";
            Iterator<Map.Entry<String, Object>> iter = conditionColumns.entrySet().iterator();
            while (iter.hasNext()) {
                Tablet.Columns.Builder conditionBuilder = Tablet.Columns.newBuilder();
                Map.Entry<String, Object> entry = iter.next();
                colName = entry.getKey();
                colValue = entry.getValue();
                if (!nameTypeMap.containsKey(colName)) {
                    throw new TabletException("index name not found with tid " + tid);
                }
                conditionBuilder.addName(colName);
                DataType dataType = nameTypeMap.get(colName);
                ByteBuffer buffer = FieldCodec.convert(dataType, colValue);
                if (buffer != null) {
                    conditionBuilder.setValue(ByteBufferNoCopy.wrap(buffer));
                }
                builder.addConditionColumns(conditionBuilder.build());
            }
        }
        {
            Tablet.Columns.Builder valueBuilder = Tablet.Columns.newBuilder();
            for (ColumnDesc col : newValueSchema) {
                valueBuilder.addName(col.getName());
            }
            valueBuffer.rewind();
            valueBuilder.setValue(ByteBufferNoCopy.wrap(valueBuffer.asReadOnlyBuffer()));
            builder.setValueColumns(valueBuilder.build());
        }
        Tablet.UpdateRequest request = builder.build();
        Tablet.UpdateResponse response = tablet.update(request);
        if (response != null && response.getCode() == 0) {
            return new UpdateResult(true, response.getCount());
        }
        if (response != null) {
            throw new TabletException(response.getCode(), response.getMsg());
        }
        return new UpdateResult(false);
    }

    private List<ColumnDesc> getSchemaData(Map<String, Object> columns, List<ColumnDesc> schema) {
        List<ColumnDesc> newSchema = new ArrayList<>();
        for (int i = 0; i < schema.size(); i++) {
            ColumnDesc columnDesc = schema.get(i);
            String colName = columnDesc.getName();
            if (columns.containsKey(colName)) {
                newSchema.add(columnDesc);
            }
        }
        return newSchema;
    }

    public void putObjectStore(Map<String, Object> row, TableHandler th, Map<String, Long> blobKeys) throws TimeoutException, TabletException {
        List<ColumnDesc> schema = th.getSchema();
        for (Integer idx : th.getBlobIdxList()) {

            long[] keys = new long[1];
            ColumnDesc colDesc = schema.get(idx);
            if (!row.containsKey(colDesc.getName())) {
                continue;
            }
            Object col = row.get(colDesc.getName());
            if (col == null) {
                if (colDesc.isNotNull()) {
                    throw new TabletException("col " + colDesc.getName() + " should not be null");
                }
                continue;
            }
            boolean ok = putObjectStore(th.getTableInfo().getTid(), (ByteBuffer) col, keys, th);
            if (!ok) {
                throw new TabletException("put blob failed");
            }
            blobKeys.put(colDesc.getName(), keys[0]);
        }
    }

    @Override
    public UpdateResult update(String tableName, Map<String, Object> conditionColumns, Map<String, Object> valueColumns, WriteOption wo)
            throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tableName);
        if (th == null) {
            throw new TabletException("no table with name " + tableName);
        }
        if (conditionColumns == null || conditionColumns.isEmpty()) {
            throw new TabletException("conditionColumns is null or empty");
        }
        if (valueColumns == null || valueColumns.isEmpty()) {
            throw new TabletException("valueColumns is null or empty");
        }
        List<ColumnDesc> newValueSchema = getSchemaData(valueColumns, th.getSchema());
        Map<String, Long> blobKeys = new HashMap<String, Long>();
        if (th.getBlobServer() != null && !th.IsObjectTable()) {
            putObjectStore(valueColumns, th, blobKeys);
        }
        ByteBuffer valueBuffer;
        if (blobKeys.isEmpty()) {
            valueBuffer = RowBuilder.encode(valueColumns, newValueSchema);
        } else {
            valueBuffer = RowBuilder.encode(valueColumns, newValueSchema, blobKeys);
        }
        try {
            return updateRequest(th, 0, conditionColumns, newValueSchema, valueBuffer);
        } catch (Exception e) {
            deleteBlobByMap(th, blobKeys);
            throw e;
        }
    }

    @Override
    public boolean put(String tname, long time, Map<String, Object> row) throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tname);
        if (th == null) {
            throw new TabletException("no table with name " + tname);
        }
        if (th.hasTsCol()) {
//            throw new TabletException("has ts column. should not set time");
            return put(tname, row);
        }
        if (row == null) {
            throw new TabletException("putting data is null");
        }
        Object[] arrayRow = null;
        if (row.size() > th.getSchema().size() && th.getSchemaMap().size() > 0) {
            int columnSize = row.size();
            if (row.size() > th.getSchema().size() + th.getSchemaMap().size()) {
                columnSize = th.getSchema().size() + th.getSchemaMap().size();
            }
            arrayRow = new Object[columnSize];
            List<ColumnDesc> schema = th.getSchemaMap().get(columnSize);
            for (int i = 0; i < schema.size(); i++) {
                arrayRow[i] = row.get(schema.get(i).getName());
            }
        } else {
            arrayRow = new Object[th.getSchema().size()];
            for (int i = 0; i < th.getSchema().size(); i++) {
                arrayRow[i] = row.get(th.getSchema().get(i).getName());
            }
        }
        return put(tname, time, arrayRow);
    }

    @Override
    public List<ColumnDesc> getSchema(String tname) throws TabletException {
        TableHandler th = client.getHandler(tname);
        if (th == null) {
            throw new TabletException("no table with name " + tname);
        }
        if (th.getSchemaMap().size() == 0) {
            return th.getSchema();
        } else {
            return th.getSchemaMap().get(th.getSchema().size() + th.getSchemaMap().size());
        }
    }

    @Override
    public boolean put(int tid, int pid, long time, Map<String, Object> row) throws TimeoutException, TabletException {
        TableHandler th = client.getHandler(tid);
        if (th == null) {
            throw new TabletException("fail to find table with id " + tid);
        }
        if (row == null) {
            throw new TabletException("putting data is null");
        }
        Object[] arrayRow = null;
        if (row.size() > th.getSchema().size() && th.getSchemaMap().size() > 0) {
            int columnSize = row.size();
            if (row.size() > th.getSchema().size() + th.getSchemaMap().size()) {
                columnSize = th.getSchema().size() + th.getSchemaMap().size();
            }
            arrayRow = new Object[columnSize];
            List<ColumnDesc> schema = th.getSchemaMap().get(columnSize);
            for (int i = 0; i < schema.size(); i++) {
                arrayRow[i] = row.get(schema.get(i).getName());
            }
        } else {
            arrayRow = new Object[th.getSchema().size()];
            for (int i = 0; i < th.getSchema().size(); i++) {
                arrayRow[i] = row.get(th.getSchema().get(i).getName());
            }
        }
        return put(tid, pid, time, arrayRow);
    }

    @Override
    public List<ColumnDesc> getSchema(int tid) throws TabletException {
        TableHandler th = client.getHandler(tid);
        if (th == null) {
            throw new TabletException("fail to find table with id " + tid);
        }
        if (th.getSchemaMap().size() == 0) {
            return th.getSchema();
        } else {
            return th.getSchemaMap().get(th.getSchema().size() + th.getSchemaMap().size());
        }
    }


}
