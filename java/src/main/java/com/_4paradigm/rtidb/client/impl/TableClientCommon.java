package com._4paradigm.rtidb.client.impl;

import com._4paradigm.rtidb.client.*;
import com._4paradigm.rtidb.client.ha.PartitionHandler;
import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.ha.TableHandler;
import com._4paradigm.rtidb.client.ha.TabletServerWapper;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.ColumnType;
import com._4paradigm.rtidb.client.schema.ProjectionInfo;
import com._4paradigm.rtidb.tablet.Tablet;
import com._4paradigm.rtidb.utils.MurmurHash;
import io.brpc.client.RpcCallback;
import org.joda.time.DateTime;
import rtidb.api.TabletServer;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.Future;

public class TableClientCommon {

    public static RpcCallback<Tablet.PutResponse> putFakeCallback = new RpcCallback<Tablet.PutResponse>() {

        @Override
        public void success(Tablet.PutResponse response) {
        }

        @Override
        public void fail(Throwable e) {

        }

    };

    public static RpcCallback<Tablet.GetResponse> getFakeCallback = new RpcCallback<Tablet.GetResponse>() {

        @Override
        public void success(Tablet.GetResponse response) {
        }

        @Override
        public void fail(Throwable e) {
        }

    };

    public static RpcCallback<Tablet.ScanResponse> scanFakeCallback = new RpcCallback<Tablet.ScanResponse>() {

        @Override
        public void success(Tablet.ScanResponse response) {
        }

        @Override
        public void fail(Throwable e) {
        }

    };

    public static void parseMapInput(Map<String, Object> row, TableHandler th, Object[] arrayRow, List<Tablet.TSDimension> tsDimensions) throws TabletException {
        if (row == null) {
            throw new TabletException("input row is null");
        }
        if (arrayRow.length < th.getSchema().size()) {
            throw new TabletException("row length mismatch schema");
        }
        tsDimensions.clear();
        int tsIndex = 0;
        List<ColumnDesc> schema;
        if (arrayRow.length > th.getSchema().size() && th.getSchemaMap().size() > 0) {
            schema = th.getSchemaMap().get(arrayRow.length);
        } else {
            schema = th.getSchema();
        }
        boolean hasTsCol = false;
        for (int i = 0; i < schema.size(); i++) {
            ColumnDesc columnDesc = schema.get(i);
            Object colValue = row.get(columnDesc.getName());
            arrayRow[i] = colValue;
            if (columnDesc.isTsCol()) {
                hasTsCol = true;
                int curTsIndex = tsIndex;
                tsIndex++;
                if (colValue == null) {
                    continue;
                }
                if (columnDesc.getType() == ColumnType.kInt64) {
                    tsDimensions.add(Tablet.TSDimension.newBuilder().setIdx(curTsIndex).setTs((Long)colValue).build());
                } else if (columnDesc.getType() == ColumnType.kTimestamp) {
                    if (colValue instanceof Timestamp) {
                        tsDimensions.add(Tablet.TSDimension.newBuilder().setIdx(curTsIndex).
                                setTs(((Timestamp)colValue).getTime()).build());
                    } else if (colValue instanceof DateTime) {
                        tsDimensions.add(Tablet.TSDimension.newBuilder().setIdx(curTsIndex).
                                setTs(((DateTime)colValue).getMillis()).build());
                    } else {
                        throw new TabletException("invalid ts column");
                    }
                } else {
                    throw new TabletException("invalid ts column");
                }
            }
        }
        if (hasTsCol && tsDimensions.isEmpty()) {
            throw new TabletException("no ts column");
        }
    }

    public static List<Tablet.TSDimension> parseArrayInput(Object[] row, TableHandler th) throws TabletException {
        if (row == null) {
            throw new TabletException("input row is null");
        }
        if (row.length < th.getSchema().size()) {
            throw new TabletException("row length mismatch schema");
        }
        List<ColumnDesc> schema;
        if (row.length > th.getSchema().size() && th.getSchemaMap().size() > 0) {
            schema = th.getSchemaMap().get(row.length);
            if (schema == null) {
                throw new TabletException("no schema for column count " + row.length);
            }
        } else {
            schema = th.getSchema();
        }
        List<Tablet.TSDimension> tsDimensions = new ArrayList<Tablet.TSDimension>();
        boolean hasTsCol = false;
        int tsIndex = 0;
        for (int i = 0; i < schema.size(); i++) {
            ColumnDesc columnDesc = schema.get(i);
            Object colValue = row[i];
            if (columnDesc.isTsCol()) {
                int curTsIndex = tsIndex;
                hasTsCol = true;
                tsIndex++;
                if (colValue == null) {
                    continue;
                }
                if (columnDesc.getType() == ColumnType.kInt64) {
                    tsDimensions.add(Tablet.TSDimension.newBuilder().setIdx(curTsIndex).setTs((Long)colValue).build());
                } else if (columnDesc.getType() == ColumnType.kTimestamp) {
                    if (colValue instanceof Timestamp) {
                        tsDimensions.add(Tablet.TSDimension.newBuilder().setIdx(curTsIndex).
                                setTs(((Timestamp)colValue).getTime()).build());
                    } else if (colValue instanceof DateTime) {
                        tsDimensions.add(Tablet.TSDimension.newBuilder().setIdx(curTsIndex).
                                setTs(((DateTime)colValue).getMillis()).build());
                    } else {
                        throw new TabletException("invalid ts column");
                    }
                } else {
                    throw new TabletException("invalid ts column");
                }
            }
        }
        if (hasTsCol && tsDimensions.isEmpty()) {
            throw new TabletException("no ts column");
        }
        return tsDimensions;
    }

    public static List<Tablet.Dimension> fillTabletDimension(Object[] row, TableHandler th, boolean handleNull) throws TabletException {
        List<Tablet.Dimension> dimList = new ArrayList<Tablet.Dimension>();
        Map<Integer, List<Integer>> indexs = th.getIndexes();
        Map<Integer, List<Integer>> indexTsMap = th.getIndexTsMap();
        if (indexs.isEmpty()) {
            throw new TabletException("no dimension in this row");
        }
        for (Map.Entry<Integer, List<Integer>> entry : indexs.entrySet()) {
            int index = entry.getKey();
            String value = null;
            int null_empty_count = 0;
            List<Integer> tsList = indexTsMap.get(index);
            if (tsList != null && !tsList.isEmpty()) {
                boolean allTsIsNull = true;
                for (int tsIndex : tsList) {
                    if (row[tsIndex] != null) {
                        allTsIsNull = false;
                        break;
                    }
                }
                if (allTsIsNull) {
                    continue;
                }
            }
            for (Integer pos : entry.getValue()) {
                String cur_value = null;
                if (pos >= row.length) {
                    throw new TabletException("index is greater than row length");
                }
                if (row[pos] == null) {
                    cur_value = RTIDBClientConfig.NULL_STRING;
                    null_empty_count++;
                } else {
                    cur_value = row[pos].toString();
                }
                if (cur_value.isEmpty()) {
                    cur_value = RTIDBClientConfig.EMPTY_STRING;
                    null_empty_count++;
                }
                if (value == null) {
                    value = cur_value;
                }  else {
                    value = value + "|" + cur_value;
                }
            }
            if (!handleNull && null_empty_count == entry.getValue().size()) {
                continue;
            }
            Tablet.Dimension dim = Tablet.Dimension.newBuilder().setIdx(index).setKey(value).build();
            dimList.add(dim);
        }
        if (dimList.isEmpty()) {
            throw new TabletException("no dimension in this row");
        }
        return dimList;
    }

    public static Map<Integer, List<Tablet.Dimension>> fillPartitionTabletDimension(Object[] row, TableHandler th,
                                                                           boolean handleNull) throws TabletException {
        Map<Integer, List<Tablet.Dimension>> mapping = new HashMap<Integer, List<Tablet.Dimension>>();
        Map<Integer, List<Integer>> indexs = th.getIndexes();
        if (indexs.isEmpty()) {
            throw new TabletException("no dimension in this row");
        }
        Map<Integer, List<Integer>> indexTsMap = th.getIndexTsMap();
        int count = 0;
        for (Map.Entry<Integer, List<Integer>> entry : indexs.entrySet()) {
            int index = entry.getKey();
            String value = null;
            int null_empty_count = 0;
            List<Integer> tsList = indexTsMap.get(index);
            if (tsList != null && !tsList.isEmpty()) {
                boolean allTsIsNull = true;
                for (int tsIndex : tsList) {
                    if (row[tsIndex] != null) {
                        allTsIsNull = false;
                        break;
                    }
                }
                if (allTsIsNull) {
                    continue;
                }
            }
            for (Integer pos : entry.getValue()) {
                String cur_value = null;
                if (pos >= row.length) {
                    throw new TabletException("index is greater than row length");
                }
                if (row[pos] == null) {
                    cur_value = RTIDBClientConfig.NULL_STRING;
                    null_empty_count++;
                } else {
                    cur_value = row[pos].toString();
                }
                if (cur_value.isEmpty()) {
                    cur_value = RTIDBClientConfig.EMPTY_STRING;
                    null_empty_count++;
                }
                if (value == null) {
                    value = cur_value;
                }  else {
                    value = value + "|" + cur_value;
                }
            }
            if (!handleNull && null_empty_count == entry.getValue().size()) {
                continue;
            }
            int pid = computePidByKey(value, th.getPartitions().length);
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
        return mapping;
    }

    public static String getCombinedKey(Map<String, Object> keyMap, List<String> indexList, boolean handleNull) throws TabletException {
        String combinedKey = "";
        int null_empty_count = 0;
        for (String key : indexList) {
            if (!combinedKey.isEmpty()) {
                combinedKey += "|";
            }
            Object obj = keyMap.get(key);
            if (obj == null) {
                combinedKey += RTIDBClientConfig.NULL_STRING;
                null_empty_count++;
            } else {
                String value = obj.toString();
                if (value.isEmpty()) {
                    value = RTIDBClientConfig.EMPTY_STRING;
                    null_empty_count++;
                }
                combinedKey += value;
            }
        }
        if (!handleNull && null_empty_count == keyMap.size()) {
            throw new TabletException("all key is null or empty");
        }
        return combinedKey;
    }

    public static String getCombinedKey(Object[] row, boolean handleNull) throws TabletException {
        String combinedKey = "";
        int null_empty_count = 0;
        for (Object obj : row) {
            if (!combinedKey.isEmpty()) {
                combinedKey += "|";
            }
            if (obj == null) {
                combinedKey += RTIDBClientConfig.NULL_STRING;
                null_empty_count++;
            } else {
                String value = obj.toString();
                if (value.isEmpty()) {
                    value = RTIDBClientConfig.EMPTY_STRING;
                    null_empty_count++;
                }
                combinedKey += value;
            }
        }
        if (!handleNull && null_empty_count == row.length) {
            throw new TabletException("all key is null or empty");
        }
        return combinedKey;
    }

    public static int computePidByKey(String key, int pidNum) {
        int pid = (int) (MurmurHash.hash64(key) % pidNum);
        if (pid < 0) {
            pid = pid * -1;
        }
        return pid;
    }

    public static String combinePartitionKey(Object[] row, List<Integer> partitionKeyList, int pidNum) throws TabletException {
        if (partitionKeyList.size() > 1) {
            Object[] partitionRow = new Object[partitionKeyList.size()];
            int pos = 0;
            for (Integer idx : partitionKeyList) {
                if (idx >= row.length) {
                    throw new TabletException("out of index");
                }
                partitionRow[pos] = row[idx];
                pos++;
            }
            return getCombinedKey(partitionRow, true);
        } else {
            if (partitionKeyList.get(0) >= row.length) {
                throw new TabletException("out of index");
            }
            return row[partitionKeyList.get(0)].toString();
        }
    }

    public static List<ScanFuture> scanInternal(int tid, String key, long st, long et, TableHandler th, ScanOption option)
            throws TabletException {
        List<TabletServerWapper> serverList = new ArrayList<>();
        Map<String, List<Integer>> endpointMap = new HashMap<>();
        for (int pid = 0; pid < th.getPartitions().length; pid++) {
            TabletServerWapper tabletServerWapper = th.getHandler(pid).getTabletServerWapper(th.getReadStrategy());
            serverList.add(tabletServerWapper);
            if (!endpointMap.containsKey(tabletServerWapper.getEndpoint())) {
                endpointMap.put(tabletServerWapper.getEndpoint(), new ArrayList<Integer>());
            }
            endpointMap.get(tabletServerWapper.getEndpoint()).add(pid);
        }
        List<ScanFuture> futureList = new ArrayList<>();
        for (List<Integer> list : endpointMap.values()) {
            futureList.add(scanInternal(tid, 0, list, key, st, et, th, option, serverList.get(list.get(0)).getServer()));
        }
        return futureList;
    }

    public static ScanFuture scanInternal(int tid, int pid, String key, long st, long et, TableHandler th, ScanOption option) throws TabletException{
        return scanInternal(tid, pid, null, key, st, et, th, option, null);
    }

    public static ScanFuture scanInternal(int tid, int pid, List<Integer> pidGroup, String key, long st, long et,
                                          TableHandler th, ScanOption option, TabletServer ts) throws TabletException{
        if (ts == null) {
            PartitionHandler ph = th.getHandler(pid);
            ts = ph.getReadHandler(th.getReadStrategy());
        }
        if (ts == null) {
            throw new TabletException("Cannot find available tabletServer with tid " + tid);
        }
        Tablet.ScanRequest.Builder builder = Tablet.ScanRequest.newBuilder();
        builder.setPk(key);
        builder.setTid(tid);
        builder.setEt(et);
        builder.setSt(st);
        if (pidGroup != null) {
            for (Integer curPid : pidGroup) {
                builder.addPidGroup(curPid);
            }
        }
        builder.setPid(pid);
        builder.setLimit(option.getLimit());
        builder.setAtleast(option.getAtLeast());
        if (option.getIdxName() != null)
            builder.setIdxName(option.getIdxName());
        if (option.getTsName() != null)
            builder.setTsName(option.getTsName());
        builder.setEnableRemoveDuplicatedRecord(option.isRemoveDuplicateRecordByTime());
        List<ColumnDesc> schema = th.getSchema();
        ProjectionInfo projectionInfo = null;
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
                projectionInfo = new ProjectionInfo(schema);
            }
            Tablet.ScanRequest request = builder.build();
            Future<Tablet.ScanResponse> response = ts.scan(request, TableClientCommon.scanFakeCallback);
            return new ScanFuture(response, th, projectionInfo);
        } else {
            Tablet.ScanRequest request = builder.build();
            if (option.getProjection().size() > 0) {
                List<Integer> projectionIdx = new ArrayList<>();
                int maxIdx = -1;
                BitSet bitSet = new BitSet(schema.size());
                for (String name : option.getProjection()) {
                    Integer idx = th.getSchemaPos().get(name);
                    if (idx == null) {
                        throw new TabletException("Cannot find column " + name);
                    }
                    bitSet.set(idx, true);
                    if (idx > maxIdx) {
                        maxIdx = idx;
                    }
                    projectionIdx.add(idx);
                }
                projectionInfo = new ProjectionInfo(projectionIdx, bitSet, maxIdx);
            }
            Future<Tablet.ScanResponse> response = ts.scan(request, TableClientCommon.scanFakeCallback);
            return new ScanFuture(response, th, projectionInfo);
        }
    }

    public static boolean isQueryByPartitionKey(String idxName, TableHandler th) {
        if (idxName == null || th.GetPartitionKeyList().isEmpty()) {
            return false;
        }
        Object obj = th.getKeyMap().get(idxName);
        if (obj != null && ((List<String>)obj).size() == th.GetPartitionKeyList().size()) {
            for (String col : (List<String>)obj) {
                Integer colPos = th.getSchemaPos().get(col);
                if (colPos == null || !th.GetPartitionKeyList().contains(colPos)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
    public static List<GetFuture> getInternal(String key, long time, GetOption getOption, TableHandler th) throws TabletException {
        List<TabletServerWapper> serverList = new ArrayList<>();
        Map<String, List<Integer>> endpointMap = new HashMap<>();
        for (int pid = 0; pid < th.getPartitions().length; pid++) {
            TabletServerWapper tabletServerWapper = th.getHandler(pid).getTabletServerWapper(th.getReadStrategy());
            serverList.add(tabletServerWapper);
            if (!endpointMap.containsKey(tabletServerWapper.getEndpoint())) {
                endpointMap.put(tabletServerWapper.getEndpoint(), new ArrayList<Integer>());
            }
            endpointMap.get(tabletServerWapper.getEndpoint()).add(pid);
        }
        List<GetFuture> futureList = new ArrayList<>();
        for (List<Integer> list : endpointMap.values()) {
            futureList.add(getInternal(0, list, key, time, getOption, th, serverList.get(list.get(0)).getServer()));
        }
        return futureList;
    }

    public static GetFuture getInternal(int pid, String key, long time, GetOption getOption, TableHandler th) throws TabletException {
        return getInternal(pid, null, key, time, getOption, th, null);
    }

    public static GetFuture getInternal(int pid, List<Integer> pidGroup, String key, long time, GetOption getOption,
                                        TableHandler th, TabletServer ts) throws TabletException {
        if (ts == null) {
            PartitionHandler ph = th.getHandler(pid);
            ts = ph.getReadHandler(th.getReadStrategy());
        }
        if (ts == null) {
            throw new TabletException("Cannot find available tabletServer with tid " + th.getTableInfo().getTid());
        }
        List<ColumnDesc> schema = null;
        Tablet.GetRequest.Builder builder = Tablet.GetRequest.newBuilder();
        builder.setTid(th.getTableInfo().getTid());
        if (pidGroup != null) {
            for (Integer curPid : pidGroup) {
                builder.addPidGroup(curPid);
            }
        } else {
            builder.setPid(pid);
        }
        builder.setKey(key);
        builder.setTs(time);
        builder.setEt(getOption.getEt());
        if (getOption.getStType() != null) builder.setType(getOption.getStType());
        if (getOption.getEtType() != null) builder.setEtType(getOption.getEtType());
        if (getOption.getIdxName() != null && !getOption.getIdxName().isEmpty()) {
            builder.setIdxName(getOption.getIdxName());
        }
        if (getOption.getTsName()!= null && !getOption.getTsName().isEmpty()) {
            builder.setTsName(getOption.getTsName());
        }
        if (th.getFormatVersion() == 1 ) {
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
            Tablet.GetRequest request = builder.build();
            Future<Tablet.GetResponse> future = ts.get(request, TableClientCommon.getFakeCallback);
            ProjectionInfo projectionInfo = new ProjectionInfo(schema);
            return new GetFuture(future, th, projectionInfo);
        }else {
            if (getOption.getProjection().size() > 0) {
                List<Integer> projectIdx = new ArrayList<>();
                BitSet bitSet = new BitSet(th.getSchema().size());
                int maxIndex = -1;
                for (String name : getOption.getProjection()) {
                    Integer idx = th.getSchemaPos().get(name);
                    if (idx == null) {
                        throw new TabletException("Cannot find column " + name);
                    }
                    projectIdx.add(idx);
                    if (idx > maxIndex) {
                        maxIndex = idx;
                    }
                    bitSet.set(idx, true);
                }
                ProjectionInfo projectionInfo = new ProjectionInfo(projectIdx, bitSet, maxIndex);
                Tablet.GetRequest request = builder.build();
                Future<Tablet.GetResponse> future = ts.get(request, TableClientCommon.getFakeCallback);
                return new GetFuture(future, th, projectionInfo);
            }else {
                Tablet.GetRequest request = builder.build();
                Future<Tablet.GetResponse> future = ts.get(request, TableClientCommon.getFakeCallback);
                return new GetFuture(future, th);
            }
        }
    }

}
