package com._4paradigm.rtidb.client.impl;

import com._4paradigm.rtidb.client.TabletException;
import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.ha.TableHandler;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.ColumnType;
import com._4paradigm.rtidb.tablet.Tablet;
import com._4paradigm.rtidb.utils.MurmurHash;
import org.joda.time.DateTime;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableClientCommon {

    public static void parseMapInput(Map<String, Object> row, TableHandler th, Object[] arrayRow, List<Tablet.TSDimension> tsDimensions) throws TabletException {
        tsDimensions.clear();
        int tsIndex = 0;
        List<ColumnDesc> schema;
        if (row.size() > th.getSchema().size()) {
            schema = th.getSchemaMap().get(row.size());
        } else {
            schema = th.getSchema();
        }
        for (int i = 0; i < schema.size(); i++) {
            ColumnDesc columnDesc = schema.get(i);
            Object colValue = row.get(columnDesc.getName());
            arrayRow[i] = colValue;
            if (columnDesc.isTsCol()) {
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
        if (tsDimensions.isEmpty()) {
            throw new TabletException("no ts column");
        }
    }

    public static List<Tablet.TSDimension> parseArrayInput(Object[] row, TableHandler th) throws TabletException {
        if (row.length != th.getSchema().size()) {
            throw new TabletException("input row size error");
        }
        List<Tablet.TSDimension> tsDimensions = new ArrayList<Tablet.TSDimension>();
        int tsIndex = 0;
        for (int i = 0; i < th.getSchema().size(); i++) {
            ColumnDesc columnDesc = th.getSchema().get(i);
            Object colValue = row[i];
            if (columnDesc.isTsCol()) {
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
        if (tsDimensions.isEmpty()) {
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
}
