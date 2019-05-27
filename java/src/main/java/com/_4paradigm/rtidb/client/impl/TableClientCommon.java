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

    public static void ParseMapInput(Map<String, Object> row, TableHandler th, Object[] arrayRow, List<Tablet.TSDimension> tsDimensions) throws TabletException {
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
                        throw new TabletException("invalid ts column");
                    }
                } else {
                    throw new TabletException("invalid ts column");
                }
                tsIndex++;
            }
        }
    }

    public static List<Tablet.Dimension> FillTabletDimension(Object[] row, TableHandler th, boolean handleNull) throws TabletException {
        List<Tablet.Dimension> dimList = new ArrayList<Tablet.Dimension>();
        Map<Integer, List<Integer>> indexs = th.getIndexes();
        if (indexs.isEmpty()) {
            throw new TabletException("no dimension in this row");
        }
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
            Tablet.Dimension dim = Tablet.Dimension.newBuilder().setIdx(index).setKey(value).build();
            dimList.add(dim);
            count++;
        }
        if (count == 0) {
            throw new TabletException("no dimension in this row");
        }
        return dimList;
    }

    public static Map<Integer, List<Tablet.Dimension>> FillPartitionTabletDimension(Object[] row, TableHandler th,
                                                                           boolean handleNull) throws TabletException {
        Map<Integer, List<Tablet.Dimension>> mapping = new HashMap<Integer, List<Tablet.Dimension>>();
        Map<Integer, List<Integer>> indexs = th.getIndexes();
        if (indexs.isEmpty()) {
            throw new TabletException("no dimension in this row");
        }
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
            int pid = ComputePidByKey(value, th.getPartitions().length);
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

    public static String GetCombinedKey(Map<String, Object> keyMap, List<String> indexList, boolean handleNull) throws TabletException {
        String combinedKey = "";
        for (String key : indexList) {
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
        return combinedKey;
    }

    public static String GetCombinedKey(Object[] row, boolean handleNull) throws TabletException {
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
        return combinedKey;
    }

    public static int ComputePidByKey(String key, int pidNum) {
        int pid = (int) (MurmurHash.hash64(key) % pidNum);
        if (pid < 0) {
            pid = pid * -1;
        }
        return pid;
    }
}
