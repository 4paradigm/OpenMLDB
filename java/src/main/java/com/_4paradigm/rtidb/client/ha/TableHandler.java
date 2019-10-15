package com._4paradigm.rtidb.client.ha;

import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.ColumnType;
import com._4paradigm.rtidb.ns.NS.TableInfo;

import java.util.*;

public class TableHandler {

    private TableInfo tableInfo;
    private PartitionHandler[] partitions;
    private Map<Integer, List<Integer>> indexes = new TreeMap<Integer, List<Integer>>();
    private Map<Integer, List<Integer>> indexTsMap = new TreeMap<Integer, List<Integer>>();
    private Map<String, List<String>> keyMap = new TreeMap<String, List<String>>();
    private List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
    private Map<Integer, List<ColumnDesc>> schemaMap = new TreeMap<>();
    private ReadStrategy readStrategy = ReadStrategy.kReadLeader;
    private boolean hasTsCol = false;
    public TableHandler(TableInfo tableInfo) {
        this.tableInfo = tableInfo;
        int schemaSize = 0;
        List<ColumnDesc> schemaMapList = new ArrayList<ColumnDesc>();
        int index = 0;
        if (tableInfo.getColumnDescV1Count() > 0) {
            schemaSize = tableInfo.getColumnDescV1Count();
            Map<String, Integer> schemaPos = new HashMap<String, Integer>();
            Map<String, Integer> tsPos = new HashMap<String, Integer>();
            for (int i = 0; i< tableInfo.getColumnDescV1Count(); i++) {
                com._4paradigm.rtidb.common.Common.ColumnDesc cd = tableInfo.getColumnDescV1(i);
                ColumnDesc ncd = new ColumnDesc();
                ncd.setName(cd.getName());
                ncd.setAddTsIndex(cd.getAddTsIdx());
                if (cd.getIsTsCol()) {
                    hasTsCol = true;
                    tsPos.put(cd.getName(), i);
                }
                ncd.setTsCol(cd.getIsTsCol());
                ncd.setType(ColumnType.valueFrom(cd.getType()));
                schema.add(ncd);
                schemaMapList.add(ncd);
                if (cd.getAddTsIdx()) {
                    List<Integer> indexList = new ArrayList<Integer>();
                    indexList.add(i);
                    indexes.put(index, indexList);
                    List<String> nameList = new ArrayList<String>();
                    nameList.add(cd.getName());
                    keyMap.put(cd.getName(), nameList);
                    index++;
                }
                schemaPos.put(cd.getName(), i);
            }
            if (tableInfo.getColumnKeyCount() > 0) {
                indexes.clear();
                keyMap.clear();
                index = 0;
                Set<String> indexSet = new HashSet<String>();
                for (com._4paradigm.rtidb.common.Common.ColumnKey ck : tableInfo.getColumnKeyList()) {
                    List<Integer> indexList = new ArrayList<Integer>();
                    List<Integer> tsList = new ArrayList<Integer>();
                    List<String> nameList = new ArrayList<String>();
                    for (String colName : ck.getColNameList()) {
                        indexList.add(schemaPos.get(colName));
                        nameList.add(colName);
                    }
                    for (String tsName : ck.getTsNameList()) {
                        tsList.add(tsPos.get(tsName));
                    }
                    if (indexList.isEmpty()) {
                        String key = ck.getIndexName();
                        indexList.add(schemaPos.get(key));
                        nameList.add(key);
                    }
                    if (indexSet.contains(ck.getIndexName())) {
                        continue;
                    }
                    indexSet.add(ck.getIndexName());
                    indexes.put(index, indexList);
                    keyMap.put(ck.getIndexName(), nameList);
                    if (!tsList.isEmpty()) {
                        indexTsMap.put(index, tsList);
                    } else if (!tsPos.isEmpty()) {
                        for (Integer curTsPos : tsPos.values()) {
                            tsList.add(curTsPos);
                        }
                        for (Integer cur_index : indexes.keySet()) {
                            indexTsMap.put(index, tsList);
                        }
                    }
                    index++;
                }
            } else {
                if (!tsPos.isEmpty()) {
                    List<Integer> tsList = new ArrayList<Integer>();
                    for (Integer curTsPos : tsPos.values()) {
                        tsList.add(curTsPos);
                    }
                    for (Integer cur_index : indexes.keySet()) {
                        indexTsMap.put(index, tsList);
                    }
                }
            }

        } else {
            schemaSize = tableInfo.getColumnDescCount();
            for (int i = 0; i < schemaSize; i++) {
                com._4paradigm.rtidb.ns.NS.ColumnDesc cd = tableInfo.getColumnDesc(i);
                ColumnDesc ncd = new ColumnDesc();
                ncd.setName(cd.getName());
                ncd.setAddTsIndex(cd.getAddTsIdx());
                ncd.setTsCol(false);
                ncd.setType(ColumnType.valueFrom(cd.getType()));
                schema.add(ncd);
                schemaMapList.add(ncd);
                if (cd.getAddTsIdx()) {
                    List<Integer> list = new ArrayList<Integer>();
                    list.add(i);
                    indexes.put(index, list);
                    index++;
                }
            }
        }
        if (tableInfo.getAddedColumnDescCount() > 0) {
            for (int i = 0; i < tableInfo.getAddedColumnDescCount(); i++) {
                com._4paradigm.rtidb.common.Common.ColumnDesc cd = tableInfo.getAddedColumnDesc(i);
                ColumnDesc ncd = new ColumnDesc();
                ncd.setName(cd.getName());
                ncd.setType(ColumnType.valueFrom(cd.getType()));
                schemaMapList.add(ncd);
                schemaMap.put(schemaSize + i + 1, schemaMapList);
            }
        }
    }
    
    public ReadStrategy getReadStrategy() {
        return readStrategy;
    }

    public void setReadStrategy(ReadStrategy readStrategy) {
        this.readStrategy = readStrategy;
    }

    public TableHandler(List<ColumnDesc> schema) {
        int index = 0;
        int col_num = 0;
        for (ColumnDesc col : schema) {
            if (col.isAddTsIndex()) {
                List<Integer> list = new ArrayList<Integer>();
                list.add(col_num);
                indexes.put(index, list);
                index ++;
            }
            col_num++;
        }
        this.schema = schema;
    }
    
    public TableHandler() {}
    public PartitionHandler getHandler(int pid) {
        if (pid >= partitions.length) {
            return null;
        }
        return partitions[pid];
    }

    public void setPartitions(PartitionHandler[] partitions) {
        this.partitions = partitions;
    }

    public TableInfo getTableInfo() {
        return tableInfo;
    }

    public void setTableInfo(TableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }

    public PartitionHandler[] getPartitions() {
        return partitions;
    }

    public Map<Integer, List<Integer>> getIndexes() {
        return indexes;
    }

    public Map<Integer, List<Integer>> getIndexTsMap() {
        return indexTsMap;
    }

    public Map<String, List<String>> getKeyMap() {
        return keyMap;
    }

    public List<ColumnDesc> getSchema() {
        return schema;
    }

    public enum ReadStrategy {
        kReadLeader,
        kReadFollower,
        kReadLocal,
        KReadRandom
    }

    public boolean hasTsCol() {
        return hasTsCol;
    }

    public Map<Integer, List<ColumnDesc>> getSchemaMap() {
        return schemaMap;
    }

}
