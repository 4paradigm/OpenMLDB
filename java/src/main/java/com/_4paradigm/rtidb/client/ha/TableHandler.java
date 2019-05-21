package com._4paradigm.rtidb.client.ha;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.ColumnType;
import com._4paradigm.rtidb.ns.NS.TableInfo;

public class TableHandler {

    private TableInfo tableInfo;
    private PartitionHandler[] partitions;
    private Map<Integer, List<Integer>> indexes = new HashMap<Integer, List<Integer>>();
    private Map<String, List<String>> keyMap = new HashMap<String, List<String>>();
    private List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
    private ReadStrategy readStrategy = ReadStrategy.kReadLeader;
    private boolean hasTsCol = false;
    public TableHandler(TableInfo tableInfo) {
        this.tableInfo = tableInfo;
        int index = 0;
        if (tableInfo.getColumnDescV1Count() > 0) {
            Map<String, Integer> schemaPos = new HashMap<String, Integer>();
            for (int i = 0; i< tableInfo.getColumnDescV1Count(); i++) {
                com._4paradigm.rtidb.common.Common.ColumnDesc cd = tableInfo.getColumnDescV1(i);
                ColumnDesc ncd = new ColumnDesc();
                ncd.setName(cd.getName());
                ncd.setAddTsIndex(cd.getAddTsIdx());
                if (cd.getIsTsCol()) {
                    hasTsCol = true;
                }
                ncd.setTsCol(cd.getIsTsCol());
                ncd.setType(ColumnType.valueFrom(cd.getType()));
                schema.add(ncd);
                if (cd.getAddTsIdx()) {
                    List<Integer> list = new ArrayList<Integer>();
                    list.add(i);
                    indexes.put(index, list);
                    List<String> list1 = new ArrayList<String>();
                    list1.add(cd.getName());
                    keyMap.put(cd.getName(), list1);
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
                    List<Integer> list = new ArrayList<Integer>();
                    List<String> list1 = new ArrayList<String>();
                    for (String colName : ck.getColNameList()) {
                        list.add(schemaPos.get(colName));
                        list1.add(colName);
                    }
                    if (list.isEmpty()) {
                        String key = ck.getKeyName();
                        list.add(schemaPos.get(key));
                        list1.add(key);
                    }
                    if (indexSet.contains(ck.getKeyName())) {
                        continue;
                    }
                    indexSet.add(ck.getKeyName());
                    indexes.put(index, list);
                    keyMap.put(ck.getKeyName(), list1);
                    index++;
                }
            }

        } else {
            for (int i = 0; i < tableInfo.getColumnDescCount(); i++) {
                com._4paradigm.rtidb.ns.NS.ColumnDesc cd = tableInfo.getColumnDesc(i);
                ColumnDesc ncd = new ColumnDesc();
                ncd.setName(cd.getName());
                ncd.setAddTsIndex(cd.getAddTsIdx());
                ncd.setTsCol(false);
                ncd.setType(ColumnType.valueFrom(cd.getType()));
                schema.add(ncd);
                if (cd.getAddTsIdx()) {
                    List<Integer> list = new ArrayList<Integer>();
                    list.add(i);
                    indexes.put(index, list);
                    index++;
                }
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

    public Map<String, List<String>> getKeyMap() {
        return keyMap;
    }

    public List<ColumnDesc> getSchema() {
        return schema;
    }

    public enum ReadStrategy {
        kReadLocal,
        kReadLeader
    }

    public boolean hasTsCol() {
        return hasTsCol;
    }
    
    
}
