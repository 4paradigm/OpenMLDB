package com._4paradigm.rtidb.client.ha;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.ColumnType;
import com._4paradigm.rtidb.ns.NS.TableInfo;

public class TableHandler {

    private TableInfo tableInfo;
    private PartitionHandler[] partitions;
    private Map<String, Integer> indexes = new HashMap<String, Integer>();
    private List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
    public TableHandler(TableInfo tableInfo) {
        this.tableInfo = tableInfo;
        int index = 0;
        for (com._4paradigm.rtidb.ns.NS.ColumnDesc cd : tableInfo.getColumnDescList()) {
            ColumnDesc ncd = new ColumnDesc();
            ncd.setName(cd.getName());
            ncd.setAddTsIndex(cd.getAddTsIdx());
            ncd.setType(ColumnType.valueFrom(cd.getType()));
            schema.add(ncd);
            indexes.put(cd.getName(), index);
            index ++;
        }
        
    }
    
    public TableHandler(List<ColumnDesc> schema) {
        int index = 0;
        for (ColumnDesc col : schema) {
            if (col.isAddTsIndex()) {
                indexes.put(col.getName(), index);
                index ++;
            }
        }
        this.schema = schema;
    }
    
    public TableHandler() {}
    public PartitionHandler getHandler(int pid) {
        return partitions[pid];
    }

    public void setPartitions(PartitionHandler[] partitions) {
        this.partitions = partitions;
    }

    public TableInfo getTableInfo() {
        return tableInfo;
    }

    public PartitionHandler[] getPartitions() {
        return partitions;
    }

    public Map<String, Integer> getIndexes() {
        return indexes;
    }

    public List<ColumnDesc> getSchema() {
        return schema;
    }

}
