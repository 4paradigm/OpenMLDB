package com._4paradigm.openmldb.sdk.impl;

import com._4paradigm.openmldb.SQLInsertRow;
import com._4paradigm.openmldb.DefaultValueContainer;
import com._4paradigm.openmldb.VectorUint32;
import com._4paradigm.openmldb.common.codec.CodecMetaData;
import com._4paradigm.openmldb.common.codec.CodecUtil;
import com._4paradigm.openmldb.proto.NS;
import com._4paradigm.openmldb.sdk.Common;
import com._4paradigm.openmldb.sdk.Schema;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.*;

public class InsertPreparedStatementMeta {

    public static String NONETOKEN = "!N@U#L$L%";
    public static String EMPTY_STRING = "!@#$%";

    private String sql;
    private String db;
    private String name;
    private int tid;
    private int partitionNum;
    private Schema schema;
    private CodecMetaData codecMetaData;
    private Map<Integer, Object> defaultValue = new HashMap<>();
    private List<Integer> holeIdx = new ArrayList<>();
    private Set<Integer> indexPos = new HashSet<>();
    private Map<Integer, List<Integer>> indexMap = new HashMap<>();
    private Map<Integer, String> defaultIndexValue = new HashMap<>();

    public InsertPreparedStatementMeta(String sql, NS.TableInfo tableInfo, SQLInsertRow insertRow) {
        this.sql = sql;
        try {
            schema = Common.convertSchema(tableInfo.getColumnDescList());
            codecMetaData = new CodecMetaData(tableInfo.getColumnDescList(), false);
        } catch (Exception e) {
            e.printStackTrace();
        }
        db = tableInfo.getDb();
        name = tableInfo.getName();
        tid = tableInfo.getTid();
        partitionNum = tableInfo.getTablePartitionCount();
        buildIndex(tableInfo);
        DefaultValueContainer value = insertRow.GetDefaultValue();
        buildDefaultValue(value);
        value.delete();
        VectorUint32 idxArray = insertRow.GetHoleIdx();
        buildHoleIdx(idxArray);
        idxArray.delete();
    }

    private void buildIndex(NS.TableInfo tableInfo) {
        Map<String, Integer> nameIdxMap = new HashMap<>();
        for (int i = 0; i < schema.size(); i++) {
            nameIdxMap.put(schema.getColumnName(i), i);
        }
        for (int i = 0; i < tableInfo.getColumnKeyList().size(); i++) {
            com._4paradigm.openmldb.proto.Common.ColumnKey columnKey = tableInfo.getColumnKeyList().get(i);
            List<Integer> colList = new ArrayList<>(columnKey.getColNameCount());
            for (String name : columnKey.getColNameList()) {
                colList.add(nameIdxMap.get(name));
                indexPos.add(nameIdxMap.get(name));
            }
            indexMap.put(i, colList);
        }
    }

    private void buildHoleIdx(VectorUint32 idxArray) {
        int size = idxArray.size();
        for (int i = 0; i < size; i++) {
            holeIdx.add(idxArray.get(i).intValue());
        }
    }

    private void buildDefaultValue(DefaultValueContainer valueContainer) {
        VectorUint32 defaultPos = valueContainer.GetAllPosition();
        int size = defaultPos.size();
        for (int i = 0; i < size; i++) {
            int schemaIdx = defaultPos.get(i).intValue();
            boolean isIndexVal = indexPos.contains(schemaIdx);
            if (valueContainer.IsNull(schemaIdx)) {
                defaultValue.put(schemaIdx, null);
                if (isIndexVal) {
                    defaultIndexValue.put(schemaIdx, NONETOKEN);
                }
            } else {
                switch (schema.getColumnType(schemaIdx)) {
                    case Types.BOOLEAN: {
                        boolean val = valueContainer.GetBool(schemaIdx);
                        defaultValue.put(schemaIdx, val);
                        if (isIndexVal) {
                            defaultIndexValue.put(schemaIdx, String.valueOf(val));
                        }
                        break;
                    }
                    case Types.SMALLINT: {
                        short val = valueContainer.GetSmallInt(schemaIdx);
                        defaultValue.put(schemaIdx, val);
                        if (isIndexVal) {
                            defaultIndexValue.put(schemaIdx, String.valueOf(val));
                        }
                        break;
                    }
                    case Types.INTEGER: {
                        int val = valueContainer.GetInt(schemaIdx);
                        defaultValue.put(schemaIdx, val);
                        if (isIndexVal) {
                            defaultIndexValue.put(schemaIdx, String.valueOf(val));
                        }
                        break;
                    }
                    case Types.BIGINT: {
                        long val = valueContainer.GetBigInt(schemaIdx);
                        defaultValue.put(schemaIdx, val);
                        if (isIndexVal) {
                            defaultIndexValue.put(schemaIdx, String.valueOf(val));
                        }
                        break;
                    }
                    case Types.FLOAT:
                        defaultValue.put(schemaIdx, valueContainer.GetFloat(schemaIdx));
                        break;
                    case Types.DOUBLE:
                        defaultValue.put(schemaIdx, valueContainer.GetDouble(schemaIdx));
                        break;
                    case Types.DATE: {
                        int val = valueContainer.GetDate(schemaIdx);
                        defaultValue.put(schemaIdx, CodecUtil.dateIntToDate(val));
                        if (isIndexVal) {
                            defaultIndexValue.put(schemaIdx, String.valueOf(val));
                        }
                        break;
                    }
                    case Types.TIMESTAMP: {
                        long val = valueContainer.GetTimeStamp(schemaIdx);
                        defaultValue.put(schemaIdx, new Timestamp(val));
                        if (isIndexVal) {
                            defaultIndexValue.put(schemaIdx, String.valueOf(val));
                        }
                        break;
                    }
                    case Types.VARCHAR: {
                        String val = valueContainer.GetString(schemaIdx);
                        defaultValue.put(schemaIdx, val);
                        if (isIndexVal) {
                            if (val.isEmpty()) {
                                defaultIndexValue.put(schemaIdx, EMPTY_STRING);
                            } else {
                                defaultIndexValue.put(schemaIdx, val);
                            }
                        }
                        break;
                    }
                }
            }
        }
        defaultPos.delete();
    }

    public Schema getSchema() {
        return schema;
    }

    public String getDatabase() {
        return db;
    }

    public String getName() {
        return name;
    }

    public int getTid() {
        return tid;
    }

    public int getPartitionNum() {
        return partitionNum;
    }

    public CodecMetaData getCodecMeta() {
        return codecMetaData;
    }

    public Map<Integer, Object> getDefaultValue() {
        return defaultValue;
    }

    public String getSql() {
        return sql;
    }

    public int getSchemaIdx(int idx) throws SQLException {
        if (idx >= holeIdx.size()) {
            throw new SQLException("out of data range");
        }
        return holeIdx.get(idx);
    }

    List<Integer> getHoleIdx() {
        return holeIdx;
    }

    Set<Integer> getIndexPos() {
        return indexPos;
    }

    Map<Integer, List<Integer>> getIndexMap() {
        return indexMap;
    }

    Map<Integer, String> getDefaultIndexValue() {
        return defaultIndexValue;
    }
}
