package com._4paradigm.openmldb.test_common.chain.result;

import com._4paradigm.openmldb.test_common.bean.*;
import org.apache.commons.lang3.StringUtils;
import org.testng.collections.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DescResultParser extends AbstractResultHandler {

    @Override
    public boolean preHandle(SQLType sqlType) {
        return sqlType == SQLType.DESC;
    }

    @Override
    public void onHandle(OpenMLDBResult openMLDBResult) {
        try {
            List<List<Object>> resultList = openMLDBResult.getResult();
            List<String> lines = resultList.stream().map(l -> String.valueOf(l.get(0))).collect(Collectors.toList());
            OpenMLDBTable table = new OpenMLDBTable();
            List<OpenMLDBColumn> columns = new ArrayList<>();
            String columnStr = lines.get(0);
            String[] ss = columnStr.split("\n");
            for(String s:ss){
                s = s.trim();
                if(s.startsWith("#")||s.startsWith("-")) continue;
                String[] infos = s.split("\\s+");
                OpenMLDBColumn openMLDBColumn = new OpenMLDBColumn();
                openMLDBColumn.setId(Integer.parseInt(infos[0]));
                openMLDBColumn.setFieldName(infos[1]);
                openMLDBColumn.setFieldType(infos[2]);
                openMLDBColumn.setNullable(infos[3].equals("YES"));
                columns.add(openMLDBColumn);
            }
            table.setColumns(columns);
            String indexStr = lines.get(1);
            List<OpenMLDBIndex> indices = new ArrayList<>();
            String[] indexSS = indexStr.split("\n");
            for(String s:indexSS){
                s = s.trim();
                if(s.startsWith("#")||s.startsWith("-")) continue;
                String[] infos = s.split("\\s+");
                OpenMLDBIndex openMLDBIndex = new OpenMLDBIndex();
                openMLDBIndex.setId(Integer.parseInt(infos[0]));
                openMLDBIndex.setIndexName(infos[1]);
                openMLDBIndex.setKeys(Lists.newArrayList(infos[2].split("\\|")));
                openMLDBIndex.setTs(infos[3]);
                openMLDBIndex.setTtl(infos[4]);
                openMLDBIndex.setTtlType(infos[5]);
                indices.add(openMLDBIndex);
            }
            table.setIndexs(indices);
            String storageStr = null;
            String offlineStr = null;
            if(lines.size()==3) {
                storageStr = lines.get(2);
            }else if(lines.size()==4){
                storageStr = lines.get(3);
                offlineStr = lines.get(2);
            }
            table.setStorageMode(storageStr.split("\n")[3].trim());
            openMLDBResult.setSchema(table);
            if(StringUtils.isNotEmpty(offlineStr)){
                String[] offlineArray = offlineStr.split("\n")[3].trim().split("\\s+");
                OfflineInfo offlineInfo = new OfflineInfo();
                offlineInfo.setPath(offlineArray[0]);
                offlineInfo.setFormat(offlineArray[1]);
                offlineInfo.setDeepCopy(Boolean.parseBoolean(offlineArray[2]));
                if(offlineArray.length>=4){
                    offlineInfo.setOption(offlineArray[3]);
                }
                table.setOfflineInfo(offlineInfo);
            }
        } catch (Exception e) {
            e.printStackTrace();
            openMLDBResult.setOk(false);
            openMLDBResult.setMsg(e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
