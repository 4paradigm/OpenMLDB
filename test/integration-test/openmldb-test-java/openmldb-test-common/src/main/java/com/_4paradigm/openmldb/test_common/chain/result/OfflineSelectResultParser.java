package com._4paradigm.openmldb.test_common.chain.result;

import com._4paradigm.openmldb.test_common.bean.OpenMLDBJob;
import com._4paradigm.openmldb.test_common.bean.OpenMLDBResult;
import com._4paradigm.openmldb.test_common.bean.SQLType;
import com._4paradigm.openmldb.test_common.util.ResultUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


public class OfflineSelectResultParser extends AbstractResultHandler {

    @Override
    public boolean preHandle(SQLType sqlType) {
        return sqlType == SQLType.OFFLINE_SELECT;
    }

    @Override
    public void onHandle(OpenMLDBResult openMLDBResult) {
        String result = openMLDBResult.getResult().get(0).get(0).toString();
        String[] lines = result.split("\n");
        List<List<Object>> offlineResult = new ArrayList<>();
        if(lines.length>4){
            String columnStr = lines[1];
            columnStr = columnStr.substring(1,columnStr.length()-1);
            List<String> columns = Arrays.stream(columnStr.split("\\|")).map(s -> s.trim()).collect(Collectors.toList());
            openMLDBResult.setOfflineColumns(columns);
            for(int i=3;i<lines.length-1;i++){
                List<Object> offlineRow = Arrays.stream(lines[i].substring(1,lines[i].length()-1).split("\\|")).map(s -> s.trim()).collect(Collectors.toList());
                offlineResult.add(offlineRow);
            }
            openMLDBResult.setOfflineResult(offlineResult);
        }
    }
}
