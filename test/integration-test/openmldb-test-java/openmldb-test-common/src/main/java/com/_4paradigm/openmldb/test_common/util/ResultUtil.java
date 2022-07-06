package com._4paradigm.openmldb.test_common.util;

import com._4paradigm.openmldb.test_common.bean.OpenMLDBResult;
import com._4paradigm.openmldb.test_common.model.OpenmldbDeployment;
import com.google.common.base.Joiner;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ResultUtil {
    public static OpenmldbDeployment parseDeployment(List<String> lines){
        OpenmldbDeployment deployment = new OpenmldbDeployment();
        List<String> inColumns = new ArrayList<>();
        List<String> outColumns = new ArrayList<>();
        String[] db_sp = lines.get(3).split("\\s+");
        deployment.setDbName(db_sp[1]);
        deployment.setName(db_sp[2]);

        String sql = "";
        List<String> list = lines.subList(9, lines.size());
        Iterator<String> it = list.iterator();
        while(it.hasNext()) {
            String line = it.next().trim();
            if (line.contains("row in set")) break;
            if (line.startsWith("#") || line.startsWith("-")) continue;
            sql += line+"\n";
        }
        deployment.setSql(sql);
        while(it.hasNext()){
            String line = it.next().trim();
            if (line.contains("Output Schema")) break;
            if (line.startsWith("#") || line.startsWith("-")|| line.equals("")) continue;
            String[] infos = line.split("\\s+");
            String in = Joiner.on(",").join(infos);
            inColumns.add(in);
        }
        while(it.hasNext()){
            String line = it.next().trim();
            if(line.startsWith("#")||line.startsWith("-"))continue;
            String[] infos = line.split("\\s+");
            String out = Joiner.on(",").join(infos);
            outColumns.add(out);
        }
        deployment.setInColumns(inColumns);
        deployment.setOutColumns(outColumns);
        return deployment;
    }
    public static void setSchema(ResultSetMetaData metaData, OpenMLDBResult openMLDBResult) {
        try {
            int columnCount = metaData.getColumnCount();
            List<String> columnNames = new ArrayList<>();
            List<String> columnTypes = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                String columnLabel = null;
                try {
                    columnLabel = metaData.getColumnLabel(i);
                }catch (SQLException e){
                    columnLabel = metaData.getColumnName(i);
                }
                columnNames.add(columnLabel);
                columnTypes.add(TypeUtil.fromJDBCTypeToString(metaData.getColumnType(i)));
            }
            openMLDBResult.setColumnNames(columnNames);
            openMLDBResult.setColumnTypes(columnTypes);
        }catch (SQLException e){
            e.printStackTrace();
        }
    }
}
