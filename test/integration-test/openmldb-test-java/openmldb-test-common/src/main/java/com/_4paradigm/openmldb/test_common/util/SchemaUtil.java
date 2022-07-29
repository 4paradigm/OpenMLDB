package com._4paradigm.openmldb.test_common.util;

import com._4paradigm.openmldb.test_common.bean.OpenMLDBColumn;
import com._4paradigm.openmldb.test_common.bean.OpenMLDBIndex;
import com._4paradigm.openmldb.test_common.bean.OpenMLDBTable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class SchemaUtil {
    public static int getIndexByColumnName(List<String> columnNames, String columnName) {
        for (int i = 0; i < columnNames.size(); i++) {
            if (columnNames.get(i).equals(columnName)) {
                return i;
            }
        }
        return -1;
    }
    public static OpenMLDBTable parseSchemaBySDK(List<String> lines){
        OpenMLDBTable schema = new OpenMLDBTable();
        List<OpenMLDBColumn> cols = new ArrayList<>();
        List<OpenMLDBIndex> indexs = new ArrayList<>();
        Iterator<String> it = lines.iterator();
//        while(it.hasNext()){
//            String line = it.next();
//            if(line.contains("ttl_type")) break;
//            if(line.startsWith("#")||line.startsWith("-"))continue;
//            OpenMLDBColumn col = new OpenMLDBColumn();
//            String[] infos = line.split("\\s+");
//            col.setId(Integer.parseInt(infos[0]));
//            col.setFieldName(infos[1]);
//            col.setFieldType(infos[2]);
//            col.setNullable(infos[3].equals("NO")?false:true);
//            cols.add(col);
//            it.remove();
//        }
        while(it.hasNext()){
            String line = it.next().trim();
            if(line.startsWith("#")||line.startsWith("-"))continue;
            OpenMLDBIndex index = new OpenMLDBIndex();
            String[] infos = line.split("\\s+");
            index.setId(Integer.parseInt(infos[0]));
            index.setIndexName(infos[1]);
            index.setKeys(Arrays.asList(infos[2].split("\\|")));
            index.setTs(infos[3]);
            index.setTtl(infos[4]);
            index.setTtlType(infos[5]);
            indexs.add(index);
            //it.remove();
        }
        schema.setIndexs(indexs);
        //schema.setColumns(cols);
        return schema;
    }
}
