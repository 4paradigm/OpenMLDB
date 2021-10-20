package com._4paradigm.openmldb.java_sdk_test.util;

import com._4paradigm.openmldb.java_sdk_test.entity.OpenMLDBColumn;
import com._4paradigm.openmldb.java_sdk_test.entity.OpenMLDBIndex;
import com._4paradigm.openmldb.java_sdk_test.entity.OpenMLDBSchema;
import org.apache.commons.collections4.CollectionUtils;
import org.testng.collections.Lists;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class CommandResultUtil {
    public static final List<String> CLI_ERRORS = Lists.newArrayList("fail","error","Unknow","");
    public static boolean success(List<String> result){
        if(CollectionUtils.isNotEmpty(result)){
            long failed = result.stream()
                    .filter(s -> containsErrorMsg(s))
                    .count();
            return failed==0;
        }
        return true;
    }
    private static boolean containsErrorMsg(String s){
        String tmp = s.toLowerCase();
        return tmp.contains("fail")||tmp.contains("error")||tmp.contains("unknow")||tmp.contains("unsupported")||tmp.contains("not an astintervalliteral");
    }
    public static OpenMLDBSchema parseSchema(List<String> lines){
        OpenMLDBSchema schema = new OpenMLDBSchema();
        List<OpenMLDBColumn> cols = new ArrayList<>();
        List<OpenMLDBIndex> indexs = new ArrayList<>();
        Iterator<String> it = lines.iterator();
        while(it.hasNext()){
            String line = it.next();
            if(line.contains("ttl_type")) break;
            if(line.startsWith("#")||line.startsWith("-"))continue;
            OpenMLDBColumn col = new OpenMLDBColumn();
            String[] infos = line.split("\\s+");
            col.setId(Integer.parseInt(infos[0]));
            col.setFieldName(infos[1]);
            col.setFieldType(infos[2]);
            col.setNullable(infos[3].equals("NO")?false:true);
            cols.add(col);
            it.remove();
        }
        while(it.hasNext()){
            String line = it.next();
            if(line.startsWith("#")||line.startsWith("-"))continue;
            OpenMLDBIndex index = new OpenMLDBIndex();
            String[] infos = line.split("\\s+");
            index.setId(Integer.parseInt(infos[0]));
            index.setIndexName(infos[1]);
            index.setKeys(Arrays.asList(infos[2].split(",")));
            index.setTs(infos[3]);
            index.setTtl(infos[4]);
            index.setTtlType(infos[5]);
            indexs.add(index);
            it.remove();
        }
        schema.setIndexs(indexs);
        schema.setColumns(cols);
        return schema;
    }
}
