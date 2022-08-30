package com._4paradigm.openmldb.test_common.util;

import com._4paradigm.openmldb.test_common.bean.OpenMLDBColumn;
import com._4paradigm.openmldb.test_common.bean.OpenMLDBIndex;
import com._4paradigm.openmldb.test_common.bean.OpenMLDBResult;
import com._4paradigm.openmldb.test_common.bean.OpenMLDBTable;
import com._4paradigm.openmldb.test_common.model.OpenmldbDeployment;
import com.google.common.base.Joiner;
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
        return tmp.contains("fail")||tmp.contains("error")||tmp.contains("unknow")||tmp.contains("unsupported")||
                tmp.contains("not an astint")||tmp.contains("already exists")||tmp.contains("not supported")
                ||tmp.contains("not found")||tmp.contains("un-support")||tmp.contains("invalid")
                ||tmp.contains("distribution element is not")||tmp.contains("is not currently supported")
                ||tmp.contains("wrong type")||tmp.contains("not a supported object type")||tmp.contains("is not");
    }
    public static OpenMLDBTable parseSchema(List<String> lines){
        OpenMLDBTable schema = new OpenMLDBTable();
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
            index.setKeys(Arrays.asList(infos[2].split("\\|")));
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
    public static OpenmldbDeployment parseDeployment(List<String> lines){
        OpenmldbDeployment deployment = new OpenmldbDeployment();
        List<String> inColumns = new ArrayList<>();
        List<String> outColumns = new ArrayList<>();
        String[] db_sp = lines.get(3).split("\\s+");
        deployment.setDbName(db_sp[0]);
        deployment.setName(db_sp[1]);
        for(int i=0;i<9;i++) {
            lines.remove(0);
        }
        Iterator<String> it = lines.iterator();
        String sql = "";
        while(it.hasNext()) {
            String line = it.next();
            if (line.contains("row in set")) break;
            if (line.startsWith("#") || line.startsWith("-")) continue;
            sql += line+"\n";
        }
        deployment.setSql(sql);
        while(it.hasNext()){
            String line = it.next();
            if (line.contains("Output Schema")) break;
            if (line.startsWith("#") || line.startsWith("-")) continue;
            String[] infos = line.split("\\s+");
            String in = Joiner.on(",").join(infos);
            inColumns.add(in);
        }
        while(it.hasNext()){
            String line = it.next();
            if(line.startsWith("#")||line.startsWith("-"))continue;
            String[] infos = line.split("\\s+");
            String out = Joiner.on(",").join(infos);
            outColumns.add(out);
        }
        deployment.setInColumns(inColumns);
        deployment.setOutColumns(outColumns);
        return deployment;
    }
    public static List<OpenmldbDeployment> parseDeployments(List<String> lines){
        List<OpenmldbDeployment> deployments = new ArrayList<>();
        for(int i=0;i<3;i++) {
            lines.remove(0);
        }
        Iterator<String> it = lines.iterator();
        while(it.hasNext()){
            String line = it.next();
            OpenmldbDeployment deployment = new OpenmldbDeployment();
            if (line.startsWith("-")) break;
            String[] infos = line.split("\\s+");
            deployment.setDbName(infos[0]);
            deployment.setName(infos[1]);
            deployments.add(deployment);
        }
        return deployments;
    }
    //  ---------- ---------------- --------------- -------------- ------ ------------------ ---------------- ----------- ------------------- --------- --------------------------------------------------------------- ---------------- -------------------
    //  Table_id   Table_name       Database_name   Storage_type   Rows   Memory_data_size   Disk_data_size   Partition   Partition_unalive   Replica   Offline_path                                                    Offline_format   Offline_deep_copy
    // ---------- ---------------- --------------- -------------- ------ ------------------ ---------------- ----------- ------------------- --------- --------------------------------------------------------------- ---------------- -------------------
    //  27         auto_AITzyByZ    default_db      ssd            1      0                  473414           2           0                   3         NULL                                                            NULL             NULL
    //  19         auto_GlcndMiH    default_db      hdd            1      0                  515239           2           0                   3         NULL
    public static void parseResult(List<String> lines, OpenMLDBResult openMLDBResult){
        if(CollectionUtils.isNotEmpty(lines)&&lines.size()>=2) {
            int count = 0;
            List<List<Object>> rows = new ArrayList<>();
            List<String> columnNames = Arrays.asList(lines.get(1).split("\\s+"));
            for (int i = 3; i < lines.size() - 2; i++) {
                count++;
                List<Object> row = Arrays.asList(lines.get(i).split("\\s+"));
                rows.add(row);
            }
            openMLDBResult.setColumnNames(columnNames);
            openMLDBResult.setCount(count);
            openMLDBResult.setResult(rows);
        }
    }
}
