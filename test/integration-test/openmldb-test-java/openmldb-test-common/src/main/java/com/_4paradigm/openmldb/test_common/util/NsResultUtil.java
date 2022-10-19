package com._4paradigm.openmldb.test_common.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NsResultUtil {
    public static boolean checkOPStatus(List<String> lines,String status){
        if(lines.size()<=2) return false;
        for(int i=2;i<lines.size();i++){
            String line = lines.get(i);
            String[] infos = line.split("\\s+");
            if(infos[1].equals("kAddReplicaSimplyRemoteOP")) continue;
            if(!infos[4].equals(status)){
                return false;
            }
        }
        return true;
    }
    public static boolean checkOPStatusAny(List<String> lines,String status){
        if(lines.size()<=2) return false;
        for(int i=2;i<lines.size();i++){
            String line = lines.get(i);
            String[] infos = line.split("\\s+");
            if(infos[1].equals("kAddReplicaSimplyRemoteOP")) continue;
            if(infos[4].equals(status)){
                return true;
            }
        }
        return false;
    }
    public static Map<String,List<Long>> getTableOffset(List<String> lines){
        Map<String,List<Long>> offsets = new HashMap<>();
        for(int i=2;i<lines.size();i++){
            String[] infos = lines.get(i).split("\\s+");
            String key = infos[0]+"_"+infos[2];
            List<Long> value = offsets.get(key);
            String role = infos[4];
            long offset = 0;
            String offsetStr = infos[7].trim();
            if(!offsetStr.equals("-")&&!offsetStr.equals("")){
                offset = Long.parseLong(offsetStr);
            }
            if(value==null){
                value = new ArrayList<>();
                offsets.put(key,value);
            }
            if(role.equals("leader")){
                value.add(0,offset);
            }else {
                value.add(offset);
            }
        }
        return offsets;
    }
    public static Map<String,Long> getTableOffsetByLeader(List<String> lines){
        Map<String,Long> offsets = new HashMap<>();
        for(int i=2;i<lines.size();i++){
            String[] infos = lines.get(i).split("\\s+");
            if(!"leader".equals(infos[4])) continue;
            if("no".equals(infos[6])) continue;
            String key = infos[0] + "_" + infos[2];
            long offset = Long.parseLong(infos[7]);
            offsets.put(key, offset);
        }
        return offsets;
    }
}
