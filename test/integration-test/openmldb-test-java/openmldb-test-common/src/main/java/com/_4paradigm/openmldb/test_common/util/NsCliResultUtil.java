package com._4paradigm.openmldb.test_common.util;

import java.util.List;

public class NsCliResultUtil {
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
}
