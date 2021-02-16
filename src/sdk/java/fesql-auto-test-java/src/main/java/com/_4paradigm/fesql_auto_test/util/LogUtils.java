package com._4paradigm.fesql_auto_test.util;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhaowei
 * @date 2021/2/16 10:12 AM
 */
public class LogUtils {
    public static ThreadLocal<List<String>> threadLog = new ThreadLocal<>();

    public static List<String> getLogs(){
        List<String> logs = threadLog.get();
        if(logs==null){
            logs = new ArrayList<>();
            threadLog.set(logs);
        }
        return logs;
    }

    public static void log(String log){
        getLogs().add(log);
    }

    public static void log(String log,Object... objs){
        for(Object obj:objs) {
            log = log.replaceFirst("\\{\\}",obj.toString());
        }
        getLogs().add(log);
    }

    public static void clean(){
        getLogs().clear();
    }

    public static void remove(){
        List<String> logs = threadLog.get();
        if(logs!=null){
            threadLog.set(null);
        }
    }
}
