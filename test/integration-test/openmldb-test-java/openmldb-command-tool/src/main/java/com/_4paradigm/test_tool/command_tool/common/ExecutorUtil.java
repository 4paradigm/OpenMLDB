package com._4paradigm.test_tool.command_tool.common;


import com._4paradigm.test_tool.command_tool.conf.CommandConfig;
import com._4paradigm.test_tool.command_tool.util.OSInfoUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class ExecutorUtil {
    private static final CommandExecutor executor;
    static{
        executor = getExecutor();
    }
    public static List<String> run(String command){
       // CommandExecutor executor = getExecutor();
        log.info("command:"+command);
        List<String> list = new ArrayList<>();
        String result = executor.execute(command);
       // log.info("result:\n{}",result);
        String[] results = result.split("\n");
        for(String line:results){
            line = line.trim();
            if(line.contains("ZOO_INFO") || line.contains("zk_client.cc")||
                    line.startsWith("ns leader:")||line.startsWith("client start in")||line.startsWith("WARNING:")){
                continue;
            }
            if(line.length()==0) continue;
            list.add(line);
        }
        printResult(list);
        return list;
    }

    private static void printResult(List<String> lines){
        StringBuilder result = new StringBuilder("result-list:\n");
        for(String line:lines){
            result.append(line+"\n");
        }
        log.info(result.toString());
    }
    private static CommandExecutor getExecutor(){
        CommandExecutor executor;
        if(CommandConfig.IS_REMOTE){
            executor = new RemoteExecutor();
        }else{
            executor = new LocalExecutor();
        }
//        if(OSInfoUtil.isMac()){
//            executor = new RemoteExecutor();
////            executor = new LocalExecutor();
//        }else{
//            executor = new LocalExecutor();
//        }
        return executor;
    }
}
