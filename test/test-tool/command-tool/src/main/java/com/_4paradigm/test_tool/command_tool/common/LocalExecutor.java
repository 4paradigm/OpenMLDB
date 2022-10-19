package com._4paradigm.test_tool.command_tool.common;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class LocalExecutor implements CommandExecutor {

    private List<String> starts = new ArrayList<>();
    private List<String> contains = new ArrayList<>();

    public LocalExecutor(){
        starts.add("wget");
        starts.add("tar");
//        contains.add("--role=ns_client");
    }
    public boolean isUseExec(String command){
        for(String start:starts){
            if(command.startsWith(start)){
                return true;
            }
        }
        for(String contain:contains){
            if(command.contains(contain)){
                return true;
            }
        }
        return false;
    }
    @Override
    public String execute(String command) {
        String result;
        if(isUseExec(command)){
            result = ExecUtil.exeCommand(command);
        }else{
            result = CommandUtil.run(command);
        }
        return result;
    }

}
