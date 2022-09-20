package com._4paradigm.openmldb.test_common.command;


import com._4paradigm.openmldb.test_common.common.LogProxy;
import com._4paradigm.openmldb.test_common.util.Tool;
import com._4paradigm.test_tool.command_tool.common.ExecutorUtil;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import java.util.List;
@Slf4j
public class CommandUtil {
    private static final Logger logger = new LogProxy(log);
    public static List<String> run(String command){
        return run(command,1000,30);
    }
    public static List<String> run(String command, int time, int count){
        int num = 0;
        List<String> result;
        do{
            result = ExecutorUtil.run(command);
            if((result.size()==0)||(result.contains("zk client init failed"))){
                num++;
                Tool.sleep(time);
                logger.info("command retry:"+num);
            }else {
                return result;
            }
        }while (num<count);
        return result;
    }
}
