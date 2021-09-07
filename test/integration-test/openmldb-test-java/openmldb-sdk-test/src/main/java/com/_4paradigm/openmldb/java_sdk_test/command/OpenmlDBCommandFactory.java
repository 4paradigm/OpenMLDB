package com._4paradigm.openmldb.java_sdk_test.command;

import com._4paradigm.openmldb.java_sdk_test.util.Tool;
import com._4paradigm.openmldb.test_common.bean.FEDBInfo;
import com._4paradigm.openmldb.test_common.common.LogProxy;
import com._4paradigm.test_tool.command_tool.common.ExecutorUtil;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import java.util.List;

@Slf4j
public class OpenmlDBCommandFactory {
    private static final Logger logger = new LogProxy(log);

    private static String getNoInteractiveCommand(String rtidbPath,String zkEndPoint,String zkRootPath,String dbName,String command){
        String line = "%s --zk_cluster=%s --zk_root_path=%s --role=sql_client --interactive=false --database=%s --cmd='%s'";
        line = String.format(line,rtidbPath,zkEndPoint,zkRootPath,dbName,command);
        // logger.info("generate rtidb no interactive command:{}",line);
        return line;
    }
    private static String getNoInteractiveCommand(FEDBInfo fedbInfo, String dbName, String command){
        return getNoInteractiveCommand(fedbInfo.getFedbPath(),fedbInfo.getZk_cluster(),fedbInfo.getZk_root_path(),dbName,command);
    }

    public static List<String> runNoInteractive(FEDBInfo fedbInfo, String dbName, String command){
        return run(getNoInteractiveCommand(fedbInfo,dbName,command));
    }

    private static List<String> run(String command){
        return run(command,1000,30);
    }
    private static List<String> run(String command, int time, int count){
        int num = 0;
        List<String> result;
        do{
            result = ExecutorUtil.run(command);
            if((result.size()==0)||(result.size()==1&&result.get(0).equals("zk client init failed"))){
                num++;
                Tool.sleep(time);
                log.info("command retry:"+num);
            }else {
                return result;
            }
        }while (num<count);
        return result;
    }

}
