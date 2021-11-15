/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com._4paradigm.openmldb.java_sdk_test.command;

import com._4paradigm.openmldb.java_sdk_test.util.Tool;
import com._4paradigm.openmldb.test_common.bean.FEDBInfo;
import com._4paradigm.openmldb.test_common.bean.OpenMLDBDeployType;
import com._4paradigm.openmldb.test_common.common.LogProxy;
import com._4paradigm.test_tool.command_tool.common.ExecutorUtil;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import java.util.List;

@Slf4j
public class OpenmlDBCommandFactory {
    private static final Logger logger = new LogProxy(log);
    private static String getNoInteractiveCommandByStandalone(String rtidbPath,String host,int port,String dbName,String command){
        String line = "%s --host=%s --port=%s --interactive=false --database=%s --cmd='%s'";
        if(command.contains("'")){
            line = "%s --host=%s --port=%s --interactive=false --database=%s --cmd=\"%s\"";
        }
        line = String.format(line,rtidbPath,host,port,dbName,command);
        // logger.info("generate rtidb no interactive command:{}",line);
        return line;
    }
    private static String getNoInteractiveCommandByCLuster(String rtidbPath,String zkEndPoint,String zkRootPath,String dbName,String command){
        String line = "%s --zk_cluster=%s --zk_root_path=%s --role=sql_client --interactive=false --database=%s --cmd='%s'";
        if(command.contains("'")){
            line = "%s --zk_cluster=%s --zk_root_path=%s --role=sql_client --interactive=false --database=%s --cmd=\"%s\"";
        }
        line = String.format(line,rtidbPath,zkEndPoint,zkRootPath,dbName,command);
        // logger.info("generate rtidb no interactive command:{}",line);
        return line;
    }
    private static String getNoInteractiveCommand(FEDBInfo fedbInfo, String dbName, String command){
        if(fedbInfo.getDeployType()== OpenMLDBDeployType.CLUSTER){
            return getNoInteractiveCommandByCLuster(fedbInfo.getFedbPath(),fedbInfo.getZk_cluster(),fedbInfo.getZk_root_path(),dbName,command);
        }else{
            return getNoInteractiveCommandByStandalone(fedbInfo.getFedbPath(),fedbInfo.getHost(),fedbInfo.getPort(),dbName,command);
        }
    }

    public static List<String> runNoInteractive(FEDBInfo fedbInfo, String dbName, String command){
        return CommandUtil.run(getNoInteractiveCommand(fedbInfo,dbName,command));
    }

}
