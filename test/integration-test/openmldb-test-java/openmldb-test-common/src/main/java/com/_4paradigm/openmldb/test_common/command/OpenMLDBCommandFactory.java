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
package com._4paradigm.openmldb.test_common.command;

import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBDeployType;
import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBInfo;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class OpenMLDBCommandFactory {
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
    private static String getNoInteractiveCommand(OpenMLDBInfo openMLDBInfo, String dbName, String command){
        if(openMLDBInfo.getDeployType()== OpenMLDBDeployType.CLUSTER){
            return getNoInteractiveCommandByCLuster(openMLDBInfo.getOpenMLDBPath(),openMLDBInfo.getZk_cluster(),openMLDBInfo.getZk_root_path(),dbName,command);
        }else{
            return getNoInteractiveCommandByStandalone(openMLDBInfo.getOpenMLDBPath(),openMLDBInfo.getHost(),openMLDBInfo.getPort(),dbName,command);
        }
    }

    public static List<String> runNoInteractive(OpenMLDBInfo openMLDBInfo, String dbName, String command){
        return CommandUtil.run(getNoInteractiveCommand(openMLDBInfo,dbName,command));
    }

}
