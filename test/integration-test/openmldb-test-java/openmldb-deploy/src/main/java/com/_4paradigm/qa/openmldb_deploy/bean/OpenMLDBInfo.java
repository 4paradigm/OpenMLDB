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

package com._4paradigm.qa.openmldb_deploy.bean;

import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhaowei
 * @date 2021/2/7 12:10 PM
 */
@Data
//@Builder
public class OpenMLDBInfo {
    private OpenMLDBDeployType deployType;
    private String host;
    private int port;
    private String basePath;
    private String openMLDBPath;
    private String openMLDBDirectoryName;
    private String zk_cluster;
    private String zk_root_path;
    private int nsNum;
    private List<String> nsEndpoints = new ArrayList<>();
    private List<String> nsNames = new ArrayList<>();
    private int tabletNum;
    private List<String> tabletEndpoints = new ArrayList<>();
    private List<String> tabletNames = new ArrayList<>();
    private int blobServerNum;
    private List<String> blobServerEndpoints = new ArrayList<>();
    private List<String> blobServerNames = new ArrayList<>();
    private int blobProxyNum;
    private List<String> blobProxyEndpoints = new ArrayList<>();
    private List<String> blobProxyNames = new ArrayList<>();
    private List<String> apiServerEndpoints = new ArrayList<>();
    private List<String> apiServerNames = new ArrayList<>();
    private List<String> taskManagerEndpoints = new ArrayList<>();
    private String runCommand;
    private String sparkHome;

    public String getRunCommand(){
        if(deployType==OpenMLDBDeployType.CLUSTER) {
            return openMLDBPath + " --zk_cluster=" + zk_cluster + " --zk_root_path=" + zk_root_path + " --role=sql_client";
        }else{
            return openMLDBPath + " --host=" + host + " --port=" + port;
        }
    }
}
