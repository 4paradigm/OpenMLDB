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

package com._4paradigm.openmldb.test_common.util;


import com._4paradigm.openmldb.test_common.bean.FEDBInfo;
import com._4paradigm.openmldb.test_common.common.FedbDeployConfig;
import com._4paradigm.test_tool.command_tool.common.ExecutorUtil;
import com._4paradigm.test_tool.command_tool.common.LinuxUtil;
import com.google.common.collect.Lists;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.util.List;

@Slf4j
public class FEDBDeploy {

    private String version;
    private String fedbUrl;
    private String fedbName;
    @Setter
    private String fedbPath;
    @Setter
    private boolean useName;
    @Setter
    private boolean isCluster = true;

    public FEDBDeploy(String version){
        this.version = version;
        this.fedbUrl = FedbDeployConfig.getUrl(version);
    }
    public FEDBInfo deployFEDB(int ns, int tablet){
        return deployFEDB(null,ns,tablet);
    }
    public FEDBInfo deployFEDB(String clusterName, int ns, int tablet){
        FEDBInfo.FEDBInfoBuilder builder = FEDBInfo.builder();
        String testPath = DeployUtil.getTestPath(version);
        if(StringUtils.isNotEmpty(clusterName)) {
            testPath = testPath + "/" + clusterName;
        }
        builder.nsNum(ns).tabletNum(tablet).basePath(testPath);
        String ip = LinuxUtil.getLocalIP();
        File file = new File(testPath);
        if(!file.exists()){
            file.mkdirs();
        }
        int zkPort = deployZK(testPath);
        downloadFEDB(testPath);
        String zk_point = ip+":"+zkPort;
        builder.zk_cluster(zk_point).zk_root_path("/openmldb");
        builder.nsEndpoints(Lists.newArrayList()).nsNames(Lists.newArrayList());
        builder.tabletEndpoints(Lists.newArrayList()).tabletNames(Lists.newArrayList());
        builder.apiServerEndpoints(Lists.newArrayList()).apiServerNames(Lists.newArrayList());
        builder.fedbPath(testPath+"/openmldb-ns-1/bin/openmldb");
        FEDBInfo fedbInfo = builder.build();
        for(int i=1;i<=ns;i++){
            int ns_port;
            if(useName){
                String nsName = clusterName+"-ns-"+i;
                ns_port = deployNS(testPath,null, i, zk_point,nsName);
                fedbInfo.getNsNames().add(nsName);
            }else {
                ns_port = deployNS(testPath, ip, i, zk_point,null);
            }
            fedbInfo.getNsEndpoints().add(ip+":"+ns_port);
        }
        for(int i=1;i<=tablet;i++) {
            int tablet_port ;
            if(useName){
                String tabletName = clusterName+"-tablet-"+i;
                tablet_port = deployTablet(testPath,null, i, zk_point,tabletName);
                fedbInfo.getTabletNames().add(tabletName);
            }else {
                tablet_port = deployTablet(testPath, ip, i, zk_point,null);
            }
            fedbInfo.getTabletEndpoints().add(ip+":"+tablet_port);
        }
        for(int i=1;i<=1;i++) {
            int apiserver_port ;
            if(useName){
                String apiserverName = clusterName+"-apiserver-"+i;
                apiserver_port = deployApiserver(testPath,null, i, zk_point,apiserverName);
                fedbInfo.getApiServerNames().add(apiserverName);
            }else {
                apiserver_port = deployApiserver(testPath, ip, i, zk_point,null);
            }
            fedbInfo.getApiServerEndpoints().add(ip+":"+apiserver_port);
        }
        log.info("openmldb-info:"+fedbInfo);
        return fedbInfo;
    }

    private void downloadFEDB(String testPath){
        try {
            String command = "wget -P " + testPath + " -q " + fedbUrl;
            String packageName = fedbUrl.substring(fedbUrl.lastIndexOf("/")+1);
            ExecutorUtil.run(command);
            command = "ls " + testPath + " | grep "+packageName;
            List<String> result = ExecutorUtil.run(command);
            String tarName = result.get(0);
            command = "tar -zxvf " + testPath + "/"+tarName+" -C "+testPath;
            ExecutorUtil.run(command);
            command = "ls " + testPath + " | grep openmldb | grep -v .tar.gz";
            result = ExecutorUtil.run(command);
            if (result != null && result.size() > 0) {
                fedbName = result.get(0);
                log.info("FEDB下载成功:{}",fedbName);
            }else{
                throw new RuntimeException("FEDB下载失败");
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public int deployZK(String testPath){
        try {
            int port = LinuxUtil.getNoUsedPort();
            String[] commands = {
                    "wget -P "+testPath+" "+ FedbDeployConfig.ZK_URL,
                    "tar -zxvf "+testPath+"/zookeeper-3.4.14.tar.gz -C "+testPath,
                    "cp "+testPath+"/zookeeper-3.4.14/conf/zoo_sample.cfg "+testPath+"/zookeeper-3.4.14/conf/zoo.cfg",
                    "sed -i 's#dataDir=/tmp/zookeeper#dataDir="+testPath+"/data#' "+testPath+"/zookeeper-3.4.14/conf/zoo.cfg",
                    "sed -i 's#clientPort=2181#clientPort="+port+"#' "+testPath+"/zookeeper-3.4.14/conf/zoo.cfg",
                    "sh "+testPath+"/zookeeper-3.4.14/bin/zkServer.sh start"
            };
            for(String command:commands){
                ExecutorUtil.run(command);
            }
            boolean used = LinuxUtil.checkPortIsUsed(port,3000,30);
            if(used){
                log.info("zk部署成功，port："+port);
                return port;
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        throw new RuntimeException("zk部署失败");
    }

    public int deployNS(String testPath, String ip, int index, String zk_endpoint, String name){
        try {
            int port = LinuxUtil.getNoUsedPort();
            String ns_name = "/openmldb-ns-"+index;
            List<String> commands = Lists.newArrayList(
                    "cp -r " + testPath + "/" + fedbName + " " + testPath + ns_name,
                    "sed -i 's#--zk_cluster=.*#--zk_cluster=" + zk_endpoint + "#' " + testPath + ns_name + "/conf/nameserver.flags",
                    "sed -i 's@--zk_root_path=.*@--zk_root_path=/openmldb@' "+testPath+ns_name+"/conf/nameserver.flags",
                    "echo '--request_timeout_ms=60000' >> " + testPath + ns_name + "/conf/nameserver.flags"
            );
            if(useName){
                commands.add("sed -i 's/--endpoint=.*/#&/' " + testPath + ns_name + "/conf/nameserver.flags");
                commands.add("echo '--use_name=true' >> " + testPath + ns_name + "/conf/nameserver.flags");
                commands.add("echo '--port=" + port + "' >> " + testPath + ns_name + "/conf/nameserver.flags");
                if(name!=null){
                    commands.add("mkdir -p " + testPath + ns_name + "/data");
                    commands.add("echo " + name + " >> " + testPath + ns_name + "/data/name.txt");
                }
            }else{
                String ip_port = ip+":"+port;
                commands.add("sed -i 's#--endpoint=.*#--endpoint=" + ip_port + "#' " + testPath + ns_name + "/conf/nameserver.flags");
            }
            if(isCluster){
                commands.add("sed -i 's#--enable_distsql=.*#--enable_distsql=true#' " + testPath + ns_name + "/conf/nameserver.flags");
            }else{
                commands.add("sed -i 's#--enable_distsql=.*#--enable_distsql=false#' " + testPath + ns_name + "/conf/nameserver.flags");
            }
            commands.forEach(ExecutorUtil::run);
            if(StringUtils.isNotEmpty(fedbPath)){
                FEDBCommandUtil.cpRtidb(testPath+ns_name,fedbPath);
            }
//            ExecutorUtil.run("sh "+testPath+ns_name+"/bin/start_ns.sh start");
            ExecutorUtil.run("sh "+testPath+ns_name+"/bin/start.sh start nameserver");
            boolean used = LinuxUtil.checkPortIsUsed(port,3000,30);
            if(used){
                log.info("ns部署成功，port："+port);
                return port;
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        throw new RuntimeException("ns部署失败");
    }
    public int deployTablet(String testPath, String ip, int index, String zk_endpoint, String name){
        try {
            int port = LinuxUtil.getNoUsedPort();
            String tablet_name = "/openmldb-tablet-"+index;
            List<String> commands = Lists.newArrayList(
                    "cp -r "+testPath+"/"+fedbName+" "+testPath+tablet_name,
                    "sed -i 's/--zk_cluster=.*/--zk_cluster="+zk_endpoint+"/' "+testPath+tablet_name+"/conf/tablet.flags",
                    "sed -i 's@--zk_root_path=.*@--zk_root_path=/openmldb@' "+testPath+tablet_name+"/conf/tablet.flags",
                    "sed -i 's@#--make_snapshot_threshold_offset=100000@--make_snapshot_threshold_offset=10@' "+testPath+tablet_name+"/conf/tablet.flags",
                    "sed -i 's@--scan_concurrency_limit=16@--scan_concurrency_limit=0@' "+testPath+tablet_name+"/conf/tablet.flags",
                    "sed -i 's@--put_concurrency_limit=8@--put_concurrency_limit=0@' "+testPath+tablet_name+"/conf/tablet.flags",
                    "sed -i 's@--get_concurrency_limit=16@--get_concurrency_limit=0@' "+testPath+tablet_name+"/conf/tablet.flags"
            );
            if(useName){
                commands.add("sed -i 's/--endpoint=.*/#&/' " + testPath + tablet_name + "/conf/tablet.flags");
                commands.add("echo '--use_name=true' >> " + testPath + tablet_name + "/conf/tablet.flags");
                commands.add("echo '--port=" + port + "' >> " + testPath + tablet_name + "/conf/tablet.flags");
                if(name!=null){
                    commands.add("mkdir -p " + testPath + tablet_name + "/data");
                    commands.add("echo " + name + " >> " + testPath + tablet_name + "/data/name.txt");
                }
            }else{
                String ip_port = ip+":"+port;
                commands.add("sed -i 's#--endpoint=.*#--endpoint="+ip_port+"#' "+testPath+tablet_name+"/conf/tablet.flags");

            }
            if(isCluster){
                commands.add("sed -i 's#--enable_distsql=.*#--enable_distsql=true#' " + testPath + tablet_name + "/conf/tablet.flags");
            }else{
                commands.add("sed -i 's#--enable_distsql=.*#--enable_distsql=false#' " + testPath + tablet_name + "/conf/tablet.flags");
            }
            commands.forEach(ExecutorUtil::run);
            if(StringUtils.isNotEmpty(fedbPath)){
                FEDBCommandUtil.cpRtidb(testPath+tablet_name,fedbPath);
            }
//            ExecutorUtil.run("sh "+testPath+tablet_name+"/bin/start.sh start");
            ExecutorUtil.run("sh "+testPath+tablet_name+"/bin/start.sh start tablet");
            boolean used = LinuxUtil.checkPortIsUsed(port,3000,30);
            if(used){
                log.info("tablet部署成功，port："+port);
                return port;
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        throw new RuntimeException("tablet部署失败");
    }
    public int deployApiserver(String testPath, String ip, int index, String zk_endpoint, String name){
        try {
            int port = LinuxUtil.getNoUsedPort();
            String apiserver_name = "/openmldb-apiserver-"+index;
            List<String> commands = Lists.newArrayList(
                    "cp -r "+testPath+"/"+fedbName+" "+testPath+apiserver_name,
                    "sed -i 's/--zk_cluster=.*/--zk_cluster="+zk_endpoint+"/' "+testPath+apiserver_name+"/conf/apiserver.flags",
                    "sed -i 's@--zk_root_path=.*@--zk_root_path=/openmldb@' "+testPath+apiserver_name+"/conf/apiserver.flags"
            );
            if(useName){
                commands.add("sed -i 's/--endpoint=.*/#&/' " + testPath + apiserver_name + "/conf/apiserver.flags");
                commands.add("echo '--use_name=true' >> " + testPath + apiserver_name + "/conf/apiserver.flags");
                commands.add("echo '--port=" + port + "' >> " + testPath + apiserver_name + "/conf/apiserver.flags");
                if(name!=null){
                    commands.add("mkdir -p " + testPath + apiserver_name + "/data");
                    commands.add("echo " + name + " >> " + testPath + apiserver_name + "/data/name.txt");
                }
            }else{
                String ip_port = ip+":"+port;
                commands.add("sed -i 's#--endpoint=.*#--endpoint="+ip_port+"#' "+testPath+apiserver_name+"/conf/apiserver.flags");

            }
            if(isCluster){
                commands.add("sed -i 's#--enable_distsql=.*#--enable_distsql=true#' " + testPath + apiserver_name + "/conf/apiserver.flags");
            }else{
                commands.add("sed -i 's#--enable_distsql=.*#--enable_distsql=false#' " + testPath + apiserver_name + "/conf/apiserver.flags");
            }
            commands.forEach(ExecutorUtil::run);
            if(StringUtils.isNotEmpty(fedbPath)){
                FEDBCommandUtil.cpRtidb(testPath+apiserver_name,fedbPath);
            }
//            ExecutorUtil.run("sh "+testPath+tablet_name+"/bin/start.sh start");
            ExecutorUtil.run("sh "+testPath+apiserver_name+"/bin/start.sh start apiserver");
            boolean used = LinuxUtil.checkPortIsUsed(port,3000,30);
            if(used){
                log.info("apiserver部署成功，port："+port);
                return port;
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        throw new RuntimeException("apiserver部署失败");
    }
}

