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
import com._4paradigm.openmldb.test_common.bean.OpenMLDBDeployType;
import com._4paradigm.openmldb.test_common.common.FedbDeployConfig;
import com._4paradigm.test_tool.command_tool.common.ExecutorUtil;
import com._4paradigm.test_tool.command_tool.common.LinuxUtil;
import com.google.common.collect.Lists;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import sun.tools.jar.resources.jar;

import java.io.File;
import java.util.List;

@Slf4j
@Setter
public class FEDBDeploy {
    private String version;
    private String fedbUrl;
    private String fedbName;
    private String fedbPath;
    private boolean useName;
    private boolean isCluster = true;
    private String sparkMaster = "local";
    private String batchJobJarPath;
    private String sparkYarnJars = "";
    private String offlineDataPrefix = "file:///tmp/openmldb_offline_storage/";
    private String nameNodeUri = "172.27.12.215:8020";

    public static final int SLEEP_TIME = 10*1000;

    public FEDBDeploy(String version){
        this.version = version;
        this.fedbUrl = FedbDeployConfig.getUrl(version);
    }
    public FEDBInfo deployFEDBByStandalone(){
        String testPath = DeployUtil.getTestPath(version);
        String ip = LinuxUtil.getLocalIP();
        File file = new File(testPath);
        if(!file.exists()){
            file.mkdirs();
        }
        downloadFEDB(testPath);
        FEDBInfo fedbInfo = deployStandalone(testPath,ip);
        log.info("openmldb-info:"+fedbInfo);
        return fedbInfo;
    }
    public FEDBInfo deployFEDB(int ns, int tablet){
        return deployFEDB(null,ns,tablet);
    }
    public FEDBInfo deployFEDB(String clusterName, int ns, int tablet){
        FEDBInfo.FEDBInfoBuilder builder = FEDBInfo.builder();
        builder.deployType(OpenMLDBDeployType.CLUSTER);
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
        builder.taskManagerEndpoints(Lists.newArrayList());
        builder.fedbPath(testPath+"/openmldb-ns-1/bin/openmldb");
        FEDBInfo fedbInfo = builder.build();
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
            FedbTool.sleep(SLEEP_TIME);
        }
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
            FedbTool.sleep(SLEEP_TIME);
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
            FedbTool.sleep(SLEEP_TIME);
        }
        if(version.equals("tmp")||version.compareTo("0.4.0")>=0) {
            for (int i = 1; i <= 1; i++) {
                int task_manager_port = deployTaskManager(testPath, ip, i, zk_point);
                fedbInfo.getTaskManagerEndpoints().add(ip + ":" + task_manager_port);
            }
        }
        log.info("openmldb-info:"+fedbInfo);
        return fedbInfo;
    }

    private void downloadFEDB(String testPath){
        try {
            String command;
            if(fedbUrl.startsWith("http")) {
                command = "wget -P " + testPath + " -q " + fedbUrl;
            }else{
                command = "cp -r " + fedbUrl +" "+ testPath;
            }
            ExecutorUtil.run(command);
            String packageName = fedbUrl.substring(fedbUrl.lastIndexOf("/") + 1);
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
                    "wget -P "+testPath+" "+ FedbDeployConfig.getZKUrl(version),
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
                    "sed -i 's@#--zk_cluster=.*@--zk_cluster=" + zk_endpoint + "@' " + testPath + ns_name + "/conf/nameserver.flags",
                    "sed -i 's@#--zk_root_path=.*@--zk_root_path=/openmldb@' "+testPath+ns_name+"/conf/nameserver.flags",
                    "sed -i 's@--tablet=.*@#--tablet=127.0.0.1:9921@' "+testPath+ns_name+"/conf/nameserver.flags",
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
                commands.add("echo '--enable_distsql=true' >> " + testPath + ns_name + "/conf/nameserver.flags");
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
                    "sed -i 's@#--zk_cluster=.*@--zk_cluster="+zk_endpoint+"@' "+testPath+tablet_name+"/conf/tablet.flags",
                    "sed -i 's@#--zk_root_path=.*@--zk_root_path=/openmldb@' "+testPath+tablet_name+"/conf/tablet.flags",
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
                    "sed -i 's@--zk_root_path=.*@--zk_root_path=/openmldb@' "+testPath+apiserver_name+"/conf/apiserver.flags",
                    "sed -i 's@#--zk_cluster=.*@--zk_cluster="+zk_endpoint+"@' "+testPath+apiserver_name+"/conf/apiserver.flags",
                    "sed -i 's@#--zk_root_path=.*@--zk_root_path=/openmldb@' "+testPath+apiserver_name+"/conf/apiserver.flags",
                    "sed -i 's@--nameserver=.*@#--nameserver=127.0.0.1:6527@' "+testPath+apiserver_name+"/conf/apiserver.flags"
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
    
    
    public String deploySpark(String testPath){
        try {
            ExecutorUtil.run("wget -P "+testPath+" -q "+ FedbDeployConfig.getSparkUrl(version));
            String tarName = ExecutorUtil.run("ls "+ testPath +" | grep spark").get(0);
            ExecutorUtil.run("tar -zxvf " + testPath + "/"+tarName+" -C "+testPath);
            String sparkHome = ExecutorUtil.run("ls "+ testPath +" | grep spark | grep  -v .tgz ").get(0);
            String sparkPath = testPath+"/"+sparkHome;
            return sparkPath;
        }catch (Exception e){
            e.printStackTrace();
        }
        throw new RuntimeException("spark 部署失败");
    }

    public int deployTaskManager(String testPath, String ip, int index, String zk_endpoint){
        try {
            String sparkHome = deploySpark(testPath);
            int port = LinuxUtil.getNoUsedPort();
            String task_manager_name = "/openmldb-task_manager-"+index;
            ExecutorUtil.run("cp -r " + testPath + "/" + fedbName + " " + testPath + task_manager_name);
            if(batchJobJarPath==null) {
                String batchJobName = ExecutorUtil.run("ls " + testPath + task_manager_name + "/taskmanager/lib | grep openmldb-batchjob").get(0);
                batchJobJarPath = testPath + task_manager_name + "/taskmanager/lib/" + batchJobName;
            }

            List<String> commands = Lists.newArrayList(
                    "sed -i 's#server.host=.*#server.host=" + ip + "#' " + testPath + task_manager_name + "/conf/taskmanager.properties",
                    "sed -i 's#server.port=.*#server.port=" + port + "#' " + testPath + task_manager_name + "/conf/taskmanager.properties",
                    "sed -i 's#zookeeper.cluster=.*#zookeeper.cluster=" + zk_endpoint + "#' " + testPath + task_manager_name + "/conf/taskmanager.properties",
                    "sed -i 's@zookeeper.root_path=.*@zookeeper.root_path=/openmldb@' "+testPath + task_manager_name+ "/conf/taskmanager.properties",
                    "sed -i 's@spark.master=.*@spark.master=" + sparkMaster + "@' "+testPath + task_manager_name+ "/conf/taskmanager.properties",
                    "sed -i 's@spark.home=.*@spark.home=" + sparkHome + "@' "+testPath + task_manager_name+ "/conf/taskmanager.properties",
                    "sed -i 's@batchjob.jar.path=.*@batchjob.jar.path=" + batchJobJarPath + "@' "+testPath + task_manager_name+ "/conf/taskmanager.properties",
                    "sed -i 's@spark.yarn.jars=.*@spark.yarn.jars=" + sparkYarnJars + "@' "+testPath + task_manager_name+ "/conf/taskmanager.properties",
                    "sed -i 's@offline.data.prefix=.*@offline.data.prefix=" + offlineDataPrefix + "@' "+testPath + task_manager_name+ "/conf/taskmanager.properties",
                    "sed -i 's@namenode.uri=.*@namenode.uri=" + nameNodeUri + "@' "+testPath + task_manager_name+ "/conf/taskmanager.properties"
            );
            commands.forEach(ExecutorUtil::run);
            ExecutorUtil.run("sh "+testPath+task_manager_name+"/bin/start.sh start taskmanager");
            boolean used = LinuxUtil.checkPortIsUsed(port,3000,30);
            if(used){
                log.info("task manager部署成功，port："+port);
                return port;
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        throw new RuntimeException("task manager部署失败");
    }

    public FEDBInfo deployStandalone(String testPath, String ip){
        try {
            int nsPort = LinuxUtil.getNoUsedPort();
            int tabletPort = LinuxUtil.getNoUsedPort();
            int apiServerPort = LinuxUtil.getNoUsedPort();
            String nsEndpoint = ip+":"+nsPort;
            String tabletEndpoint = ip+":"+tabletPort;
            String apiServerEndpoint = ip+":"+apiServerPort;
            String standaloneName = "/openmldb-standalone";
            List<String> commands = Lists.newArrayList(
                    "cp -r " + testPath + "/" + fedbName + " " + testPath + standaloneName,
                    "sed -i 's@--zk_cluster=.*@#--zk_cluster=127.0.0.1:2181@' " + testPath + standaloneName + "/conf/standalone_nameserver.flags",
                    "sed -i 's@--zk_root_path=.*@#--zk_root_path=/openmldb@' "+testPath+standaloneName+"/conf/standalone_nameserver.flags",
                    "sed -i 's#--endpoint=.*#--endpoint=" + nsEndpoint + "#' " + testPath + standaloneName + "/conf/standalone_nameserver.flags",
                    "sed -i 's@#--tablet=.*@--tablet=" + tabletEndpoint + "@' " + testPath + standaloneName + "/conf/standalone_nameserver.flags",
                    "sed -i 's@--tablet=.*@--tablet=" + tabletEndpoint + "@' " + testPath + standaloneName + "/conf/standalone_nameserver.flags",
                    "sed -i 's@--zk_cluster=.*@#--zk_cluster=127.0.0.1:2181@' " + testPath + standaloneName + "/conf/standalone_tablet.flags",
                    "sed -i 's@--zk_root_path=.*@#--zk_root_path=/openmldb@' "+testPath+standaloneName+"/conf/standalone_tablet.flags",
                    "sed -i 's#--endpoint=.*#--endpoint=" + tabletEndpoint + "#' " + testPath + standaloneName + "/conf/standalone_tablet.flags",
                    "sed -i 's@--zk_cluster=.*@#--zk_cluster=127.0.0.1:2181@' "+testPath+standaloneName+"/conf/standalone_apiserver.flags",
                    "sed -i 's@--zk_root_path=.*@#--zk_root_path=/openmldb@' "+testPath+standaloneName+"/conf/standalone_apiserver.flags",
                    "sed -i 's#--endpoint=.*#--endpoint="+apiServerEndpoint+"#' "+testPath+standaloneName+"/conf/standalone_apiserver.flags",
                    "sed -i 's#--nameserver=.*#--nameserver="+nsEndpoint+"#' "+testPath+standaloneName+"/conf/standalone_apiserver.flags"
            );
            commands.forEach(ExecutorUtil::run);
            if(StringUtils.isNotEmpty(fedbPath)){
                FEDBCommandUtil.cpRtidb(testPath+standaloneName,fedbPath);
            }
            ExecutorUtil.run("sh "+testPath+standaloneName+"/bin/start-standalone.sh");
            boolean nsOk = LinuxUtil.checkPortIsUsed(nsPort,3000,30);
            boolean tabletOk = LinuxUtil.checkPortIsUsed(tabletPort,3000,30);
            boolean apiServerOk = LinuxUtil.checkPortIsUsed(apiServerPort,3000,30);
            if(nsOk&&tabletOk&&apiServerOk){
                log.info(String.format("standalone 部署成功,nsPort：{},tabletPort:{},apiServerPort:{}",nsPort,tabletPort,apiServerPort));
                FEDBInfo fedbInfo = FEDBInfo.builder()
                        .deployType(OpenMLDBDeployType.STANDALONE)
                        .fedbPath(testPath+"/openmldb-standalone/bin/openmldb")
                        .apiServerEndpoints(Lists.newArrayList())
                        .basePath(testPath)
                        .nsEndpoints(Lists.newArrayList(nsEndpoint))
                        .nsNum(1)
                        .host(ip)
                        .port(nsPort)
                        .tabletNum(1)
                        .tabletEndpoints(Lists.newArrayList(tabletEndpoint))
                        .apiServerEndpoints(Lists.newArrayList(apiServerEndpoint))
                        .build();
                return fedbInfo;
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        throw new RuntimeException("standalone 部署失败");
    }
}

