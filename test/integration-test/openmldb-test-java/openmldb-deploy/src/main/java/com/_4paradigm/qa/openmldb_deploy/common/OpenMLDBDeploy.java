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

package com._4paradigm.qa.openmldb_deploy.common;

import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBDeployType;
import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBInfo;
import com._4paradigm.qa.openmldb_deploy.conf.OpenMLDBDeployConfig;
import com._4paradigm.qa.openmldb_deploy.util.DeployUtil;
import com._4paradigm.qa.openmldb_deploy.util.OpenMLDBCommandUtil;
import com._4paradigm.qa.openmldb_deploy.util.Tool;
import com._4paradigm.test_tool.command_tool.common.ExecutorUtil;
import com._4paradigm.test_tool.command_tool.common.LinuxUtil;
import com._4paradigm.test_tool.command_tool.conf.CommandConfig;
import com._4paradigm.test_tool.command_tool.util.OSInfoUtil;
import com.google.common.collect.Lists;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.util.List;

@Slf4j
@Setter
public class OpenMLDBDeploy {
    private String installPath;
    private String version;
    private String openMLDBUrl;
    private String openMLDBDirectoryName;
    private String sparkHome;
    private String openMLDBPath;
    private boolean useName;
    private boolean isCluster = true;
    private String sparkMaster = "local";
    private String batchJobJarPath;
    private String sparkYarnJars = "";
    private String offlineDataPrefix = "file:///tmp/openmldb_offline_storage/";
    private String nameNodeUri = "172.27.12.215:8020";
    private int systemTableReplicaNum = 2;
    private String sparkDefaultConf = "";
    private String externalFunctionDir = "./udf/";
    private String hadoopConfDir = "";
    private String hadoopUserName = "root";

    public static final int SLEEP_TIME = 10*1000;

    private String sedSeparator;

    public OpenMLDBDeploy(String version){
        this.version = version;
        this.openMLDBUrl = OpenMLDBDeployConfig.getUrl(version);
        if(CommandConfig.IS_REMOTE){
            this.sedSeparator = "";
        }else {
            this.sedSeparator = OSInfoUtil.isMac() ? "''" : "";
        }
    }
    public OpenMLDBInfo deployStandalone(){
        String testPath = DeployUtil.getTestPath(version);
        if(StringUtils.isNotEmpty(installPath)){
            testPath = installPath+"/"+version;
        }
        String ip = LinuxUtil.getLocalIP();
        File file = new File(testPath);
        if(!file.exists()){
            file.mkdirs();
        }
        downloadOpenMLDB(testPath);
        OpenMLDBInfo openMLDBInfo = deployStandalone(testPath,ip);
        log.info("openmldb-info:"+openMLDBInfo);
        return openMLDBInfo;
    }
    public OpenMLDBInfo deployCluster(int ns, int tablet){
        return deployCluster(null,ns,tablet);
    }
    public OpenMLDBInfo deployCluster(String clusterName, int ns, int tablet){
        OpenMLDBInfo openMLDBInfo = new OpenMLDBInfo();
//        OpenMLDBInfo.OpenMLDBInfoBuilder builder = OpenMLDBInfo.builder();
        openMLDBInfo.setDeployType(OpenMLDBDeployType.CLUSTER);
        String testPath = DeployUtil.getTestPath(version);
        if(StringUtils.isNotEmpty(installPath)){
            testPath = installPath+"/"+version;
        }
        if(StringUtils.isNotEmpty(clusterName)) {
            testPath = testPath + "/" + clusterName;
        }
        openMLDBInfo.setNsNum(ns);
        openMLDBInfo.setTabletNum(tablet);
        openMLDBInfo.setBasePath(testPath);
//        builder.nsNum(ns).tabletNum(tablet).basePath(testPath);
        String ip = LinuxUtil.hostnameI();
        File file = new File(testPath);
        if(!file.exists()){
            file.mkdirs();
        }
        int zkPort = deployZK(testPath);
        String openMLDBDirectoryName = downloadOpenMLDB(testPath);
        String zk_point = ip+":"+zkPort;
        openMLDBInfo.setZk_cluster(zk_point);
        openMLDBInfo.setZk_root_path("/openmldb");
        openMLDBInfo.setNsEndpoints(Lists.newArrayList());
        openMLDBInfo.setNsNames(Lists.newArrayList());
        openMLDBInfo.setTabletEndpoints(Lists.newArrayList());
        openMLDBInfo.setTabletNames(Lists.newArrayList());
        openMLDBInfo.setApiServerEndpoints(Lists.newArrayList());
        openMLDBInfo.setApiServerNames(Lists.newArrayList());
        openMLDBInfo.setTaskManagerEndpoints(Lists.newArrayList());
        openMLDBInfo.setOpenMLDBPath(testPath+"/openmldb-ns-1/bin/openmldb");
        openMLDBInfo.setOpenMLDBDirectoryName(openMLDBDirectoryName);
//        builder.zk_cluster(zk_point).zk_root_path("/openmldb");
//        builder.nsEndpoints(Lists.newArrayList()).nsNames(Lists.newArrayList());
//        builder.tabletEndpoints(Lists.newArrayList()).tabletNames(Lists.newArrayList());
//        builder.apiServerEndpoints(Lists.newArrayList()).apiServerNames(Lists.newArrayList());
//        builder.taskManagerEndpoints(Lists.newArrayList());
//        builder.openMLDBPath(testPath+"/openmldb-ns-1/bin/openmldb");
//        builder.openMLDBDirectoryName(openMLDBDirectoryName);
//        OpenMLDBInfo openMLDBInfo = builder.build();
        for(int i=1;i<=tablet;i++) {
            int tablet_port ;
            if(useName){
                String tabletName = clusterName+"-tablet-"+i;
                tablet_port = deployTablet(testPath,null, i, zk_point,tabletName);
                openMLDBInfo.getTabletNames().add(tabletName);
            }else {
                tablet_port = deployTablet(testPath, ip, i, zk_point,null);
            }
            openMLDBInfo.getTabletEndpoints().add(ip+":"+tablet_port);
            Tool.sleep(SLEEP_TIME);
        }
        for(int i=1;i<=ns;i++){
            int ns_port;
            if(useName){
                String nsName = clusterName+"-ns-"+i;
                ns_port = deployNS(testPath,null, i, zk_point,nsName);
                openMLDBInfo.getNsNames().add(nsName);
            }else {
                ns_port = deployNS(testPath, ip, i, zk_point,null);
            }
            openMLDBInfo.getNsEndpoints().add(ip+":"+ns_port);
            Tool.sleep(SLEEP_TIME);
        }

        for(int i=1;i<=1;i++) {
            int apiserver_port ;
            if(useName){
                String apiserverName = clusterName+"-apiserver-"+i;
                apiserver_port = deployApiserver(testPath,null, i, zk_point,apiserverName);
                openMLDBInfo.getApiServerNames().add(apiserverName);
            }else {
                apiserver_port = deployApiserver(testPath, ip, i, zk_point,null);
            }
            openMLDBInfo.getApiServerEndpoints().add(ip+":"+apiserver_port);
            Tool.sleep(SLEEP_TIME);
        }
        if(version.equals("tmp")||version.compareTo("0.4.0")>=0) {
            for (int i = 1; i <= 1; i++) {
                int task_manager_port = deployTaskManager(testPath, ip, i, zk_point);
                openMLDBInfo.getTaskManagerEndpoints().add(ip + ":" + task_manager_port);
                openMLDBInfo.setSparkHome(sparkHome);
            }
        }
        log.info("openmldb-info:"+openMLDBInfo);
        return openMLDBInfo;
    }

    public String downloadOpenMLDB(String testPath){
        try {
            String command;
            log.info("openMLDBUrl:{}",openMLDBUrl);
            if(openMLDBUrl.startsWith("http")) {
                command = "wget -P " + testPath + " -q " + openMLDBUrl;
            }else{
                command = "cp -r " + openMLDBUrl +" "+ testPath;
            }
            ExecutorUtil.run(command);
            String packageName = openMLDBUrl.substring(openMLDBUrl.lastIndexOf("/") + 1);
            command = "ls " + testPath + " | grep "+packageName;
            List<String> result = ExecutorUtil.run(command);
            String tarName = result.get(0);
            command = "tar -zxvf " + testPath + "/"+tarName+" -C "+testPath;
            ExecutorUtil.run(command);
            command = "ls " + testPath + " | grep openmldb | grep -v .tar.gz";
            result = ExecutorUtil.run(command);
            if (result != null && result.size() > 0) {
                openMLDBDirectoryName = result.get(0);
                log.info("FEDB下载成功:{}", openMLDBDirectoryName);
                return openMLDBDirectoryName;
            }else{
                throw new RuntimeException("FEDB下载失败");
            }
        }catch (Exception e){
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
    public int deployZK(String testPath){
        try {
            int port = LinuxUtil.getNoUsedPort();
            String[] commands = {
                    "wget -P "+testPath+" "+ OpenMLDBDeployConfig.getZKUrl(version),
                    "tar -zxvf "+testPath+"/zookeeper-3.4.14.tar.gz -C "+testPath,
                    "cp "+testPath+"/zookeeper-3.4.14/conf/zoo_sample.cfg "+testPath+"/zookeeper-3.4.14/conf/zoo.cfg",
                    "sed -i "+sedSeparator+" 's#dataDir=/tmp/zookeeper#dataDir="+testPath+"/data#' "+testPath+"/zookeeper-3.4.14/conf/zoo.cfg",
                    "sed -i "+sedSeparator+" 's#clientPort=2181#clientPort="+port+"#' "+testPath+"/zookeeper-3.4.14/conf/zoo.cfg",
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
                    "cp -r " + testPath + "/" + openMLDBDirectoryName + " " + testPath + ns_name,
                    "cp " + testPath + ns_name + "/conf/nameserver.flags.template " + testPath + ns_name + "/conf/nameserver.flags",
                    "sed -i "+sedSeparator+" 's#--zk_cluster=.*#--zk_cluster=" + zk_endpoint + "#' " + testPath + ns_name + "/conf/nameserver.flags",
                    "sed -i "+sedSeparator+" 's@--zk_root_path=.*@--zk_root_path=/openmldb@' "+testPath+ns_name+"/conf/nameserver.flags",
                    "sed -i "+sedSeparator+" 's@#--zk_cluster=.*@--zk_cluster=" + zk_endpoint + "@' " + testPath + ns_name + "/conf/nameserver.flags",
                    "sed -i "+sedSeparator+" 's@#--zk_root_path=.*@--zk_root_path=/openmldb@' "+testPath+ns_name+"/conf/nameserver.flags",
                    "sed -i "+sedSeparator+" 's@--tablet=.*@#--tablet=127.0.0.1:9921@' "+testPath+ns_name+"/conf/nameserver.flags",
                    "sed -i "+sedSeparator+" 's@--tablet_heartbeat_timeout=.*@--tablet_heartbeat_timeout=1000@' "+testPath+ns_name+"/conf/nameserver.flags",
                    "echo '--request_timeout_ms=60000' >> " + testPath + ns_name + "/conf/nameserver.flags"
            );
            // --system_table_replica_num=2
            if(systemTableReplicaNum!=2){
                commands.add("sed -i "+sedSeparator+" 's@--system_table_replica_num=.*@--system_table_replica_num="+systemTableReplicaNum+"@' " + testPath + ns_name + "/conf/nameserver.flags");
            }
            if(useName){
                commands.add("sed -i "+sedSeparator+" 's/--endpoint=.*/#&/' " + testPath + ns_name + "/conf/nameserver.flags");
                commands.add("echo '--use_name=true' >> " + testPath + ns_name + "/conf/nameserver.flags");
                commands.add("echo '--port=" + port + "' >> " + testPath + ns_name + "/conf/nameserver.flags");
                if(name!=null){
                    commands.add("mkdir -p " + testPath + ns_name + "/data");
                    commands.add("echo " + name + " >> " + testPath + ns_name + "/data/name.txt");
                }
            }else{
                String ip_port = ip+":"+port;
                commands.add("sed -i "+sedSeparator+" 's#--endpoint=.*#--endpoint=" + ip_port + "#' " + testPath + ns_name + "/conf/nameserver.flags");
            }
            if(isCluster){
                commands.add("sed -i "+sedSeparator+" 's@#--enable_distsql=.*@--enable_distsql=true@' " + testPath + ns_name + "/conf/nameserver.flags");
                // commands.add("echo '--enable_distsql=true' >> " + testPath + ns_name + "/conf/nameserver.flags");
            }else{
                commands.add("sed -i "+sedSeparator+" 's@#--enable_distsql=.*@--enable_distsql=false@' " + testPath + ns_name + "/conf/nameserver.flags");
            }
            commands.forEach(ExecutorUtil::run);
            if(StringUtils.isNotEmpty(openMLDBPath)){
                OpenMLDBCommandUtil.cpOpenMLDB(testPath+ns_name, openMLDBPath);
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
                    "cp -r "+testPath+"/"+ openMLDBDirectoryName +" "+testPath+tablet_name,
                    "cp " + testPath+tablet_name + "/conf/tablet.flags.template " + testPath+tablet_name + "/conf/tablet.flags",
                    "sed -i "+sedSeparator+" 's/--zk_cluster=.*/--zk_cluster="+zk_endpoint+"/' "+testPath+tablet_name+"/conf/tablet.flags",
                    "sed -i "+sedSeparator+" 's@--zk_root_path=.*@--zk_root_path=/openmldb@' "+testPath+tablet_name+"/conf/tablet.flags",
                    "sed -i "+sedSeparator+" 's@#--zk_cluster=.*@--zk_cluster="+zk_endpoint+"@' "+testPath+tablet_name+"/conf/tablet.flags",
                    "sed -i "+sedSeparator+" 's@#--zk_root_path=.*@--zk_root_path=/openmldb@' "+testPath+tablet_name+"/conf/tablet.flags",
                    "sed -i "+sedSeparator+" 's@#--make_snapshot_threshold_offset=100000@--make_snapshot_threshold_offset=10@' "+testPath+tablet_name+"/conf/tablet.flags",
                    "sed -i "+sedSeparator+" 's@--binlog_single_file_max_size=.*@--binlog_single_file_max_size=1@' "+testPath+tablet_name+"/conf/tablet.flags",
                    "sed -i "+sedSeparator+" 's@--scan_concurrency_limit=16@--scan_concurrency_limit=0@' "+testPath+tablet_name+"/conf/tablet.flags",
                    "sed -i "+sedSeparator+" 's@--put_concurrency_limit=8@--put_concurrency_limit=0@' "+testPath+tablet_name+"/conf/tablet.flags",
                    "sed -i "+sedSeparator+" 's@--get_concurrency_limit=16@--get_concurrency_limit=0@' "+testPath+tablet_name+"/conf/tablet.flags",
                    "echo '--hdd_root_path=./db_hdd' >> "+testPath+tablet_name+"/conf/tablet.flags",
                    "echo '--recycle_bin_hdd_root_path=./recycle_hdd' >> "+testPath+tablet_name+"/conf/tablet.flags",
                    "echo '--ssd_root_path=./db_ssd' >> "+testPath+tablet_name+"/conf/tablet.flags",
                    "echo '--recycle_bin_ssd_root_path=./recycle_ssd' >> "+testPath+tablet_name+"/conf/tablet.flags"
            );
            if(useName){
                commands.add("sed -i "+sedSeparator+" 's/--endpoint=.*/#&/' " + testPath + tablet_name + "/conf/tablet.flags");
                commands.add("echo '--use_name=true' >> " + testPath + tablet_name + "/conf/tablet.flags");
                commands.add("echo '--port=" + port + "' >> " + testPath + tablet_name + "/conf/tablet.flags");
                if(name!=null){
                    commands.add("mkdir -p " + testPath + tablet_name + "/data");
                    commands.add("echo " + name + " >> " + testPath + tablet_name + "/data/name.txt");
                }
            }else{
                String ip_port = ip+":"+port;
                commands.add("sed -i "+sedSeparator+" 's#--endpoint=.*#--endpoint="+ip_port+"#' "+testPath+tablet_name+"/conf/tablet.flags");

            }
            if(isCluster){
                commands.add("sed -i "+sedSeparator+" 's#--enable_distsql=.*#--enable_distsql=true#' " + testPath + tablet_name + "/conf/tablet.flags");
            }else{
                commands.add("sed -i "+sedSeparator+" 's#--enable_distsql=.*#--enable_distsql=false#' " + testPath + tablet_name + "/conf/tablet.flags");
            }
            commands.forEach(ExecutorUtil::run);
            if(StringUtils.isNotEmpty(openMLDBPath)){
                OpenMLDBCommandUtil.cpOpenMLDB(testPath+tablet_name, openMLDBPath);
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
                    "cp -r "+testPath+"/"+ openMLDBDirectoryName +" "+testPath+apiserver_name,
                    "cp " + testPath + apiserver_name + "/conf/apiserver.flags.template " + testPath + apiserver_name + "/conf/apiserver.flags",
                    "sed -i "+sedSeparator+" 's/--zk_cluster=.*/--zk_cluster="+zk_endpoint+"/' "+testPath+apiserver_name+"/conf/apiserver.flags",
                    "sed -i "+sedSeparator+" 's@--zk_root_path=.*@--zk_root_path=/openmldb@' "+testPath+apiserver_name+"/conf/apiserver.flags",
                    "sed -i "+sedSeparator+" 's@#--zk_cluster=.*@--zk_cluster="+zk_endpoint+"@' "+testPath+apiserver_name+"/conf/apiserver.flags",
                    "sed -i "+sedSeparator+" 's@#--zk_root_path=.*@--zk_root_path=/openmldb@' "+testPath+apiserver_name+"/conf/apiserver.flags",
                    "sed -i "+sedSeparator+" 's@--nameserver=.*@#--nameserver=127.0.0.1:6527@' "+testPath+apiserver_name+"/conf/apiserver.flags"
            );
            if(useName){
                commands.add("sed -i "+sedSeparator+" 's/--endpoint=.*/#&/' " + testPath + apiserver_name + "/conf/apiserver.flags");
                commands.add("echo '--use_name=true' >> " + testPath + apiserver_name + "/conf/apiserver.flags");
                commands.add("echo '--port=" + port + "' >> " + testPath + apiserver_name + "/conf/apiserver.flags");
                if(name!=null){
                    commands.add("mkdir -p " + testPath + apiserver_name + "/data");
                    commands.add("echo " + name + " >> " + testPath + apiserver_name + "/data/name.txt");
                }
            }else{
                String ip_port = ip+":"+port;
                commands.add("sed -i "+sedSeparator+" 's#--endpoint=.*#--endpoint="+ip_port+"#' "+testPath+apiserver_name+"/conf/apiserver.flags");

            }
            if(isCluster){
                commands.add("sed -i "+sedSeparator+" 's#--enable_distsql=.*#--enable_distsql=true#' " + testPath + apiserver_name + "/conf/apiserver.flags");
            }else{
                commands.add("sed -i "+sedSeparator+" 's#--enable_distsql=.*#--enable_distsql=false#' " + testPath + apiserver_name + "/conf/apiserver.flags");
            }
            commands.forEach(ExecutorUtil::run);
            if(StringUtils.isNotEmpty(openMLDBPath)){
                OpenMLDBCommandUtil.cpOpenMLDB(testPath+apiserver_name, openMLDBPath);
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
            ExecutorUtil.run("wget -P "+testPath+" -q "+ OpenMLDBDeployConfig.getSparkUrl(version));
            String tarName = ExecutorUtil.run("ls "+ testPath +" | grep spark.tgz").get(0);
            ExecutorUtil.run("tar -zxvf " + testPath + "/"+tarName+" -C "+testPath);
            String sparkDirectoryName = ExecutorUtil.run("ls "+ testPath +" | grep spark | grep  -v .tgz ").get(0);
            String sparkPath = testPath+"/"+sparkDirectoryName;
            this.sparkHome = sparkPath;
            return sparkPath;
        }catch (Exception e){
            e.printStackTrace();
        }
        throw new RuntimeException("spark 部署失败");
    }
    public int deployTaskManager(String testPath, String ip, int index, String zk_endpoint){
        int port = LinuxUtil.getNoUsedPort();
        return deployTaskManager(testPath,ip,port,index,zk_endpoint);
    }
    public int deployTaskManager(String testPath, String ip, int port, int index, String zk_endpoint){
        try {
            String sparkHome = deploySpark(testPath);
            String task_manager_name = "/openmldb-task_manager-"+index;
            ExecutorUtil.run("cp -r " + testPath + "/" + openMLDBDirectoryName + " " + testPath + task_manager_name);
            if(batchJobJarPath==null) {
                String batchJobName = ExecutorUtil.run("ls " + testPath + task_manager_name + "/taskmanager/lib | grep openmldb-batchjob").get(0);
                batchJobJarPath = testPath + task_manager_name + "/taskmanager/lib/" + batchJobName;
            }

            List<String> commands = Lists.newArrayList(
                    "cp " + testPath + task_manager_name + "/conf/taskmanager.properties.template " + testPath + task_manager_name + "/conf/taskmanager.properties",
                    "sed -i "+sedSeparator+" 's#server.host=.*#server.host=" + ip + "#' " + testPath + task_manager_name + "/conf/taskmanager.properties",
                    "sed -i "+sedSeparator+" 's#server.port=.*#server.port=" + port + "#' " + testPath + task_manager_name + "/conf/taskmanager.properties",
                    "sed -i "+sedSeparator+" 's#zookeeper.cluster=.*#zookeeper.cluster=" + zk_endpoint + "#' " + testPath + task_manager_name + "/conf/taskmanager.properties",
                    "sed -i "+sedSeparator+" 's@zookeeper.root_path=.*@zookeeper.root_path=/openmldb@' "+testPath + task_manager_name+ "/conf/taskmanager.properties",
                    "sed -i "+sedSeparator+" 's@spark.master=.*@spark.master=" + sparkMaster + "@' "+testPath + task_manager_name+ "/conf/taskmanager.properties",
                    "sed -i "+sedSeparator+" 's@spark.home=.*@spark.home=" + sparkHome + "@' "+testPath + task_manager_name+ "/conf/taskmanager.properties",
                    "sed -i "+sedSeparator+" 's@batchjob.jar.path=.*@batchjob.jar.path=" + batchJobJarPath + "@' "+testPath + task_manager_name+ "/conf/taskmanager.properties",
                    "sed -i "+sedSeparator+" 's@spark.yarn.jars=.*@spark.yarn.jars=" + sparkYarnJars + "@' "+testPath + task_manager_name+ "/conf/taskmanager.properties",
                    "sed -i "+sedSeparator+" 's@offline.data.prefix=.*@offline.data.prefix=" + offlineDataPrefix + "@' "+testPath + task_manager_name+ "/conf/taskmanager.properties",
                    "sed -i "+sedSeparator+" 's@namenode.uri=.*@namenode.uri=" + nameNodeUri + "@' "+testPath + task_manager_name+ "/conf/taskmanager.properties",
                    "sed -i "+sedSeparator+" 's@spark.default.conf=.*@spark.default.conf=" + sparkDefaultConf + "@' "+testPath + task_manager_name+ "/conf/taskmanager.properties",
                    "echo -e \"\nexternal.function.dir=" + externalFunctionDir + "\" >> " +testPath + task_manager_name + "/conf/taskmanager.properties",
                    "echo -e \"\nhadoop.conf.dir=" + hadoopConfDir + "\" >> " +testPath + task_manager_name + "/conf/taskmanager.properties",
                    "echo -e \"\nhadoop.user.name=" + hadoopUserName + "\" >> " +testPath + task_manager_name + "/conf/taskmanager.properties"
            );

            commands.forEach(ExecutorUtil::run);

            // Download dynamic library file
            ExecutorUtil.run("curl -o /tmp/libtest_udf.so https://www.openmldb.com/download/test/self_host_hadoop_config/libtest_udf.so");

            String taskmanagerUdfPath = testPath + task_manager_name + "/taskmanager/bin/udf/";
            //ExecutorUtil.run("touch " + taskmanagerUdfPath);
            //ExecutorUtil.run("cp /tmp/libtest_udf.so " + taskmanagerUdfPath);

            String tabletUdfPath = testPath + "/openmldb-tablet-1/udf/";
            ExecutorUtil.run("touch " + tabletUdfPath);
            ExecutorUtil.run("cp /tmp/libtest_udf.so " + tabletUdfPath);

            tabletUdfPath = testPath + "/openmldb-tablet-2/udf/";
            ExecutorUtil.run("touch " + tabletUdfPath);
            ExecutorUtil.run("cp /tmp/libtest_udf.so " + tabletUdfPath);

            tabletUdfPath = testPath + "/openmldb-tablet-3/udf/";
            ExecutorUtil.run("touch " + tabletUdfPath);
            ExecutorUtil.run("cp /tmp/libtest_udf.so " + tabletUdfPath);

            if (sparkMaster.startsWith("yarn")) {
                log.info("Try to deploy TaskManager with yarn mode");
                ExecutorUtil.run("curl -o /tmp/hadoop_conf.tar.gz https://www.openmldb.com/download/test/self_host_hadoop_config/hadoop_conf.tar.gz");
                ExecutorUtil.run("tar xzf /tmp/hadoop_conf.tar.gz -C /tmp");
                ExecutorUtil.run("HADOOP_CONF_DIR=/tmp/hadoop/ sh "+testPath+task_manager_name+"/bin/start.sh start taskmanager");
            } else {
                ExecutorUtil.run("sh "+testPath+task_manager_name+"/bin/start.sh start taskmanager");
            }

            /*
            String command = "cat " +testPath+task_manager_name + "/taskmanager/bin/logs/taskmanager.log";
            List<String> result = ExecutorUtil.run(command);
            for (String line : result) {
                System.out.println(line);
            }
            */

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

    public OpenMLDBInfo deployStandalone(String testPath, String ip){
        try {
            int nsPort = LinuxUtil.getNoUsedPort();
            int tabletPort = LinuxUtil.getNoUsedPort();
            int apiServerPort = LinuxUtil.getNoUsedPort();
            String nsEndpoint = ip+":"+nsPort;
            String tabletEndpoint = ip+":"+tabletPort;
            String apiServerEndpoint = ip+":"+apiServerPort;
            String standaloneName = "/openmldb-standalone";
            List<String> commands = Lists.newArrayList(
                    "cp -r " + testPath + "/" + openMLDBDirectoryName + " " + testPath + standaloneName,
                    "sed -i "+sedSeparator+" 's@--zk_cluster=.*@#--zk_cluster=127.0.0.1:2181@' " + testPath + standaloneName + "/conf/standalone_nameserver.flags",
                    "sed -i "+sedSeparator+" 's@--zk_root_path=.*@#--zk_root_path=/openmldb@' "+testPath+standaloneName+"/conf/standalone_nameserver.flags",
                    "sed -i "+sedSeparator+" 's#--endpoint=.*#--endpoint=" + nsEndpoint + "#' " + testPath + standaloneName + "/conf/standalone_nameserver.flags",
                    "sed -i "+sedSeparator+" 's@#--tablet=.*@--tablet=" + tabletEndpoint + "@' " + testPath + standaloneName + "/conf/standalone_nameserver.flags",
                    "sed -i "+sedSeparator+" 's@--tablet=.*@--tablet=" + tabletEndpoint + "@' " + testPath + standaloneName + "/conf/standalone_nameserver.flags",
                    "sed -i "+sedSeparator+" 's@--zk_cluster=.*@#--zk_cluster=127.0.0.1:2181@' " + testPath + standaloneName + "/conf/standalone_tablet.flags",
                    "sed -i "+sedSeparator+" 's@--zk_root_path=.*@#--zk_root_path=/openmldb@' "+testPath+standaloneName+"/conf/standalone_tablet.flags",
                    "sed -i "+sedSeparator+" 's#--endpoint=.*#--endpoint=" + tabletEndpoint + "#' " + testPath + standaloneName + "/conf/standalone_tablet.flags",
                    "echo -e '\n--hdd_root_path=./db_hdd' >> "+testPath+standaloneName+"/conf/standalone_tablet.flags",
                    "echo '--recycle_bin_hdd_root_path=./recycle_hdd' >> "+testPath+standaloneName+"/conf/standalone_tablet.flags",
                    "echo '--ssd_root_path=./db_ssd' >> "+testPath+standaloneName+"/conf/standalone_tablet.flags",
                    "echo '--recycle_bin_ssd_root_path=./recycle_ssd' >> "+testPath+standaloneName+"/conf/standalone_tablet.flags",
                    "sed -i "+sedSeparator+" 's@--zk_cluster=.*@#--zk_cluster=127.0.0.1:2181@' "+testPath+standaloneName+"/conf/standalone_apiserver.flags",
                    "sed -i "+sedSeparator+" 's@--zk_root_path=.*@#--zk_root_path=/openmldb@' "+testPath+standaloneName+"/conf/standalone_apiserver.flags",
                    "sed -i "+sedSeparator+" 's#--endpoint=.*#--endpoint="+apiServerEndpoint+"#' "+testPath+standaloneName+"/conf/standalone_apiserver.flags",
                    "sed -i "+sedSeparator+" 's#--nameserver=.*#--nameserver="+nsEndpoint+"#' "+testPath+standaloneName+"/conf/standalone_apiserver.flags"
            );
            commands.forEach(ExecutorUtil::run);
            if(StringUtils.isNotEmpty(openMLDBPath)){
                OpenMLDBCommandUtil.cpOpenMLDB(testPath+standaloneName, openMLDBPath);
            }
            ExecutorUtil.run("sh "+testPath+standaloneName+"/sbin/start-all.sh");
            boolean nsOk = LinuxUtil.checkPortIsUsed(nsPort,3000,30);
            boolean tabletOk = LinuxUtil.checkPortIsUsed(tabletPort,3000,30);
            boolean apiServerOk = LinuxUtil.checkPortIsUsed(apiServerPort,3000,30);
            if(nsOk&&tabletOk&&apiServerOk){
                log.info(String.format("standalone 部署成功,nsPort：{},tabletPort:{},apiServerPort:{}",nsPort,tabletPort,apiServerPort));
                OpenMLDBInfo openMLDBInfo = new OpenMLDBInfo();
                openMLDBInfo.setDeployType(OpenMLDBDeployType.STANDALONE);
                openMLDBInfo.setNsNum(1);
                openMLDBInfo.setTabletNum(1);
                openMLDBInfo.setBasePath(testPath);
                openMLDBInfo.setHost(ip);
                openMLDBInfo.setPort(nsPort);
                openMLDBInfo.setNsEndpoints(Lists.newArrayList(nsEndpoint));
                openMLDBInfo.setNsNames(Lists.newArrayList());
                openMLDBInfo.setTabletEndpoints(Lists.newArrayList(tabletEndpoint));
                openMLDBInfo.setTabletNames(Lists.newArrayList());
                openMLDBInfo.setApiServerEndpoints(Lists.newArrayList(apiServerEndpoint));
                openMLDBInfo.setApiServerNames(Lists.newArrayList());
                openMLDBInfo.setOpenMLDBPath(testPath+"/openmldb-standalone/bin/openmldb");

//                OpenMLDBInfo openMLDBInfo = OpenMLDBInfo.builder()
//                        .deployType(OpenMLDBDeployType.STANDALONE)
//                        .openMLDBPath(testPath+"/openmldb-standalone/bin/openmldb")
//                        .apiServerEndpoints(Lists.newArrayList())
//                        .basePath(testPath)
//                        .nsEndpoints(Lists.newArrayList(nsEndpoint))
//                        .nsNum(1)
//                        .host(ip)
//                        .port(nsPort)
//                        .tabletNum(1)
//                        .tabletEndpoints(Lists.newArrayList(tabletEndpoint))
//                        .apiServerEndpoints(Lists.newArrayList(apiServerEndpoint))
//                        .build();
                return openMLDBInfo;
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        throw new RuntimeException("standalone 部署失败");
    }
}

