package com._4paradigm.openmldb.test_common.openmldb;

import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBInfo;
import com._4paradigm.qa.openmldb_deploy.common.OpenMLDBDeploy;
import com._4paradigm.qa.openmldb_deploy.util.Tool;
import com._4paradigm.test_tool.command_tool.common.ExecutorUtil;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;

import java.util.List;

@Slf4j
public class OpenMLDBDevops {
    private OpenMLDBInfo openMLDBInfo;
    private String dbName;
    private NsClient nsClient;
    private SDKClient sdkClient;
    private String basePath;

    private OpenMLDBDevops(OpenMLDBInfo openMLDBInfo,String dbName){
        this.openMLDBInfo = openMLDBInfo;
        this.dbName = dbName;
        this.nsClient = NsClient.of(openMLDBInfo);
        this.sdkClient = SDKClient.of(new OpenMLDBClient(openMLDBInfo.getZk_cluster(),openMLDBInfo.getZk_root_path()).getExecutor());
        this.basePath = openMLDBInfo.getBasePath();
    }
    public static OpenMLDBDevops of(OpenMLDBInfo openMLDBInfo,String dbName){
        return new OpenMLDBDevops(openMLDBInfo,dbName);
    }
    public void operateTablet(int tabletIndex,String operator){
        String command = String.format("sh %s/openmldb-tablet-%d/bin/start.sh %s tablet",basePath,tabletIndex+1,operator);
        ExecutorUtil.run(command);
        Tool.sleep(5*1000);
        String checkStatus = operator.equals("stop")?"offline":"online";
        sdkClient.checkComponentStatus(openMLDBInfo.getTabletEndpoints().get(tabletIndex), checkStatus);
        nsClient.checkOPStatusDone(dbName,null);
        if(!operator.equals("stop")) {
            nsClient.checkTableIsAlive(dbName, null);
        }
    }
    public void operateStandalone(String operator){
        String command = "";
        switch (operator){
            case "start":
                command = String.format("sh %s/openmldb-standalone/bin/start-standalone.sh",basePath);
                break;
            case "stop":
                command = String.format("sh %s/openmldb-standalone/bin/stop-standalone.sh",basePath);
                break;
        }
        ExecutorUtil.run(command);
        Tool.sleep(5*1000);

    }
    public void operateTablet(String operator){
        int size = openMLDBInfo.getTabletEndpoints().size();
        for(int i=0;i<size;i++){
            operateTablet(i,operator);
        }
    }
    public void operateNs(int nsIndex,String operator){
        String command = String.format("sh %s/openmldb-ns-%d/bin/start.sh %s nameserver",basePath,nsIndex+1,operator);
        ExecutorUtil.run(command);
        Tool.sleep(5*1000);
        String nsEndpoint = openMLDBInfo.getNsEndpoints().get(nsIndex);
        if(operator.equals("stop")){
            sdkClient.checkComponentNotExist(nsEndpoint);
        }else {
            sdkClient.checkComponentStatus(nsEndpoint, "online");
        }
//        nsClient.checkOPStatusDone(dbName,null);
    }
    public void operateApiServer(int apiServerIndex,String operator){
        String command = String.format("sh %s/openmldb-apiserver-%d/bin/start.sh %s apiserver",basePath,apiServerIndex+1,operator);
        ExecutorUtil.run(command);
        Tool.sleep(5*1000);
    }
    public void operateTaskManager(int taskManagerIndex,String operator){
        String command = String.format("sh %s/openmldb-task_manager-%d/bin/start.sh %s taskmanager",basePath,taskManagerIndex+1,operator);
        ExecutorUtil.run(command);
        Tool.sleep(5*1000);
        String taskManagerEndpoint = openMLDBInfo.getTaskManagerEndpoints().get(taskManagerIndex);
        if(operator.equals("stop")){
            sdkClient.checkComponentNotExist(taskManagerEndpoint);
        }else {
            sdkClient.checkComponentStatus(taskManagerEndpoint, "online");
        }
    }
    public void operateZKOne(String operator){
        String command = String.format("sh %s/zookeeper-3.4.14/bin/zkServer.sh %s",basePath,operator);
        ExecutorUtil.run(command);
        Tool.sleep(5*1000);
    }
    public void upgradeNs(String binPath,String confPath){
        String basePath = openMLDBInfo.getBasePath();
        int nsNum = openMLDBInfo.getNsNum();
        for(int i=1;i<=nsNum;i++) {
            log.info("开始升级第{}个ns",i);
            operateNs(i-1,"stop");
            String nsPath = basePath + "/openmldb-ns-"+i;
            backUp(nsPath);
            cpBin(nsPath, binPath);
            cpConf(nsPath, confPath);
            modifyNsConf(nsPath, openMLDBInfo.getNsEndpoints().get(i-1), openMLDBInfo.getZk_cluster());
            operateNs(i-1,"start");
            Tool.sleep(20*1000);
            log.info("第{}个ns升级结束",i);
        }
    }
    public void upgradeTablet(String binPath, String confPath){
        String basePath = openMLDBInfo.getBasePath();
        int tabletNum = openMLDBInfo.getTabletNum();
        for(int i=1;i<=tabletNum;i++) {
            log.info("开始升级第{}个tablet",i);
            operateTablet(i-1,"stop");
            String tabletPath = basePath + "/openmldb-tablet-"+i;
            backUp(tabletPath);
            cpBin(tabletPath, binPath);
            cpConf(tabletPath, confPath);
            modifyTabletConf(tabletPath, openMLDBInfo.getTabletEndpoints().get(i-1), openMLDBInfo.getZk_cluster());
            Tool.sleep(10*1000);
            operateTablet(i-1,"start");
            Tool.sleep(20*1000);
            log.info("第{}个tablet升级结束",i);
        }
    }
    public void upgradeApiServer(String binPath,String confPath){
        String basePath = openMLDBInfo.getBasePath();
        int apiServerNum = openMLDBInfo.getApiServerEndpoints().size();
        for(int i=1;i<=apiServerNum;i++) {
            log.info("开始升级第{}个apiserver",i);
            operateApiServer(i-1,"stop");
            String apiServerPath = basePath + "/openmldb-apiserver-"+i;
            backUp(apiServerPath);
            cpBin(apiServerPath, binPath);
            cpConf(apiServerPath, confPath);
            modifyApiServerConf(apiServerPath, openMLDBInfo.getApiServerEndpoints().get(i-1), openMLDBInfo.getZk_cluster());
            operateApiServer(i-1,"start");
            Tool.sleep(20*1000);
            log.info("第{}个apiserver升级结束",i);
        }
    }
    public void upgradeTaskManager(OpenMLDBDeploy openMLDBDeploy){
        String basePath = openMLDBInfo.getBasePath();
        int taskManagerNum = openMLDBInfo.getTaskManagerEndpoints().size();
        for(int i=1;i<=taskManagerNum;i++) {
            log.info("开始升级第{}个taskmanager",i);
            operateTaskManager(i-1,"stop");
            String taskManagerPath = basePath + "/openmldb-task_manager-"+i;
            backDirectory(taskManagerPath);
            ExecutorUtil.run("rm -rf "+taskManagerPath);
            ExecutorUtil.run("rm -rf "+openMLDBInfo.getSparkHome());
            ExecutorUtil.run("rm -rf "+basePath + "/spark-*.tgz");
            String ipPort = openMLDBInfo.getTaskManagerEndpoints().get(i-1);
            String[] ss = ipPort.split(":");
            String ip = ss[0];
            int port = Integer.parseInt(ss[1]);
            openMLDBDeploy.deployTaskManager(basePath,ip,port,i,openMLDBInfo.getZk_cluster());
            log.info("第{}个taskmanager升级结束",i);
        }
    }
    public void upgradeStandalone(String binPath,String confPath){
        log.info("升级单机版 开始");
        String basePath = openMLDBInfo.getBasePath();
        String standalonePath = basePath + "/openmldb-standalone";
        backUp(standalonePath);
        cpBin(standalonePath, binPath);
        cpConf(standalonePath, confPath);
        modifyStandaloneConf(standalonePath, openMLDBInfo.getNsEndpoints().get(0), openMLDBInfo.getTabletEndpoints().get(0), openMLDBInfo.getApiServerEndpoints().get(0));
        operateStandalone("stop");
        Tool.sleep(10*1000);
        operateStandalone("start");
        Tool.sleep(20*1000);
        log.info("升级单机版 结束");
    }
    public static void backUp(String path){
        String command = "cp -rf "+path +"/conf "+path+"/conf-back";
        ExecutorUtil.run(command);
        command = "ls "+path+" | grep conf-back";
        List<String> result = ExecutorUtil.run(command);
        Assert.assertEquals(result.get(0),"conf-back");
        command = "cp -rf "+path +"/bin "+path+"/bin-back";
        ExecutorUtil.run(command);
        command = "ls "+path+" | grep bin-back";
        result = ExecutorUtil.run(command);
        Assert.assertEquals(result.get(0),"bin-back");
    }
    public static void backDirectory(String path){
        if(path.endsWith("/")){
            path = path.substring(0,path.length()-1);
        }
        String directoryName = path.substring(path.lastIndexOf("/")+1);
        String parentName = path.substring(0,path.lastIndexOf("/"));
        String command = "cp -rf "+path +" "+path+"-back";
        ExecutorUtil.run(command);
        command = "ls "+parentName+" | grep "+directoryName+"-back";
        List<String> result = ExecutorUtil.run(command);
        Assert.assertEquals(result.get(0),directoryName+"-back");
    }
    public static void cpBin(String path, String binPath){
        String command = "rm -rf "+path+"/bin/";
        List<String> result = ExecutorUtil.run(command);
        Assert.assertEquals(result.size(),0);
        command = "cp -rf "+binPath+" "+path;
        result = ExecutorUtil.run(command);
        Assert.assertEquals(result.size(),0);
    }
    public static void cpConf(String path,String confPath){
        String command = "rm -rf "+path+"/conf";
        List<String> result = ExecutorUtil.run(command);
        Assert.assertEquals(result.size(),0);
        command = "cp -rf "+confPath+" "+path;
        result = ExecutorUtil.run(command);
        Assert.assertEquals(result.size(),0);
    }
    public static void modifyNsConf(String nsPath,String ip_port,String zk_endpoint){
        String[] commands = {
                "sed -i 's#--endpoint=.*#--endpoint="+ip_port+"#' "+nsPath+"/conf/nameserver.flags",
                "sed -i 's#--zk_cluster=.*#--zk_cluster=" + zk_endpoint + "#' " + nsPath + "/conf/nameserver.flags",
                "sed -i 's@--zk_root_path=.*@--zk_root_path=/openmldb@' "+nsPath+"/conf/nameserver.flags",
                "sed -i 's@#--zk_cluster=.*@--zk_cluster=" + zk_endpoint + "@' " + nsPath + "/conf/nameserver.flags",
                "sed -i 's@#--zk_root_path=.*@--zk_root_path=/openmldb@' "+nsPath+"/conf/nameserver.flags",
                "sed -i 's@--tablet=.*@#--tablet=127.0.0.1:9921@' "+nsPath+"/conf/nameserver.flags"
        };
        for(String command:commands){
            ExecutorUtil.run(command);
        }
    }
    public static void modifyTabletConf(String tabletPath,String ip_port,String zk_endpoint){
        String[] commands = {
                "sed -i 's#--endpoint=.*#--endpoint="+ip_port+"#' "+tabletPath+"/conf/tablet.flags",
                "sed -i 's/--zk_cluster=.*/--zk_cluster="+zk_endpoint+"/' "+tabletPath+"/conf/tablet.flags",
                "sed -i 's@--zk_root_path=.*@--zk_root_path=/openmldb@' "+tabletPath+"/conf/tablet.flags",
                "sed -i 's@#--zk_cluster=.*@--zk_cluster="+zk_endpoint+"@' "+tabletPath+"/conf/tablet.flags",
                "sed -i 's@#--zk_root_path=.*@--zk_root_path=/openmldb@' "+tabletPath+"/conf/tablet.flags",
                "sed -i 's@#--make_snapshot_threshold_offset=100000@--make_snapshot_threshold_offset=10@' "+tabletPath+"/conf/tablet.flags",
                "sed -i 's@--gc_interval=60@--gc_interval=1@' "+tabletPath+"/conf/tablet.flags",
                "echo '--hdd_root_path=./db_hdd' >> "+tabletPath+"/conf/tablet.flags",
                "echo '--recycle_bin_hdd_root_path=./recycle_hdd' >> "+tabletPath+"/conf/tablet.flags",
                "echo '--ssd_root_path=./db_ssd' >> "+tabletPath+"/conf/tablet.flags",
                "echo '--recycle_bin_ssd_root_path=./recycle_ssd' >> "+tabletPath+"/conf/tablet.flags"
        };
        for(String command:commands){
            ExecutorUtil.run(command);
        }
    }
    public static void modifyApiServerConf(String apiServerPath,String ip_port,String zk_endpoint){
        String[] commands = {
                "sed -i 's#--endpoint=.*#--endpoint="+ip_port+"#' "+apiServerPath+"/conf/apiserver.flags",
                "sed -i 's/--zk_cluster=.*/--zk_cluster="+zk_endpoint+"/' "+apiServerPath+"/conf/apiserver.flags",
                "sed -i 's@--zk_root_path=.*@--zk_root_path=/openmldb@' "+apiServerPath+"/conf/apiserver.flags",
                "sed -i 's@#--zk_cluster=.*@--zk_cluster="+zk_endpoint+"@' "+apiServerPath+"/conf/apiserver.flags",
                "sed -i 's@#--zk_root_path=.*@--zk_root_path=/openmldb@' "+apiServerPath+"/conf/apiserver.flags",
                "sed -i 's@--nameserver=.*@#--nameserver=127.0.0.1:6527@' "+apiServerPath+"/conf/apiserver.flags"
        };
        for(String command:commands){
            ExecutorUtil.run(command);
        }
    }
    public static void modifyTaskManagerConf(String taskManagerPath,String ip_port,String zk_endpoint,String sparkHome){
        String[] ss = ip_port.split(":");
        String ip = ss[0];
        String port = ss[1];
        String sparkMaster = "local";
        String batchJobName = ExecutorUtil.run("ls " + taskManagerPath + "/taskmanager/lib | grep openmldb-batchjob").get(0);
        String batchJobJarPath = taskManagerPath + "/taskmanager/lib/" + batchJobName;
        String[] commands = {
                "sed -i 's#server.host=.*#server.host=" + ip + "#' " + taskManagerPath + "/conf/taskmanager.properties",
                "sed -i 's#server.port=.*#server.port=" + port + "#' " + taskManagerPath+ "/conf/taskmanager.properties",
                "sed -i 's#zookeeper.cluster=.*#zookeeper.cluster=" + zk_endpoint + "#' " + taskManagerPath + "/conf/taskmanager.properties",
                "sed -i 's@zookeeper.root_path=.*@zookeeper.root_path=/openmldb@' "+taskManagerPath+ "/conf/taskmanager.properties",
                "sed -i 's@spark.master=.*@spark.master=" + sparkMaster + "@' "+taskManagerPath+ "/conf/taskmanager.properties",
                "sed -i 's@spark.home=.*@spark.home=" + sparkHome + "@' "+taskManagerPath+ "/conf/taskmanager.properties",
                "sed -i 's@batchjob.jar.path=.*@batchjob.jar.path=" + batchJobJarPath + "@' "+taskManagerPath+ "/conf/taskmanager.properties",
//                "sed -i 's@spark.yarn.jars=.*@spark.yarn.jars=" + sparkYarnJars + "@' "+taskManagerPath+ "/conf/taskmanager.properties",
//                "sed -i 's@offline.data.prefix=.*@offline.data.prefix=" + offlineDataPrefix + "@' "+taskManagerPath+ "/conf/taskmanager.properties",
//                "sed -i 's@namenode.uri=.*@namenode.uri=" + nameNodeUri + "@' "+taskManagerPath+ "/conf/taskmanager.properties"
        };
        for(String command:commands){
            ExecutorUtil.run(command);
        }
    }
    public static void modifyStandaloneConf(String standalonePath,String nsEndpoint,String tabletEndpoint,String apiServerEndpoint){
        String[] commands = {
                "sed -i 's@--zk_cluster=.*@#--zk_cluster=127.0.0.1:2181@' " + standalonePath + "/conf/standalone_nameserver.flags",
                "sed -i 's@--zk_root_path=.*@#--zk_root_path=/openmldb@' "+standalonePath+"/conf/standalone_nameserver.flags",
                "sed -i 's#--endpoint=.*#--endpoint=" + nsEndpoint + "#' " + standalonePath + "/conf/standalone_nameserver.flags",
                "sed -i 's@#--tablet=.*@--tablet=" + tabletEndpoint + "@' " + standalonePath + "/conf/standalone_nameserver.flags",
                "sed -i 's@--tablet=.*@--tablet=" + tabletEndpoint + "@' " + standalonePath + "/conf/standalone_nameserver.flags",
                "sed -i 's@--zk_cluster=.*@#--zk_cluster=127.0.0.1:2181@' " + standalonePath + "/conf/standalone_tablet.flags",
                "sed -i 's@--zk_root_path=.*@#--zk_root_path=/openmldb@' "+standalonePath+"/conf/standalone_tablet.flags",
                "sed -i 's#--endpoint=.*#--endpoint=" + tabletEndpoint + "#' " + standalonePath + "/conf/standalone_tablet.flags",
                "echo -e '\n--hdd_root_path=./db_hdd' >> "+standalonePath+"/conf/standalone_tablet.flags",
                "echo '--recycle_bin_hdd_root_path=./recycle_hdd' >> "+standalonePath+"/conf/standalone_tablet.flags",
                "echo '--ssd_root_path=./db_ssd' >> "+standalonePath+"/conf/standalone_tablet.flags",
                "echo '--recycle_bin_ssd_root_path=./recycle_ssd' >> "+standalonePath+"/conf/standalone_tablet.flags",
                "sed -i 's@--zk_cluster=.*@#--zk_cluster=127.0.0.1:2181@' "+standalonePath+"/conf/standalone_apiserver.flags",
                "sed -i 's@--zk_root_path=.*@#--zk_root_path=/openmldb@' "+standalonePath+"/conf/standalone_apiserver.flags",
                "sed -i 's#--endpoint=.*#--endpoint="+apiServerEndpoint+"#' "+standalonePath+"/conf/standalone_apiserver.flags",
                "sed -i 's#--nameserver=.*#--nameserver="+nsEndpoint+"#' "+standalonePath+"/conf/standalone_apiserver.flags"
        };
        for(String command:commands){
            ExecutorUtil.run(command);
        }
    }
}
