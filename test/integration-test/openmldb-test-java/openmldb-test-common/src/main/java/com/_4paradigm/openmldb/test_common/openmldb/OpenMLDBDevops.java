package com._4paradigm.openmldb.test_common.openmldb;

import com._4paradigm.openmldb.test_common.bean.OpenMLDBResult;
import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBInfo;
import com._4paradigm.qa.openmldb_deploy.util.Tool;
import com._4paradigm.test_tool.command_tool.common.ExecutorUtil;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
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
    public void operateZKOne(String operator){
        String command = String.format("sh %s/zookeeper-3.4.14/bin/zkServer.sh %s",basePath,operator);
        ExecutorUtil.run(command);
        Tool.sleep(5*1000);
    }
    public void upgradeNs(String openMLDBPath,String confPath){
        String basePath = openMLDBInfo.getBasePath();
        int nsNum = openMLDBInfo.getNsNum();
        for(int i=1;i<=nsNum;i++) {
            log.info("开始升级第{}个ns",i);
            String nsPath = basePath + "/openmldb-ns-"+i;
            backUp(nsPath);
            cpOpenMLDB(nsPath, openMLDBPath);
            cpConf(nsPath, confPath);
            modifyNsConf(nsPath, openMLDBInfo.getNsEndpoints().get(i-1), openMLDBInfo.getZk_cluster());
            operateNs(i,"restart");
            Tool.sleep(20*1000);
            log.info("第{}个ns升级结束",i);
        }
    }
    public void upgradeTablet(String openMLDBPath, String confPath){
        String basePath = openMLDBInfo.getBasePath();
        int tabletNum = openMLDBInfo.getTabletNum();
        for(int i=1;i<=tabletNum;i++) {
            log.info("开始升级第{}个tablet",i);
            String tabletPath = basePath + "/openmldb-tablet-"+i;
            backUp(tabletPath);
            cpOpenMLDB(tabletPath, openMLDBPath);
            cpConf(tabletPath, confPath);
            modifyTabletConf(tabletPath, openMLDBInfo.getTabletEndpoints().get(i-1), openMLDBInfo.getZk_cluster());
            operateTablet(i,"stop");
            Tool.sleep(10*1000);
            operateTablet(i,"start");
            Tool.sleep(20*1000);
            log.info("第{}个tablet升级结束",i);
        }
    }
    public static void backUp(String path){
        String command = "cp -rf "+path +"/conf "+path+"/conf-back";
        ExecutorUtil.run(command);
        command = "ls "+path+" | grep conf-back";
        List<String> result = ExecutorUtil.run(command);
        Assert.assertEquals(result.get(0),"conf-back");
        command = "cp -rf "+path +"/bin/openmldb "+path+"/openmldb.back";
        ExecutorUtil.run(command);
        command = "ls "+path+" | grep openmldb.back";
        result = ExecutorUtil.run(command);
        Assert.assertEquals(result.get(0),"openmldb.back");
    }
    public static void cpOpenMLDB(String path,String openMLDBPath){
        String command = "rm -rf "+path+"/bin/openmldb";
        List<String> result = ExecutorUtil.run(command);
        Assert.assertEquals(result.size(),0);
        command = "cp -rf "+openMLDBPath+" "+path+"/bin";
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
                "sed -i 's#--zk_cluster=.*#--zk_cluster="+zk_endpoint+"#' "+nsPath+"/conf/nameserver.flags"
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
}
