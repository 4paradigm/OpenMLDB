package com._4paradigm.openmldb.test_common.openmldb;

import com._4paradigm.openmldb.test_common.bean.OpenMLDBResult;
import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBInfo;
import com._4paradigm.qa.openmldb_deploy.util.Tool;
import com._4paradigm.test_tool.command_tool.common.ExecutorUtil;
import org.testng.Assert;

import java.util.List;
import java.util.stream.Collectors;

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
            OpenMLDBResult openMLDBResult = sdkClient.execute("show components;");
            List<Object> list = openMLDBResult.getResult().stream().map(l -> l.get(0)).collect(Collectors.toList());
            Assert.assertTrue(!list.contains(nsEndpoint),"ns stop, show components failed.");
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
}
