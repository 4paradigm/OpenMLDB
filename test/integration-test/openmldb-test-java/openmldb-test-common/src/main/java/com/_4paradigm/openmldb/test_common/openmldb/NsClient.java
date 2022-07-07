package com._4paradigm.openmldb.test_common.openmldb;

import com._4paradigm.openmldb.test_common.command.CommandUtil;
import com._4paradigm.openmldb.test_common.util.NsCliResultUtil;
import com._4paradigm.openmldb.test_common.util.Tool;
import com._4paradigm.openmldb.test_common.util.WaitUtil;
import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

@Slf4j
public class NsClient {
    private OpenMLDBInfo openMLDBInfo;
    private String openMLDBPath;
    private String zkCluster;
    private String zkRootPath;

    private NsClient(OpenMLDBInfo openMLDBInfo){
        this.openMLDBInfo = openMLDBInfo;
        this.openMLDBPath = openMLDBInfo.getOpenMLDBPath();
        this.zkCluster = openMLDBInfo.getZk_cluster();
        this.zkRootPath = openMLDBInfo.getZk_root_path();
    }
    public static NsClient of(OpenMLDBInfo openMLDBInfo){
        return new NsClient(openMLDBInfo);
    }
    public String genNsCommand(String openMLDBPath,String zkCluster,String zkRootPath,String dbName,String command){
        String line = "%s --zk_cluster=%s --zk_root_path=%s --role=ns_client --interactive=false --database=%s --cmd='%s'";
        line = String.format(line,openMLDBPath,zkCluster,zkRootPath,dbName,command);
        log.info("ns command:"+line);
        return line;
    }
    public String genNsCommand(String dbName,String command){
        return genNsCommand(openMLDBPath,zkCluster,zkRootPath,dbName,command);
    }
    public List<String> runNs(String dbName,String command){
        String nsCommand = genNsCommand(dbName,command);
        return CommandUtil.run(nsCommand);
    }
    public boolean checkOPStatusDone(String dbName,String tableName){
        String command = StringUtils.isNotEmpty(tableName) ?"showopstatus "+tableName:"showopstatus";
        String nsCommand = genNsCommand(dbName,command);
        Tool.sleep(3*1000);
        return WaitUtil.waitCondition(()->{
            List<String> lines = CommandUtil.run(nsCommand);
            return NsCliResultUtil.checkOPStatus(lines,"kDone");
        },()->{
            List<String> lines = CommandUtil.run(nsCommand);
            return NsCliResultUtil.checkOPStatusAny(lines,"kFailed");
        });
    }



}
