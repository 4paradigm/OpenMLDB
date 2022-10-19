package com._4paradigm.openmldb.test_common.openmldb;

import com._4paradigm.openmldb.test_common.command.CommandUtil;
import com._4paradigm.openmldb.test_common.util.NsResultUtil;
import com._4paradigm.openmldb.test_common.util.Tool;
import com._4paradigm.openmldb.test_common.util.WaitUtil;
import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;

import java.util.*;

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
        String dbStr = StringUtils.isNotEmpty(dbName)?"--database="+dbName:"";
        String line = "%s --zk_cluster=%s --zk_root_path=%s --role=ns_client --interactive=false %s --cmd='%s'";
        line = String.format(line,openMLDBPath,zkCluster,zkRootPath,dbStr,command);
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
    public void checkOPStatusDone(String dbName,String tableName){
        String command = StringUtils.isNotEmpty(tableName) ?"showopstatus "+tableName:"showopstatus";
        String nsCommand = genNsCommand(dbName,command);
        Tool.sleep(3*1000);
        boolean b = WaitUtil.waitCondition(()->{
            List<String> lines = CommandUtil.run(nsCommand);
            if(lines.size()<=2){
                return false;
            }
            return NsResultUtil.checkOPStatus(lines,"kDone");
        },()->{
            List<String> lines = CommandUtil.run(nsCommand);
            return NsResultUtil.checkOPStatusAny(lines,"kFailed");
        });
        Assert.assertTrue(b,"check op done failed.");
    }
    public List<String> showTableHaveTable(String dbName,String tableName){
        String command = StringUtils.isNotEmpty(tableName) ?"showtable "+tableName:"showtable";
        String nsCommand = genNsCommand(dbName,command);
        Tool.sleep(10*1000);
        List<String> result = WaitUtil.waitCondition(() -> {
            List<String> lines = CommandUtil.run(nsCommand);
            if (lines.size() <= 2) {
                return Pair.of(false, lines);
            }
            return Pair.of(true, lines);
        });
        return result;
    }
    public List<String> showTable(String dbName,String tableName){
        String command = StringUtils.isNotEmpty(tableName) ?"showtable "+tableName:"showtable";
        List<String> lines = runNs(dbName,command);
        return lines;
    }
    public long getTableCount(String dbName, String tableName){
        List<String> lines = showTableHaveTable(dbName,tableName);
        long count = 0;
        for(int i=2;i<lines.size();i++) {
            String[] infos = lines.get(i).split("\\s+");
            String role = infos[4];
            long offset = 0;
            String offsetStr = infos[7].trim();
            if (!offsetStr.equals("-") && !offsetStr.equals("")) {
                offset = Long.parseLong(offsetStr);
            }
            if (role.equals("leader")) {
                count += offset;
            }
        }
        return count;
    }

    public void checkTableIsAlive(String dbName,String tableName){
        List<String> lines = showTable(dbName,tableName);
        Assert.assertTrue(lines.size()>2,"show table lines <= 2");
        for(int i=2;i<lines.size();i++){
            String line = lines.get(i);
            String[] infos = line.split("\\s+");
            Assert.assertEquals(infos[5],"yes");
        }
    }
    public void checkTableOffSet(String dbName,String tableName){
        List<String> lines = showTable(dbName,tableName);
        Assert.assertTrue(lines.size()>2,"show table lines <= 2");
        Map<String,List<Long>> table1 = NsResultUtil.getTableOffset(lines);
        for(List<Long> values:table1.values()){
            for(Long offset:values){
                Assert.assertEquals(offset,values.get(0));
            }
        }
    }
    public void makeSnapshot(String dbName,String tableName,int pid){
        String command = String.format("makesnapshot %s %d",tableName,pid);
        List<String> lines = runNs(dbName,command);
        Assert.assertEquals(lines.get(0),"MakeSnapshot ok");
        Tool.sleep(3*1000);
        checkTableOffSet(dbName,tableName);
        checkOPStatusDone(dbName,tableName);
    }
    public void makeSnapshot(String dbName,String tableName){
        List<Integer> pidList = getPid(dbName,tableName);
        for(Integer pid:pidList) {
            makeSnapshot(dbName,tableName,pid);
        }
    }
    public List<Integer> getPid(String dbName,String tableName){
        Map<String, Set<Integer>> pidMap = getPid(dbName);
        Set<Integer> value = pidMap.get(tableName);
        return new ArrayList<>(value);
    }
    public Map<String,Set<Integer>> getPid(String dbName){
        List<String> lines = showTable(dbName,null);
        Map<String,Set<Integer>> map = new HashMap<>();
        for(int i=2;i<lines.size();i++){
            String line = lines.get(i);
            String[] infos = line.split("\\s+");
            String key = infos[0];
            int pid = Integer.parseInt(infos[2]);
            Set<Integer> values = map.get(key);
            if (values==null) {
                values = new HashSet<>();
            }
            values.add(pid);
            map.put(key,values);
        }
        return map;
    }

    public void confset(String key,String value){
        String command = String.format("confset %s %s",key,value);
        List<String> lines = runNs(null,command);
        Assert.assertTrue(lines.get(0).contains("ok"));
    }
    public void migrate(String dbName,String tableName,String desEndpoint){
        Map<Integer, List<String>> tableEndPointMap = getTableEndPoint(dbName, tableName);
        for(int pid:tableEndPointMap.keySet()){
            List<String> srcEndpointList = tableEndPointMap.get(pid);
            if(srcEndpointList.size()<=1){
                throw new IllegalStateException("only have leader not migrate");
            }
            int index = new Random().nextInt(srcEndpointList.size()-1)+1;
            String srcEndpoint = srcEndpointList.get(index);
            migrate(dbName,srcEndpoint,tableName,pid,desEndpoint);
        }
    }
    public void migrate(String dbName,String srcEndpoint,String tableName,int pid,String desEndpoint){
        String command = String.format("migrate %s %s %s %s",srcEndpoint,tableName,pid,desEndpoint);
        List<String> lines = runNs(dbName,command);
        Assert.assertEquals(lines.get(0),"partition migrate ok");
        Tool.sleep(3*1000);
        checkOPStatusDone(dbName,tableName);
        List<String> desEndpointList = getTableEndPoint(dbName, tableName, pid);
        Assert.assertTrue(desEndpointList.contains(desEndpoint),"migrate check endpoint failed.");
        checkTableOffSet(dbName,tableName);
    }
    public List<String> getTableEndPoint(String dbName,String tableName,int pid){
        Map<Integer, List<String>> tableEndPointMap = getTableEndPoint(dbName, tableName);
        return tableEndPointMap.get(pid);
    }
    public Map<Integer,List<String>> getTableEndPoint(String dbName,String tableName){
        Map<Integer,List<String>> map = new HashMap<>();
        List<String> lines = showTable(dbName,tableName);
        Assert.assertTrue(lines.size()>2,"show table lines <= 2");
        for(int i=2;i<lines.size();i++){
            String line = lines.get(i);
            String[] infos = line.split("\\s+");
            int pid = Integer.parseInt(infos[2]);
            String endpoint = infos[3];
            String role = infos[4];
            List<String> values = map.get(pid);
            if(values == null){
                values = new ArrayList<>();
            }
            if(role.equals("leader")){
                values.add(0,endpoint);
            }else {
                values.add(endpoint);
            }
            map.put(pid,values);
        }
        return map;
    }

    public Map<String,List<Long>> getTableOffset(String dbName){
        List<String> lines = showTableHaveTable(dbName,null);
        Map<String,List<Long>> offsets = new HashMap<>();
        for(int i=2;i<lines.size();i++){
            String[] infos = lines.get(i).split("\\s+");
            String key = infos[0]+"_"+infos[2];
            List<Long> value = offsets.get(key);
            String role = infos[4];
            long offset = 0;
            String offsetStr = infos[7].trim();
            if(!offsetStr.equals("-")&&!offsetStr.equals("")){
                offset = Long.parseLong(offsetStr);
            }
            if(value==null){
                value = new ArrayList<>();
                offsets.put(key,value);
            }
            if(role.equals("leader")){
                value.add(0,offset);
            }else {
                value.add(offset);
            }
        }
        return offsets;
    }

}
