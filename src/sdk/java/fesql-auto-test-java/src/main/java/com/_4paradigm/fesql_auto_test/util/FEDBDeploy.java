package com._4paradigm.fesql_auto_test.util;



import com._4paradigm.fe.command.common.ExecutorUtil;
import com._4paradigm.fe.command.common.LinuxUtil;
import com._4paradigm.fesql_auto_test.common.FesqlVersionConfig;
import com._4paradigm.fesql_auto_test.entity.FEDBInfo;
import com.google.common.collect.Lists;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class FEDBDeploy {

    private String version;
    private String fedbUrl;
    private String fedbName;
    @Setter
    private String fedbPath;
    @Setter
    private boolean useName;

    public FEDBDeploy(String version){
        this.version = version;
        this.fedbUrl = FesqlVersionConfig.getUrl(version);
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
        // fedbInfo.setNsNum(ns);
        // fedbInfo.setTabletNum(tablet);
        // fedbInfo.setBasePath(testPath);
        String ip = LinuxUtil.getLocalIP();
        File file = new File(testPath);
        if(!file.exists()){
            file.mkdirs();
        }
        int zkPort = deployZK(testPath);
        // fedbInfo.setZk_cluster();
        // fedbInfo.setZk_root_path();
        downloadFEDB(testPath);
        String zk_point = ip+":"+zkPort;
        builder.zk_cluster(zk_point).zk_root_path("/fedb");
        builder.nsEndpoints(Lists.newArrayList()).nsNames(Lists.newArrayList());
        builder.tabletEndpoints(Lists.newArrayList()).tabletNames(Lists.newArrayList());
        builder.fedbPath(testPath+"/fedb-ns-1/bin/fedb");
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
        // fedbInfo.setFedbPath();
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
        log.info("fedb-info:"+fedbInfo);
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
            command = "ls " + testPath + " | grep fedb-cluster | grep -v .tar.gz";
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
                    "wget -P "+testPath+" http://pkg.4paradigm.com:81/rtidb/dev/zookeeper-3.4.10.tar.gz",
                    "tar -zxvf "+testPath+"/zookeeper-3.4.10.tar.gz -C "+testPath,
                    "cp "+testPath+"/zookeeper-3.4.10/conf/zoo_sample.cfg "+testPath+"/zookeeper-3.4.10/conf/zoo.cfg",
                    "sed -i 's#dataDir=/tmp/zookeeper#dataDir="+testPath+"/data#' "+testPath+"/zookeeper-3.4.10/conf/zoo.cfg",
                    "sed -i 's#clientPort=2181#clientPort="+port+"#' "+testPath+"/zookeeper-3.4.10/conf/zoo.cfg",
//                    "sed -i 's#tickTime=2000#tickTime=20000000#' "+testPath+"/zookeeper-3.4.10/conf/zoo.cfg",
                    "sh "+testPath+"/zookeeper-3.4.10/bin/zkServer.sh start"
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
            String ns_name = "/fedb-ns-"+index;
            List<String> commands = Lists.newArrayList(
                    "cp -r " + testPath + "/" + fedbName + " " + testPath + ns_name,
                    "sed -i 's#--zk_cluster=.*#--zk_cluster=" + zk_endpoint + "#' " + testPath + ns_name + "/conf/nameserver.flags",
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
            commands.forEach(ExecutorUtil::run);
            if(StringUtils.isNotEmpty(fedbPath)){
                FEDBCommandUtil.cpRtidb(testPath+ns_name,fedbPath);
            }
            ExecutorUtil.run("sh "+testPath+ns_name+"/bin/start_ns.sh start");
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
            String tablet_name = "/fedb-tablet-"+index;
            List<String> commands = Lists.newArrayList(
                    "cp -r "+testPath+"/"+fedbName+" "+testPath+tablet_name,
                    "sed -i 's/--zk_cluster=.*/--zk_cluster="+zk_endpoint+"/' "+testPath+tablet_name+"/conf/tablet.flags",
                   "sed -i 's@--zk_root_path=.*@--zk_root_path=/fedb@' "+testPath+tablet_name+"/conf/tablet.flags",
                    "sed -i 's@#--make_snapshot_threshold_offset=100000@--make_snapshot_threshold_offset=10@' "+testPath+tablet_name+"/conf/tablet.flags",
//                    "sed -i 's@--gc_interval=60@--gc_interval=1@' "+testPath+tablet_name+"/conf/tablet.flags",
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
            commands.forEach(ExecutorUtil::run);
            if(StringUtils.isNotEmpty(fedbPath)){
                FEDBCommandUtil.cpRtidb(testPath+tablet_name,fedbPath);
            }
            ExecutorUtil.run("sh "+testPath+tablet_name+"/bin/start.sh start");
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
    public int deployBlobServer(String testPath, String ip, int index, String zk_endpoint, String name){
        try {
            int port = LinuxUtil.getNoUsedPort();
            String blob_server_name = "/rtidb-blob-"+index;
            List<String> commands = Lists.newArrayList(
                    "cp -r "+testPath+"/"+fedbName+" "+testPath+blob_server_name,
                    "sed -i 's#--role=tablet#--role=blob#' "+testPath+blob_server_name+"/conf/tablet.flags",
                    "sed -i 's/#--zk_cluster=.*/--zk_cluster="+zk_endpoint+"/' "+testPath+blob_server_name+"/conf/tablet.flags",
                    "sed -i 's@#--zk_root_path=/rtidb_cluster@--zk_root_path=/rtidb_cluster@' "+testPath+blob_server_name+"/conf/tablet.flags"
            );
            if(useName){
                commands.add("sed -i 's/--endpoint=.*/#&/' " + testPath + blob_server_name + "/conf/tablet.flags");
                commands.add("echo '--use_name=true' >> " + testPath + blob_server_name + "/conf/tablet.flags");
                commands.add("echo '--port=" + port + "' >> " + testPath + blob_server_name + "/conf/tablet.flags");
                if(name!=null){
                    commands.add("mkdir -p " + testPath + blob_server_name + "/data");
                    commands.add("echo " + name + " >> " + testPath + blob_server_name + "/data/name.txt");
                }
            }else{
                String ip_port = ip+":"+port;
                commands.add("sed -i 's#--endpoint=.*#--endpoint="+ip_port+"#' "+testPath+blob_server_name+"/conf/tablet.flags");
            }
            commands.forEach(ExecutorUtil::run);
            if(StringUtils.isNotEmpty(fedbPath)){
                FEDBCommandUtil.cpRtidb(testPath+blob_server_name,fedbPath);
            }
            ExecutorUtil.run("sh "+testPath+blob_server_name+"/bin/start.sh start");
            boolean used = LinuxUtil.checkPortIsUsed(port,3000,30);
            if(used){
                log.info("blob-server部署成功，port："+port);
                return port;
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        throw new RuntimeException("blob-server部署失败");
    }
    public int deployBlobProxy(String testPath, String ip, int index, String zk_endpoint, String name){
        try {
            int port = LinuxUtil.getNoUsedPort();
            String blob_proxy_name = "/rtidb-blob-proxy-"+index;
            List<String> commands = Lists.newArrayList(
                    "cp -r "+testPath+"/"+fedbName+" "+testPath+blob_proxy_name,
                    "sed -i 's#--role=tablet#--role=blob_proxy#' "+testPath+blob_proxy_name+"/conf/tablet.flags",
                    "sed -i 's/#--zk_cluster=.*/--zk_cluster="+zk_endpoint+"/' "+testPath+blob_proxy_name+"/conf/tablet.flags",
                    "sed -i 's@#--zk_root_path=/rtidb_cluster@--zk_root_path=/rtidb_cluster@' "+testPath+blob_proxy_name+"/conf/tablet.flags"
            );
            if(useName){
                commands.add("sed -i 's/--endpoint=.*/#&/' " + testPath + blob_proxy_name + "/conf/tablet.flags");
                commands.add("echo '--use_name=true' >> " + testPath + blob_proxy_name + "/conf/tablet.flags");
                commands.add("echo '--port=" + port + "' >> " + testPath + blob_proxy_name + "/conf/tablet.flags");
                if(name!=null){
                    commands.add("mkdir -p " + testPath + blob_proxy_name + "/data");
                    commands.add("echo " + name + " >> " + testPath + blob_proxy_name + "/data/name.txt");
                }
            }else{
                String ip_port = ip+":"+port;
                commands.add("sed -i 's#--endpoint=.*#--endpoint="+ip_port+"#' "+testPath+blob_proxy_name+"/conf/tablet.flags");
            }
            commands.forEach(ExecutorUtil::run);
            if(StringUtils.isNotEmpty(fedbPath)){
                FEDBCommandUtil.cpRtidb(testPath+blob_proxy_name,fedbPath);
            }
            ExecutorUtil.run("sh "+testPath+blob_proxy_name+"/bin/start.sh start");
            boolean used = LinuxUtil.checkPortIsUsed(port,3000,30);
            if(used){
                log.info("blob-proxy部署成功，port："+port);
                return port;
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        throw new RuntimeException("blob-proxy部署失败");
    }
}

