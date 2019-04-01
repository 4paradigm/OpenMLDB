#!encoding=utf8
import json
import sys
import subprocess
import os

import socket

def CheckLocalHost(host):
    hostname = socket.gethostname()
    if host == hostname:
        return True
    ip = socket.gethostbyname(hostname)    
    if host == ip:
        return True
    return False

def RunWithRetunCode(command):
    try:
        p = subprocess.Popen(command, stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell = True)
        output = p.stdout.read()
        p.wait()
        errout = p.stderr.read()
        p.stdout.close()
        p.stderr.close()
        return p.returncode,output,errout
    except Exception,ex:
        print(ex)
        return -1,None,None

def GetPrefixAndSuffix(host):
    is_local = CheckLocalHost(host)
    prefix = ""
    suffix = ""
    if not is_local:
        prefix = "ssh {} \" ".format(host)
        suffix = "\""
    return (prefix, suffix, is_local)    
    
def InitEnv(tablet_conf):    
    for item in tablet_conf["address_arr"]:
        host = item["address"].split(":")[0]
        (prefix, suffix, is_local) = GetPrefixAndSuffix(host)
        cmd_arr = []
        print("Init Env on {}".format(host))
        cmd_arr.append("{} echo 'never' > /sys/kernel/mm/transparent_hugepage/enabled;"
                          "echo 'never' > /sys/kernel/mm/transparent_hugepage/defrag {}".format(prefix, suffix))
        cmd_arr.append("{} swapoff -a {}".format(prefix, suffix))
        for cmd in cmd_arr:
            (returncode,output,errout) = RunWithRetunCode(cmd)
            if returncode != 0:
                print("execute cmd[{}] failed! error msg: {}".format(cmd, errout))    
                return
            print("execute cmd[{}] success".format(cmd))

def CheckJava(host):
    (prefix, suffix, is_local) = GetPrefixAndSuffix(host)
    cmd = "{} java -version {}".format(prefix, suffix)
    (returncode,output,errout) = RunWithRetunCode(cmd)
    if returncode != 0:
        print("execute cmd[{}] failed! error msg: {}".format(cmd, errout))    
        return False
    if output.find("java: command not found") != -1 or errout.find("java: command not found") != -1:
        return False
    return True    

def DeployJava(host, path, source_file, teardown):
    (prefix, suffix, is_local) = GetPrefixAndSuffix(host)
    cmd_arr = []
    file_name = os.path.basename(source_file)
    if teardown:
        print("clear java on {}. path: {}".format(host, path))
        cmd_arr.append("{} cd {}; rm {}; rm -rf {} {}".format(prefix, path, file_name, "jdk1.8.0_121", suffix))
    else:    
        print("deploy java on {}. path: {}".format(host, path))
        cmd_arr.append("{} mkdir -p {} {}".format(prefix, path, suffix))
        if is_local:
            cmd_arr.append("cp {} {}".format(source_file, path))
        else:    
            cmd_arr.append("scp {} {}:{}".format(source_file, host, path))
        cmd_arr.append("{} cd {}; tar -zxvf {}; {}".format(prefix, path, file_name, suffix))
    for cmd in cmd_arr:
        (returncode,output,errout) = RunWithRetunCode(cmd)
        if returncode != 0:
            print("execute cmd[{}] failed! error msg: {}".format(cmd, errout))    
            return
        print("execute cmd[{}] success".format(cmd))
    

def DeployZookeeper(zk_conf, teardown):
    source_file = zk_conf["package"]
    file_name = os.path.basename(source_file)
    dir_name = "zookeeper"
    server_conf = []
    for idx in xrange(len(zk_conf["address_arr"])):
        host = zk_conf["address_arr"][idx]["address"].split(":")[0]
        cur_server_conf = "server." + str(idx + 1) + "=" + host
        if "inner_port1" in zk_conf["address_arr"][idx]:
            cur_server_conf += ":" + str(zk_conf["address_arr"][idx]["inner_port1"])
        else:
            cur_server_conf += ":" + str(zk_conf["inner_port1"])
        if "inner_port2" in zk_conf["address_arr"][idx]:
            cur_server_conf += ":" + str(zk_conf["address_arr"][idx]["inner_port2"])
        else:
            cur_server_conf += ":" + str(zk_conf["inner_port2"])
        server_conf.append("echo {} >> conf/zoo.cfg".format(cur_server_conf))
    
    for idx in xrange(len(zk_conf["address_arr"])):
        host = zk_conf["address_arr"][idx]["address"].split(":")[0]
        port = zk_conf["address_arr"][idx]["address"].split(":")[1]
        (prefix, suffix, is_local) = GetPrefixAndSuffix(host)
        real_path = ''
        if "path" in zk_conf["address_arr"][idx]:
            real_path = zk_conf["address_arr"][idx]["path"]
        else:    
            real_path = zk_conf["path"]
        java_home_str = "echo $PATH"
        if not CheckJava(host):
            DeployJava(host, real_path, zk_conf["java_package"], teardown)
            java_home_str = "PATH=$PATH:{}/{}/bin".format(real_path, "jdk1.8.0_121")
        cmd_arr = []
        work_path = real_path + "/" + dir_name
        if teardown:
            print("teardown zookeeper on {}. path: {}".format(host, real_path))
            cmd_arr.append("{} cd {}; sh bin/zkServer.sh stop {}".format(prefix, work_path, suffix))
            cmd_arr.append("{} cd {}; rm {}; rm -rf {} {}".format(prefix, real_path, file_name, dir_name, suffix))
        else:    
            print("start zookeeper on {}. path: {}".format(host, real_path))
            cmd_arr.append("{} mkdir -p {} {}".format(prefix, real_path, suffix))
            if is_local:
                cmd_arr.append("cp {} {}".format(source_file, real_path))
            else:
                cmd_arr.append("scp {} {}:{}".format(source_file, host, real_path))
            cmd_arr.append("{} cd {}; tar -zxvf {}; mv {} {}; cd {}; mv conf/zoo_sample.cfg conf/zoo.cfg {}".format(
                            prefix, real_path, file_name, file_name[:-7], dir_name, dir_name, suffix))
            cmd_arr.append("{} cd {}; sed -i 's/dataDir=.*/dataDir=\.\/data/g' conf/zoo.cfg;"
                                            "sed -i 's/clientPort=.*/clientPort={}/g' conf/zoo.cfg;"
                                            "{} {}".format(
                                            prefix, work_path, port, ";".join(server_conf), suffix))
            cmd_arr.append("{} cd {}; mkdir -p ./data; echo {} > ./data/myid {}".format(prefix, work_path, idx + 1, suffix))
            cmd_arr.append("{} cd {}; {}; sh bin/zkServer.sh start {}".format(prefix, work_path, java_home_str, suffix))
        for cmd in cmd_arr:
            (returncode,output,errout) = RunWithRetunCode(cmd)
            if returncode != 0:
                print("execute cmd[{}] failed! error msg: {}".format(cmd, errout))    
                return
            print("execute cmd[{}] success".format(cmd))
        
def GetZKCluster(conf):   
    if "zookeeper" not in conf or "address_arr" not in conf["zookeeper"]:
        return ""
    zk_cluster_arr = []
    for item in conf["zookeeper"]["address_arr"]:
        zk_cluster_arr.append(item["address"])
    return ",".join(zk_cluster_arr), conf["zk_root_path"]


def DeployNameserver(zk_cluster, zk_root_path, ns_conf, teardown):
    source_file = ns_conf["package"]
    file_name = os.path.basename(source_file)
    version = file_name.split("-")[-1][:-7]
    for item in ns_conf["address_arr"]:
        real_path = ''
        if "path" in item:
            real_path = item["path"]
        else:
            real_path = ns_conf["path"]
        host = item["address"].split(":")[0]
        (prefix, suffix, is_local) = GetPrefixAndSuffix(host)
        work_path = real_path + "/rtidb-nameserver-" + version 
        cmd_arr = []
        if teardown:
            print("teardown nameserver on {}. path: {}".format(host, real_path))
            cmd_arr.append("{} cd {}; sh bin/start_ns.sh stop {}".format(prefix, work_path, suffix))
            cmd_arr.append("{} cd {}; rm {}; rm -rf {} {}".format(prefix, real_path, file_name, "rtidb-nameserver-*", suffix))
        else:    
            print("start nameserver on {}. path: {}".format(host, real_path))
            cmd_arr.append("{} mkdir -p {} {}".format(prefix, real_path, suffix))
            if is_local:
                cmd_arr.append("cp {} {}".format(source_file, real_path))
            else:    
                cmd_arr.append("scp {} {}:{}".format(source_file, host, real_path))
            cmd_arr.append("{} cd {}; tar -zxvf {}; mv {} rtidb-nameserver-{} {}".format(prefix, real_path, file_name, file_name[:-7], version, suffix))
            cmd_arr.append("{} cd {}; sed -i 's/--endpoint=.*/--endpoint={}/g' conf/nameserver.flags;"
                                            "sed -i 's/--zk_cluster=.*/--zk_cluster={}/g' conf/nameserver.flags;"
                                            "sed -i 's/--zk_root_path=.*/--zk_root_path={}/g' conf/nameserver.flags {}".format(
                                            prefix, work_path, item["address"], zk_cluster, zk_root_path.replace("/", "\/"), suffix))
            cmd_arr.append("{} cd {}; sh bin/start_ns.sh start {}".format(prefix, work_path, suffix))
        for cmd in cmd_arr:
            (returncode,output,errout) = RunWithRetunCode(cmd)
            if returncode != 0:
                print("execute cmd[{}] failed! error msg: {}".format(cmd, errout))    
                return
            print("execute cmd[{}] success".format(cmd))

def DeployTablet(zk_cluster, zk_root_path, tablet_conf, teardown):
    source_file = tablet_conf["package"]
    file_name = os.path.basename(source_file)
    version = file_name.split("-")[-1][:-7]
    for item in tablet_conf["address_arr"]:
        real_path = ''
        if "path" in item:
            real_path = item["path"]
        else:    
            real_path = tablet_conf["path"]
        host = item["address"].split(":")[0]
        (prefix, suffix, is_local) = GetPrefixAndSuffix(host)
        work_path = real_path + "/rtidb-tablet-" + version 
        cmd_arr = []
        if teardown:
            print("teardown tablet on {}. path: {}".format(host, real_path))
            cmd_arr.append("{} cd {}; sh bin/start.sh stop {}".format(prefix, work_path, suffix))
            cmd_arr.append("{} cd {}; rm {}; rm -rf {} {}".format(prefix, real_path, file_name, "rtidb-tablet-*", suffix))
        else:
            print("start tablet on {}. path: {}".format(host, real_path))
            cmd_arr.append("{} mkdir -p {} {}".format(prefix, real_path, suffix))
            if is_local:
                cmd_arr.append("cp {} {}".format(source_file, real_path))
            else:    
                cmd_arr.append("scp {} {}:{}".format(source_file, host, real_path))
            cmd_arr.append("{} cd {}; tar -zxvf {}; mv {} rtidb-tablet-{} {}".format(prefix, real_path, file_name, file_name[:-7], version, suffix))
            cmd_arr.append("{} cd {}; sed -i 's/--endpoint=.*/--endpoint={}/g' conf/tablet.flags {}".format(prefix, work_path, item["address"], suffix))
            if zk_cluster != "":
                cmd_arr.append("{} cd {}; sed -i 's/#--zk_cluster=.*/--zk_cluster={}/g' conf/tablet.flags;"
                                            "sed -i 's/#--zk_root_path=.*/--zk_root_path={}/g' conf/tablet.flags {}".format(
                                            prefix, work_path, zk_cluster, zk_root_path.replace("/", "\/"), suffix))
            cmd_arr.append("{} cd {}; sh bin/start.sh start {}".format(prefix, work_path, suffix))
        for cmd in cmd_arr:
            (returncode,output,errout) = RunWithRetunCode(cmd)
            if returncode != 0:
                print("execute cmd[{}] failed! error msg: {}".format(cmd, errout))    
                return
            print("execute cmd[{}] success".format(cmd))

if __name__ == "__main__":
    conf_file = sys.argv[1]
    teardown = False
    if len(sys.argv) > 2 and sys.argv[2] == "teardown":
        teardown = True
    with open(conf_file, 'r') as f:
        conf = json.loads(f.read())
    if sys.argv[2] == "init_env" and "tablet" in conf:
        InitEnv(conf["tablet"])
        exit(0)

    if "zookeeper" in conf and "need_deploy" in conf["zookeeper"] and conf["zookeeper"]["need_deploy"] == True:
        DeployZookeeper(conf["zookeeper"], teardown)
       
    (zk_cluster, zk_root_path) = GetZKCluster(conf)    
    if "nameserver" in conf:    
        DeployNameserver(zk_cluster, zk_root_path, conf["nameserver"], teardown)

    if "tablet" in conf:   
        DeployTablet(zk_cluster, zk_root_path, conf["tablet"], teardown)

