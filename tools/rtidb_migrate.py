import logging
import os
import subprocess
import sys
import time
USE_SHELL = sys.platform.startswith( "win" )
from optparse import OptionParser
parser = OptionParser()

parser.add_option("--rtidb_bin_path", 
                  dest="rtidb_bin_path",
                  help="the rtidb bin path")

parser.add_option("--zk_cluster", 
                  dest="zk_cluster",
                  help="the zookeeper cluster")

parser.add_option("--zk_root_path",
                  dest="zk_root_path",
                  help="the zookeeper root path")

parser.add_option("--cmd",
                  dest="cmd",
                  help="the cmd for migrate")

parser.add_option("--endpoint",
                  dest="endpoint",
                  help="the endpoint for migrate")

(options, args) = parser.parse_args()
common_cmd =  [options.rtidb_bin_path, "--zk_cluster=" + options.zk_cluster, "--zk_root_path=" + options.zk_root_path, "--role=ns_client", "--interactive=false"]

def promot_input(msg,validate_func=None,try_times=1):
    while try_times>0:
        answer = raw_input(msg).strip()
        if validate_func and validate_func(answer):
            return answer
        try_times-=1
    return None
def promot_password_input(msg,validate_func=None,try_times=1):
    while try_times>0:
        answer = getpass.getpass(msg).strip()
        if validate_func and validate_func(answer):
            return answer
        try_times-=1
    return None

def not_none_or_empty(user_input):
    if input:
        return True
    return False

def yes_or_no_validate(user_input):
    if user_input and user_input.lower()=='y':
        return True
    return False

def yes_or_no_promot(msg):
    answer = raw_input(msg).strip()
    return yes_or_no_validate(answer)

def RunWithRealtimePrint(command,
                         universal_newlines = True,
                         useshell = USE_SHELL,
                         env = os.environ,
                         print_output = True):
    try:
        p = subprocess.Popen(command,
                              stdout = subprocess.PIPE,
                              stderr = subprocess.STDOUT,
                              shell = useshell, 
                              env = env )
        if print_output:
            for line in iter(p.stdout.readline,''):
                sys.stdout.write(line)
                sys.stdout.write('\r')
        p.wait()
        return p.returncode
    except Exception,ex:
	    print(ex)
	    return -1

def RunWithRetuncode(command,
                     universal_newlines = True,
                     useshell = USE_SHELL,
                     env = os.environ):
    try:
        p = subprocess.Popen(command, stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell = useshell, universal_newlines = universal_newlines, env = env )
        output = p.stdout.read()
        p.wait()
        errout = p.stderr.read()
        p.stdout.close()
        p.stderr.close()
        return p.returncode,output,errout
    except Exception,ex:
	    print(ex)
	    return -1,None,None

def GetTables(output):
    # name  tid  pid  endpoint  role  ttl  is_alive  compress_type
    lines = output.split("\n")
    content_is_started = False
    partition_on_tablet = {}
    for line in lines:
        if line.startswith("---------"):
            content_is_started = True
            continue
        if not content_is_started:
            continue
        partition = line.split()
        if len(partition) < 4:
            continue
        partitons = partition_on_tablet.get(partition[3], [])
        partitons.append(partition)
        partition_on_tablet[partition[3]] = partitons
    return partition_on_tablet

def GetTablesStatus(output):
    # tid  pid  offset  mode state enable_expire ttl ttl_offset memused compress_type skiplist_height
    lines = output.split("\n")
    content_is_started = False
    partition_on_tablet = {}
    for line in lines:
        if line.startswith("---------"):
            content_is_started = True
            continue
        if not content_is_started:
            continue
        partition = line.split()
        if len(partition) < 4:
            continue

        key = "{}_{}".format(partition[0], partition[1])
        partition_on_tablet[key] = partition
    return partition_on_tablet

def Analysis():
    # show table
    show_table = [options.rtidb_bin_path, "--zk_cluster=" + options.zk_cluster, "--zk_root_path=" + options.zk_root_path,
        "--role=ns_client", "--interactive=false", "--cmd=showtable"]
    code, stdout,stderr = RunWithRetuncode(show_table)
    if code != 0:
        print "fail to show table"
        return
    partitions = GetTables(stdout)
    leader_partitions = []
    for p in partitions[options.endpoint]:
        if p[4] == "leader":
            leader_partitions.append(p)
    if not leader_partitions:
        print "you can restart the tablet directly"
        return
    print "the following cmd in ns should be executed for migrating the node"
    for p in leader_partitions:
        print ">changeleader %s %s auto"%(p[0], p[2])
        print "the current leader and follower offset"
        GetLeaderFollowerOffset(p[3], p[1], p[2])
    print "use the following cmd in tablet to make sure the changeleader is done"
    print ">getablestatus"

def GetLeaderFollowerOffset(endpoint, tid, pid):
    command = [options.rtidb_bin_path, "--endpoint=%s"%endpoint, "--role=client", "--interactive=false", "--cmd=getfollower %s %s"%(tid, pid)]
    code, stdout,stderr = RunWithRetuncode(command)
    if code != 0:
        print "fail to getfollower"
        return
    print stdout

def ChangeLeader():
        # show tablet
    show_tablet = list(common_cmd)
    show_tablet.append("--cmd=showtablet")
    _,stdout,_ = RunWithRetuncode(show_tablet)
    print stdout
    # show table
    show_table = list(common_cmd)
    show_table.append("--cmd=showtable")
    code, stdout,stderr = RunWithRetuncode(show_table)
    if code != 0:
        print "fail to show table"
        return
    partitions = GetTables(stdout)
    leader_partitions = []
    for p in partitions[options.endpoint]:
        if p[4] == "leader":
            leader_partitions.append(p)

    if not leader_partitions:
        print "you can restart the tablet directly"
        return

    print "start to change leader on %s"%options.endpoint
    for p in leader_partitions:
        print "the current leader and follower offset"
        GetLeaderFollowerOffset(p[3], p[1], p[2])
        changeleader = list(common_cmd)
        changeleader.append("--cmd=changeleader %s %s auto"%(p[0], p[2]))
        msg = "command:%s \nwill be excute, sure to change leader(y/n):"%(" ".join(changeleader))
        yes = yes_or_no_promot(msg)
        if yes:
            code, stdout, stderr = RunWithRetuncode(changeleader)
            if code != 0:
                print "fail to change leader for %s %s"%(p[0], p[2])
                print stdout
                print stderr
            else:
                print stdout
        else:
            print "skip to change leader for %s %s"%(p[0], p[2])

def RecoverEndpoint():
    # show table
    show_table = list(common_cmd)
    show_table.append("--cmd=showtable")
    code, stdout,stderr = RunWithRetuncode(show_table)
    if code != 0:
        print "fail to show table"
        return
    partitions = GetTables(stdout)
    not_alive_partitions = []
    for p in partitions[options.endpoint]:
        # follower status no
        # leader status no
        if  p[6] == "no":
            not_alive_partitions.append(p)
    if not not_alive_partitions:
        print "no need recover not alive partition"
        return
    print "start to recover partiton on %s"%options.endpoint
    for p in not_alive_partitions:
        print "not a alive partition information"
        print " ".join(p)
        recover_cmd = list(common_cmd)
        recover_cmd.append("--cmd=recovertable %s %s %s"%(p[0], p[2], options.endpoint))
        msg = "command:%s \nwill be excute, sure to recover endpoint(y/n):"%(" ".join(recover_cmd))
        yes = yes_or_no_promot(msg)
        if yes:
            code, stdout, stderr = RunWithRetuncode(recover_cmd)
            if code != 0:
                print "fail to recover partiton for %s %s on %s"%(p[0], p[2], options.endpoint)
                print stdout
                print stderr
            else:
                print stdout
        else:
            print "skip to recover partiton for %s %s on %s"%(p[0], p[2], options.endpoint)

def RecoverData():
    # show table
    show_table = list(common_cmd)
    show_table.append("--cmd=showtable")
    code, stdout,stderr = RunWithRetuncode(show_table)
    if code != 0:
        print "fail to show table"
        return

    # check whether table partition is not exixted
    partitions = GetTables(stdout)
    # print partitions
    tablet_cmd = [options.rtidb_bin_path, "--role=client",  "--interactive=false"]
    for endpoint in partitions:
        cmd_gettablestatus = "--cmd=gettablestatus"
        gettablestatus = list(tablet_cmd)
        gettablestatus.append("--endpoint=" + endpoint)
        gettablestatus.append(cmd_gettablestatus)
        code, stdout,stderr = RunWithRetuncode(gettablestatus)
        table_status = GetTablesStatus(stdout)
        if len(table_status) == 0:
            continue
        else:
            print "endpoint[{}] is alive".format(endpoint)
            return

    conget_auto = list(common_cmd)
    conget_auto.append("--cmd=confget auto_failover")
    code, stdout,stderr = RunWithRetuncode(conget_auto)
    auto_failover_flag = stdout.find("true")
    if auto_failover_flag != -1:
        # set auto failove is no
        confset_no = list(common_cmd)
        confset_no.append("--cmd=confset auto_failover false")
        code, stdout,stderr = RunWithRetuncode(confset_no)
        # print stdout
        if code != 0:
            print "set auto_failover is failed"
            return
        print "confset auto_failover false"

    # updatetablealive $TABLE 1 172.27.128.37:9797 yes
    # ./build/bin/rtidb --cmd="updatetablealive $TABLE 1 172.27.128.37:9797 yes" --role=ns_client --endpoint=172.27.128.37:6527 --interactive=false
    # updatetablealive all of tables no
    leader_table = {}
    follower_table = []
    for key in partitions:
        tables = partitions[key]
        for p in tables:
            cmd_no = "--cmd=updatetablealive " + p[0] + " " + p[2] + " " + p[3] + " no"
            update_alive_no = list(common_cmd)
            update_alive_no.append(cmd_no)
            code, stdout,stderr = RunWithRetuncode(update_alive_no)
            if stdout.find("update ok") == -1:
                print stdout
                print "update table alive is failed"
                return

            # dont use code to determine result
            if p[4] == "leader":
                key = "{}_{}".format(p[1], p[2])
                if leader_table.has_key(key):
                    tmp = leader_table[key]
                    if (tmp[8] < p[8]):
                        leader_table[key] = p
                        follower_table.append(tmp)
                    else:
                        follower_table.append(p)
                else:
                    leader_table[key] = p
            else:
                follower_table.append(p)
            print "updatetablealive tid[{}] pid[{}] endpoint[{}] no".format(p[1], p[2], p[3])

    # ./build/bin/rtidb --cmd="loadtable $TABLE $TID $PID 144000 3 true" --role=client --endpoint=$TABLET_ENDPOINT --interactive=false
    for key in leader_table:
        # print key
        table = leader_table[key]
        print "table leader: {}".format(table)
        cmd_loadtable = "--cmd=loadtable " + table[0] + " " + table[1] + " " + table[2] + " " + table[5].split("min")[0] + " 8"
        # print cmd_loadtable
        loadtable = list(tablet_cmd)
        loadtable.append(cmd_loadtable)
        loadtable.append("--endpoint=" + table[3])
        # print loadtable
        code, stdout,stderr = RunWithRetuncode(loadtable)
        if stdout.find("LoadTable ok") == -1:
            print stdout
            print "load table is failed"
            return
        print "loadtable tid[{}] pid[{}]".format(table[1], table[2])

    # check table status
    count = 0
    while True:
        flag = True
        if count % 12 == 0:
            print "loop check NO.{}".format(count)
        for key in leader_table:
            table = leader_table[key]
            cmd_gettablestatus = "--cmd=gettablestatus"
            gettablestatus = list(tablet_cmd)
            gettablestatus.append("--endpoint=" + table[3])
            gettablestatus.append(cmd_gettablestatus)
            code, stdout,stderr = RunWithRetuncode(gettablestatus)

            table_status = GetTablesStatus(stdout)
            status = table_status[key]
            if status[3] == "kTableLeader":
                if count % 12 == 0:
                    print "{} status: {}".format(key, status[4])
                if status[4] != "kTableNormal":
                    flag = False
                else:
                    # update table is alive
                    cmd_yes = "--cmd=updatetablealive " + table[0] + " " + table[2] + " " + table[3] + " yes"
                    update_alive_yes = list(common_cmd)
                    update_alive_yes.append(cmd_yes)
                    code, stdout,stderr = RunWithRetuncode(update_alive_yes)
                    if stdout.find("update ok") == -1:
                        print stdout
                        print "update table alive is failed"
                        return
                        break

        if flag == True:
            print "Load table is ok"
            break

        if count % 12 == 0:
            print "loading table, please wait a moment"
        count = count + 1
        time.sleep(5)

    # recovertable table_name pid endpoint
    for table in follower_table:
        # print table
        cmd_recovertable = "--cmd=recovertable " + table[0] + " " + table[2] + " " + table[3]
        recovertable = list(common_cmd)
        recovertable.append(cmd_recovertable)
        code, stdout,stderr = RunWithRetuncode(recovertable)
        if stdout.find("recover table ok") == -1:
            print stdout
            print "recover is failed"
            return
        print "recovertable tid[{}] pid[{}] endpoint[{}]".format(table[1], table[2], table[3])
        # print stdout

    if auto_failover_flag != -1:
        # set auto failove is no
        confset_no = list(common_cmd)
        confset_no.append("--cmd=confset auto_failover true")
        code, stdout,stderr = RunWithRetuncode(confset_no)
        # print stdout
        if code != 0:
            print "set auto_failover true is failed"
            return
        print "confset auto_failover true"


def Main():
    if options.cmd == "analysis":
        Analysis()
    elif options.cmd == "changeleader":
        ChangeLeader()
    elif options.cmd == "recovertable":
        RecoverEndpoint()
    elif options.cmd == "recoverdata":
        RecoverData()

if __name__ == "__main__":
    Main()





