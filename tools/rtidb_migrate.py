import logging
import os
import subprocess
import sys
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
common_cmd =  [options.rtidb_bin_path, "--zk_cluster=" + options.zk_cluster, "--zk_root_path=" + options.zk_root_path,
               "--role=ns_client", "--interactive=false"]

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
	self.logger.exception(ex)
	return -1

def RunWithRetuncode(command,
                     universal_newlines = True,
                     useshell = USE_SHELL,
                     env = os.environ):
    try:
       p = subprocess.Popen(command,
			      stdout = subprocess.PIPE,
			      stderr = subprocess.PIPE,
			      shell = useshell, 
			      universal_newlines = universal_newlines,
			      env = env )
        output = p.stdout.read()
        p.wait()
        errout = p.stderr.read()
        p.stdout.close()
        p.stderr.close()
        return p.returncode,output,errout
    except Exception,ex:
	self.logger.exception(ex)
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
        if p[4] == "follower" and p[6] == "no":
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


def Main():
    if options.cmd == "analysis":
        Analysis()
    elif options.cmd == "changeleader":
        ChangeLeader()
    elif options.cmd == "recovertable":
        RecoverEndpoint()

if __name__ == "__main__":
    Main()





