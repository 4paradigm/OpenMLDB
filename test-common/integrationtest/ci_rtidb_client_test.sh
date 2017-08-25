#!/bin/bash
set +x
testpath=`pwd`
projectpath=`echo ${testpath}|awk -F '/test-common' '{print $1}'`
rtidbpath=${projectpath}/build/bin/rtidb
confpath=${projectpath}/release/conf/rtidb.flags
leaderpath=${testpath}/leader
slave1path=${testpath}/slave1
slave2path=${testpath}/slave2

leader="0.0.0.0:17770"
slave1="0.0.0.0:17771"
slave2="0.0.0.0:17772"
tid=0
result=1
failureindex=0

function setup() {
    cp -f ${rtidbpath} ${testpath}

    mkdir -p ${leaderpath}/conf
    cat ${confpath} | grep -v "endpoint=" > ${leaderpath}/conf/rtidb.flags
    sed -i '1a --endpoint='${leader} ${leaderpath}/conf/rtidb.flags
    cp -f ${testpath}/boot.sh ${leaderpath}/conf
    cp -f ${testpath}/start.sh ${leaderpath}/conf

    mkdir -p ${slave1path}/conf
    cat ${confpath} | grep -v "endpoint=" > ${slave1path}/conf/rtidb.flags
    sed -i '1a --endpoint='${slave1} ${slave1path}/conf/rtidb.flags
    cp -f ${testpath}/boot.sh ${slave1path}/conf
    cp -f ${testpath}/start.sh ${slave1path}/conf

    mkdir -p ${slave2path}/conf
    cat ${confpath} | grep -v "endpoint=" > ${slave2path}/conf/rtidb.flags
    sed -i '1a --endpoint='${slave2} ${slave2path}/conf/rtidb.flags
    cp -f ${testpath}/boot.sh ${slave2path}/conf
    cp -f ${testpath}/start.sh ${slave2path}/conf
}

function run_client() {
	rs=$(${rtidbpath} --endpoint=$1 --role=client --interactive=false --cmd="$2"|grep "$3"|grep -v "grep");
	if [ -z "${rs}" ];then rs="[================执行失败!!!!!!!]" && result=0;fi
	echo -e "[$1] 执行: "$2"\n返回: "${rs}
}

function deal_result() {
	if [ "${result}" == 0 ];then
	    failures[failureindex]=$1
	fi
	failureindex=$((${failureindex}+1))
	result=1
}

function stop_client() {
    port=`echo $1|awk -F ':' '{print $2}'`
    echo $port
    lsof -i:${port}|grep LISTEN|awk '{print $2}'
	lsof -i:`echo $1|awk -F ':' '{print $2}'`|grep LISTEN|awk '{print $2}'|xargs kill -9
}

function stop_all_clients() {
	ps xf | grep rtidb_mon | grep -v grep | awk '{print $1}' |xargs kill -9
	lsof -i:`echo ${leader}|awk -F ':' '{print $2}'`|grep -v PID|awk '{print $2}'|xargs kill -9
	lsof -i:`echo ${slave1}|awk -F ':' '{print $2}'`|grep -v PID|awk '{print $2}'|xargs kill -9
	lsof -i:`echo ${slave2}|awk -F ':' '{print $2}'`|grep -v PID|awk '{print $2}'|xargs kill -9
	rm -rf ${leaderpath}/snapshots/* ${slave1path}/snapshots/* ${slave2path}/snapshots/*
	rm -rf ${leaderpath}/binlogs/* ${slave1path}/binlogs/* ${slave2path}/binlogs/*
}

function start_client() {
    cd $1 && ../mon ./conf/boot.sh -d -l ./rtidb_mon.log
}

################# gettablestatus #################

function test_gettablestatus_all() {
	echo -e "\n查看所有表状态:"
	tid=$((${tid}+1))
	run_client ${leader} "create t1 ${tid} 1 0 true ${slave1} ${slave2}" "ok"
	run_client ${leader} "gettablestatus" "kTableNormal"
	run_client ${leader} "drop ${tid} 1" "ok"
	deal_result $FUNCNAME
}

function test_gettablestatus_tid_pid() {
	echo -e "\n查看指定表状态:"
	tid=$((${tid}+1))
	run_client ${leader} "create t1 ${tid} 1 0 true ${slave1} ${slave2}" "ok"
	run_client ${leader} "gettablestatus ${tid} 1" "kTableLeader"
	run_client ${leader} "drop ${tid} 1" "ok"
	deal_result $FUNCNAME
}

################# create #################

function test_create_table(){
	echo -e "\n1主2从，可以建表成功:"
	tid=$((${tid}+1))
	run_client ${leader} "create t1 ${tid} 1 0 true ${slave1} ${slave2}" "ok"
	run_client ${slave1} "create t1 ${tid} 1 0 false ${slave1} ${slave2}" "ok"
	run_client ${slave2} "create t1 ${tid} 1 0 false ${slave1} ${slave2}" "ok"
	run_client ${leader} "gettablestatus ${tid} 1" "kTableNormal"
	run_client ${slave1} "gettablestatus ${tid} 1" "kTableNormal"
	run_client ${slave2} "gettablestatus ${tid} 1" "kTableNormal"
	run_client ${leader} "drop ${tid} 1" "ok"
	run_client ${slave1} "drop ${tid} 1" "ok"
	run_client ${slave2} "drop ${tid} 1" "ok"
	deal_result $FUNCNAME
}


################# put #################

function test_put_slave_sync() {
	echo -e "\n1主2从，put到leader后，slave同步成功:"
	tid=$((${tid}+1))
	run_client ${leader} "create t1 ${tid} 1 0 true ${slave1} ${slave2}" "ok"
	run_client ${slave1} "create t1 ${tid} 1 0 false ${slave1} ${slave2}" "ok"
	run_client ${slave2} "create t1 ${tid} 1 0 false ${slave1} ${slave2}" "ok"
	run_client ${leader} "put ${tid} 1 naysa 1555555555555 naysa" "ok"
	run_client ${slave1} "scan ${tid} 1 naysa 1555555555555 1" "naysa"
	run_client ${slave2} "scan ${tid} 1 naysa 1555555555555 1" "naysa"
	run_client ${leader} "drop ${tid} 1" "ok"
	run_client ${slave1} "drop ${tid} 1" "ok"
	run_client ${slave2} "drop ${tid} 1" "ok"
	deal_result $FUNCNAME
}

function test_put_slave_cannot_put() {
	echo -e "\nslave不允许put，会提示Put failed:"
	tid=$((${tid}+1))
	run_client ${slave1} "create t1 ${tid} 1 0 false ${slave1}" "ok"
	run_client ${slave1} "put ${tid} 1 naysa 1555555555555 naysa" "Put failed"
	run_client ${slave1} "drop ${tid} 1" "ok"
	deal_result $FUNCNAME
}

################## pausesnapshot ##################

function test_pausesnapshot_leader() {
	echo -e "\n暂停主节点指定表的snapshot:"
	tid=$((${tid}+1))
	run_client ${leader} "create t1 ${tid} 1 0 true ${slave1}" "ok"
	run_client ${leader} "pausesnapshot ${tid} 1" "ok"
	sleep 1
	run_client ${leader} "gettablestatus ${tid} 1" "kTablePaused"
	run_client ${leader} "drop ${tid} 1" "ok"
	deal_result $FUNCNAME
}

function test_pausesnapshot_slave_cannot_pause() {
	echo -e "\n从节点不能暂停snapshot:"
	tid=$((${tid}+1))
	run_client ${slave1} "create t1 ${tid} 1 0 false ${slave1}" "ok"
	run_client ${slave1} "pausesnapshot ${tid} 1" "Fail"
	sleep 1
	run_client ${slave1} "gettablestatus ${tid} 1" "kTableNormal"
	run_client ${slave1} "drop ${tid} 1" "ok"
	deal_result $FUNCNAME
}

function test_pausesnapshot_leader_can_put_can_be_synchronized() {
	echo -e "\n暂停主节点指定表的snapshot，仍可以put数据且被同步:"
	tid=$((${tid}+1))
	run_client ${leader} "create t1 ${tid} 1 0 true ${slave1} ${slave2}" "ok"
	run_client ${slave1} "create t1 ${tid} 1 0 false ${slave1}" "ok"
	run_client ${leader} "pausesnapshot ${tid} 1" "ok"
	sleep 1
	run_client ${leader} "gettablestatus ${tid} 1" "kTablePaused"
	run_client ${leader} "put ${tid} 1 naysa 1555555555555 naysa" "ok"
	run_client ${leader} "scan ${tid} 1 naysa 1555555555555 1" "naysa"
	run_client ${slave1} "scan ${tid} 1 naysa 1555555555555 1" ""
	run_client ${leader} "drop ${tid} 1" "ok"
	deal_result $FUNCNAME
}

################## loadsnapshot ##################

function test_loadsnapshot() {
	echo -e "\nloadsnapshot，不loadtable，无法查看表状态"
	tid=$((${tid}+1))
	run_client ${slave1} "loadsnapshot ${tid} 1" "ok"
    run_client ${slave1} "gettablestatus ${tid} 1" "gettablestatus failed"
	deal_result $FUNCNAME
}

################## loadtable ##################

function test_loadtable() {
	echo -e "\nloadsnapshot后loadtable，表状态为normal"
	tid=$((${tid}+1))
	run_client ${slave1} "loadsnapshot ${tid} 1" "ok"
	run_client ${slave1} "loadtable t1 ${tid} 1 0" "ok"
	sleep 1
    run_client ${slave1} "gettablestatus ${tid} 1" "kTableNormal"
	deal_result $FUNCNAME
}

function test_loadtable_leader() {
	echo -e "\n主节点loadtable"
	tid=$((${tid}+1))
	run_client ${leader} "loadsnapshot ${tid} 1" "ok"
	run_client ${leader} "loadtable t1 ${tid} 1 0 true ${slave1}" "ok"
	sleep 1
    run_client ${leader} "gettablestatus ${tid} 1" "kTableLeader"
	deal_result $FUNCNAME
}

function test_loadtable_slave() {
	echo -e "\n从节点loadtable"
	tid=$((${tid}+1))
	run_client ${leader} "create t1 ${tid} 1 0 true ${slave1} ${slave2}" "ok"
	run_client ${slave1} "loadsnapshot ${tid} 1" "ok"
	run_client ${slave1} "loadtable t1 ${tid} 1 0 false ${slave1}" "ok"
	sleep 1
    run_client ${slave1} "gettablestatus ${tid} 1" "kTableFollower"
	deal_result $FUNCNAME
}

function test_loadtable_without_snapshot() {
	echo -e "\n从节点loadtable"
	tid=$((${tid}+1))
	run_client ${leader} "create t1 ${tid} 1 0 true ${slave1} ${slave2}" "ok"
	run_client ${slave1} "loadtable t1 ${tid} 1 0 false ${slave1}" "Fail to LoadTable"
	deal_result $FUNCNAME
}

function test_loadtable_after_drop() {
	echo -e "\n从节点drop表之后重新loadtable，仍然可以同步主节点数据"
	tid=$((${tid}+1))
	run_client ${leader} "create t1 ${tid} 1 0 true ${slave1} ${slave2}" "ok"
	run_client ${slave1} "create t1 ${tid} 1 0 false ${slave1} ${slave2}" "ok"
	run_client ${slave1} "drop ${tid} 1" "ok"
    run_client ${slave1} "loadsnapshot ${tid} 1" "ok"
	run_client ${slave1} "loadtable t1 ${tid} 1 0 false ${slave1}" "ok"
    run_client ${leader} "put ${tid} 1 naysa 1555555555555 naysa" "ok"
    run_client ${slave1} "scan ${tid} 1 naysa 1555555555555 1" "155555555555"
	deal_result $FUNCNAME
}

function test_loadtable_after_sync() {
	echo -e "\n从节点drop表之后重新loadtable，仍然可以获取之前同步的数据"
	tid=$((${tid}+1))
	run_client ${leader} "create t1 ${tid} 1 0 true ${slave1} ${slave2}" "ok"
	run_client ${slave1} "create t1 ${tid} 1 0 false ${slave1} ${slave2}" "ok"
    run_client ${leader} "put ${tid} 1 naysa 1555555555555 naysa" "ok"
	run_client ${slave1} "drop ${tid} 1" "ok"
    run_client ${slave1} "loadsnapshot ${tid} 1" "ok"
	run_client ${slave1} "loadtable t1 ${tid} 1 0 false ${slave1}" "ok"
    run_client ${slave1} "scan ${tid} 1 naysa 1555555555555 1" "155555555555"
	deal_result $FUNCNAME
}

################## changerole ##################

function test_changerole_to_leader() {
	echo -e "\n切换从节点为主节点后，mode变为Leader"
	tid=$((${tid}+1))
	run_client ${slave1} "create t1 ${tid} 1 0 false ${slave1}" "ok"
	run_client ${slave1} "gettablestatus ${tid} 1" "kTableFollower"
	run_client ${slave1} "changerole ${tid} 1 leader" "ok"
	run_client ${slave1} "gettablestatus ${tid} 1" "kTableLeader"
	run_client ${slave1} "drop ${tid} 1" "ok"
	deal_result $FUNCNAME
}

function test_changerole_to_leader_can_put() {
	echo -e "\n切换从节点为主节点后，可以成功put数据"
	tid=$((${tid}+1))
	run_client ${slave1} "create t1 ${tid} 1 0 false ${slave1}" "ok"
	run_client ${slave1} "put ${tid} 1 naysa 1555555555555 naysa" "failed"
	run_client ${slave1} "changerole ${tid} 1 leader" "ok"
	run_client ${slave1} "put ${tid} 1 naysa 1555555555555 naysa" "ok"
	run_client ${slave1} "drop ${tid} 1" "ok"
	deal_result $FUNCNAME
}

function test_changerole_to_leader_can_pausesnapshot() {
	echo -e "\n切换从节点为主节点后，可以成功pausesnapshot"
	tid=$((${tid}+1))
	run_client ${slave1} "create t1 ${tid} 1 0 false ${slave1}" "ok"
	run_client ${slave1} "put ${tid} 1 naysa 1555555555555 naysa" "failed"
	run_client ${slave1} "changerole ${tid} 1 leader" "ok"
	run_client ${slave1} "pausesnapshot ${tid} 1" "ok"
	run_client ${slave1} "gettablestatus ${tid} 1" "kTableLeader"
	run_client ${slave1} "drop ${tid} 1" "ok"
	deal_result $FUNCNAME
}

function test_changerole_to_leader_can_addreplica() {
	echo -e "\n切换从节点为主节点后，可以成功addreplica slave，slave可以同步leader数据"
	tid=$((${tid}+1))
	run_client ${slave1} "create t1 ${tid} 1 0 false ${slave1}" "ok"
	run_client ${slave1} "changerole ${tid} 1 leader" "ok"
	run_client ${slave1} "gettablestatus ${tid} 1" "kTableLeader"
	run_client ${slave1} "pausesnapshot ${tid} 1" "ok"
    sleep 1
	run_client ${slave1} "addreplica ${tid} 1 ${slave2}" "ok"
	run_client ${slave2} "loadsnapshot ${tid} 1" "ok"
	run_client ${slave2} "loadtable t1 ${tid} 1 0 false ${slave2}" "ok"
	sleep 1
	run_client ${slave1} "put ${tid} 1 naysa 1555555555554 naysa" "ok"
	run_client ${slave2} "scan ${tid} 1 naysa 1555555555554 1" "1555555555554"
	run_client ${slave1} "drop ${tid} 1" "ok"
	deal_result $FUNCNAME
}

function test_changerole_to_leader_can_be_synchronized() {
	echo -e "\n切换从节点为主节点后，addreplica slave，可以被slave同步数据"
	tid=$((${tid}+1))
	run_client ${slave1} "create t1 ${tid} 1 0 false ${slave1} ${slave2}" "ok"
	run_client ${slave2} "create t1 ${tid} 1 0 false ${slave1} ${slave2}" "ok"
	run_client ${slave1} "changerole ${tid} 1 leader" "ok"
	run_client ${slave1} "gettablestatus ${tid} 1" "kTableLeader"
	run_client ${slave1} "pausesnapshot ${tid} 1" "ok"
    sleep 1
	run_client ${slave1} "addreplica ${tid} 1 ${slave2}" "ok"
	sleep 1
	run_client ${slave1} "put ${tid} 1 naysa 1555555555554 naysa" "ok"
	run_client ${slave2} "scan ${tid} 1 naysa 1555555555554 1" "1555555555554"
	run_client ${slave1} "drop ${tid} 1" "ok"
	deal_result $FUNCNAME
}

################## addreplica ##################

function test_addreplica_leader_add() {
	echo -e "\n主节点addreplica slave成功"
	tid=$((${tid}+1))
    run_client ${leader} "create t1 ${tid} 1 0 true ${slave1}" "ok"
    run_client ${leader} "pausesnapshot ${tid} 1" "ok"
    sleep 1
	run_client ${leader} "addreplica ${tid} 1 ${slave2}" "ok"
	run_client ${leader} "drop ${tid} 1" "ok"
	deal_result $FUNCNAME
}

function test_addreplica_change_to_normal() {
	echo -e "\n主节点addreplica之后，状态变回normal"
	tid=$((${tid}+1))
    run_client ${leader} "create t1 ${tid} 1 0 true ${slave1}" "ok"
    run_client ${leader} "gettablestatus ${tid} 1" "kTableNormal"
    run_client ${leader} "pausesnapshot ${tid} 1" "ok"
    sleep 1
    run_client ${leader} "gettablestatus ${tid} 1" "kTablePaused"
	run_client ${leader} "addreplica ${tid} 1 ${slave2}" "ok"
    run_client ${leader} "gettablestatus ${tid} 1" "kTableNormal"
	run_client ${leader} "drop ${tid} 1" "ok"
	deal_result $FUNCNAME
}

function test_addreplica_changerole_to_leader_add() {
	echo -e "\n切换从节点为主节点，addreplica slave成功"
	tid=$((${tid}+1))
	run_client ${slave1} "create t1 ${tid} 1 0 false ${slave1}" "ok"
	run_client ${slave1} "changerole ${tid} 1 leader" "ok"
	run_client ${slave1} "gettablestatus ${tid} 1" "kTableLeader"
	run_client ${slave1} "pausesnapshot ${tid} 1" "ok"
    sleep 1
	run_client ${slave1} "addreplica ${tid} 1 ${slave2}" "ok"
	run_client ${slave1} "drop ${tid} 1" "ok"
	deal_result $FUNCNAME
}

function test_addreplica_slave_cannot_add() {
	echo -e "\n从节点不允许addreplica"
	tid=$((${tid}+1))
    run_client ${slave1} "create t1 ${tid} 1 0 false ${slave1}" "ok"
    run_client ${slave1} "pausesnapshot ${tid} 1" "Fail"
    sleep 1
	run_client ${slave1} "addreplica ${tid} 1 ${slave2}" "Fail"
	run_client ${slave1} "drop ${tid} 1" "ok"
	deal_result $FUNCNAME
}

################## Scenario ##################

function test_scenario_add_new_slave() {
	echo -e "\n新增从节点，slave可以同步新put的数据"
	tid=$((${tid}+1))
	stop_all_clients
	sleep 1
    start_client ${leaderpath}
	run_client ${leader} "create t1 ${tid} 1 0 true ${slave2}" "ok"
	run_client ${leader} "put ${tid} 1 naysa 1555555555555 naysa" "ok"
	run_client ${leader} "pausesnapshot ${tid} 1" "ok"
	sleep 1
	run_client ${leader} "gettablestatus ${tid} 1" "kTablePaused"

	rm -rf ${slave1path}/snapshots/*
	rm -rf ${slave1path}/binlogs/*
	cp -a ${leaderpath}/snapshots/${tid}_1 ${slave1path}/snapshots
    start_client ${slave1path}
	run_client ${slave1} "loadsnapshot ${tid} 1" "ok"
	run_client ${slave1} "loadtable t1 ${tid} 1 0" "ok"
	sleep 1
	run_client ${slave1} "gettablestatus ${tid} 1" "kTableFollower"

	run_client ${leader} "addreplica ${tid} 1 ${slave1}" "ok"
	sleep 1
	run_client ${slave1} "scan ${tid} 1 naysa 1555555555555 1" "1555555555555"

	run_client ${leader} "put ${tid} 1 naysa 1555555555554 naysa" "ok"
	run_client ${slave1} "scan ${tid} 1 naysa 1555555555554 1" "1555555555554"
	run_client ${slave1} "scan ${tid} 1 naysa 1555555555555 1" "1555555555555"

	run_client ${leader} "drop ${tid} 1" "ok"
	run_client ${slave1} "drop ${tid} 1" "ok"
	start_client ${slave2path}
	deal_result $FUNCNAME
}

function test_scenario_add_new_slave_ttl_wrong() {
	echo -e "\n新增从节点，slave loadtable时ttl与主节点不一致，可以load成功"
	tid=$((${tid}+1))
	stop_all_clients
	sleep 1
    start_client ${leaderpath}
	run_client ${leader} "create t1 ${tid} 1 0 true ${slave2}" "ok"
	run_client ${leader} "put ${tid} 1 naysa 1555555555555 naysa" "ok"
	run_client ${leader} "pausesnapshot ${tid} 1" "ok"
	sleep 1
	run_client ${leader} "gettablestatus ${tid} 1" "kTablePaused"

	rm -rf ${slave1path}/snapshots/*
	rm -rf ${slave1path}/binlogs/*
	cp -a ${leaderpath}/snapshots/${tid}_1 ${slave1path}/snapshots
    start_client ${slave1path}
	run_client ${slave1} "loadsnapshot ${tid} 1" "ok"
	run_client ${slave1} "loadtable t1 ${tid} 1 1000" "ok"
	start_client ${slave2path}
	deal_result $FUNCNAME
}

function test_scenario_slave_restore_while_putting() {
	echo -e "\n主节点写数据过程中，slave挂掉后恢复，loadsnapshot和loadtable后数据正确"
	tid=$((${tid}+1))
	run_client ${leader} "create t1 ${tid} 1 0 true ${slave1}" "ok"
	run_client ${slave1} "create t1 ${tid} 1 0 false ${slave1}" "ok"

    for i in `seq 1 2`
    do
    {
        if [ "${i}" == 1 ];then
            for (( j=1500000000000; j<1500000000015; j++)) {
                run_client ${leader} "put ${tid} 1 naysa ${j} ${j}value" "ok"
                run_client ${slave1} "scan ${tid} 1 naysa ${j} 1" "${j}value"
                sleep 1
            }
        else
            sleep 3
            stop_client ${slave1}
            sleep 3
            start_client ${slave1path}
            sleep 2
            run_client ${slave1} "loadsnapshot ${tid} 1" "ok"
            sleep 2
	        run_client ${slave1} "loadtable t1 ${tid} 1 0" "ok"
	        sleep 2
	        run_client ${slave1} "gettablestatus ${tid} 1" "kTable"
	        sleep 3
        fi
    } &
    done
    wait
    result=1
    run_client ${slave1} "gettablestatus ${tid} 1" "15"
	deal_result $FUNCNAME
}

################## TEST ##################

echo "测试Suite开始:"
setup

stop_all_clients
sleep 2

start_client ${leaderpath}
start_client ${slave1path}
start_client ${slave2path}
sleep 2

test_gettablestatus_all
test_gettablestatus_tid_pid
test_create_table
test_put_slave_sync
test_put_slave_cannot_put
test_pausesnapshot_leader
test_pausesnapshot_slave_cannot_pause
test_pausesnapshot_leader_can_put_can_be_synchronized
test_loadsnapshot
test_loadtable
test_loadtable_leader
test_loadtable_slave
test_loadtable_without_snapshot
test_loadtable_after_drop
test_loadtable_after_sync
test_changerole_to_leader
test_changerole_to_leader_can_put
test_changerole_to_leader_can_pausesnapshot
test_changerole_to_leader_can_addreplica
test_changerole_to_leader_can_be_synchronized
test_addreplica_leader_add
test_addreplica_change_to_normal
test_addreplica_changerole_to_leader_add
test_addreplica_slave_cannot_add
test_scenario_add_new_slave
test_scenario_add_new_slave_ttl_wrong
test_scenario_slave_restore_while_putting
echo -e "\nFAILURES:"
for i in "${failures[@]}"
do
    echo ${i}
done