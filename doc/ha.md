# rtidb 主从模式高可用说明

## 新增节点副本
1. 在主节点运行makesnapshot(如果已经有snapshot文件可以挑过这步)
cmd: makesnapshot tid pid
> makesnapshot 1 123

2. 获取主节点的状态为kNormal时运行pausesnapshot
cmd: gettablestatus tid pid
> gettablestatus 1 123

3. 在主节点运行pausesnapshot
cmd: pausesnapshot tid pid
> pausesnapshot 1 123

4. 启动从节点的rtidb程序

5. 在主节点上运行sendsnapshot命令
cmd: sendsnapshot tid pid endpoint(从节点的ip:port)
> sendsnapshot 1 123 0.0.0.0:8893

6. 查看从节点./db/tid_pid/snapshot目录下已经生成.sdb文件后在从节点上运行loadtable命令
cmd: loadtable name tid pid ttl seg_cnt is_leader
> loadtable table1 1 123 14400 8 false

7. 获取从节点的状态为kNormal时加载完成(正在加载的状态为kLoading)
cmd: gettablestatus
> gettablestatus 1 123

8. 在主节点上运行addreplica命令，建立主从关系
 cmd: addreplica tid pid endpoint(从节点ip:port)
> addreplica 1 123 127.0.0.1:8893

9. 把主节点snapshot由paused恢复为normal
 cmd: recoversnapshot tid pid
> recoversnapshot 1 123


## 从节点变为主节点
cmd: changerole tid pid leader
> changerole 1 123 leader
