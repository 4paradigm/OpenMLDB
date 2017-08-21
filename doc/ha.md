# rtidb 高可用说明

# 1. 新增节点
# (1) 在主节点运行pausesnapshot
# cmd: pausesnapshot tid pid
> pausesnapshot 1 123

# (2) 获取主节点的状态为paused时将文件拷贝到新增节点的snapshot目录下
# cmd: gettablestatus tid pid
> gettablestatus 1 123

# (3) 启动从节点的rtidb程序

# (4) 在从节点上运行loadsnapshot命令
# cmd: loadsnapshot tid pid
> loadsnapshot 1 123

# (5) 在从节点上运行loadtable命令
# cmd: loadtable tid pid
> loadtable 1 123

# (6) 获取从节点的状态为normal时加载完成(正在加载的状态为loading)
# cmd: gettablestatus
> gettablestatus 1 123

# (7) 在主节点上运行addreplica命令，建立主从关系
# cmd: addreplica tid pid endpoint
> addreplica 1 123 127.0.0.1:8893


# 2. 从节点变为主节点
# cmd: changerole tid pid leader
> changerole 1 123 leader
