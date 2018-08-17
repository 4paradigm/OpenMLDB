# rtidb 单机部署文档

## 机器环境准备

* 关闭操作系统swap
* 关闭THP  
  echo 'never' > /sys/kernel/mm/transparent_hugepage/enabled  
  echo 'never' > /sys/kernel/mm/transparent_hugepage/defrag  
* 保证系统时钟正确(rtidb过期删除依赖于系统时钟, 如果系统时钟不正确会导致过期数据没有删掉或者删掉了没有过期的数据)  

## 部署tablet
* 配置endpoint, 有以下两种方式, 只需要选一种就行  
  1 打开配置文件conf/tablet.flags, 修改endpoint中的值  
  2 export endpoint=ip:port, 如export endpoint=172.27.128.31:9527  
* 启动服务
  sh ./bin/start.sh  

