# rtidb
Database for Real Time Intelligence , It's awesome!

# version

0.9.0
[![Build Status](http://jenkins.4paradigm.com/view/rtidb_pipline/job/rtidb/badge/icon)](http://jenkins.4paradigm.com/view/rtidb_pipline/job/rtidb/)

# start tablet server

```
cd build/bin
./rtidb --role=tablet >log 2>&1 &
```

# start client

```
cd build/bin
./rtidb --role=client
```

# create table

```
>create t0 1 1 1
```

# put 

```
>put 1 1 testkey 9527 testvalue
Put 1 ok, latency 0 ms
```

# todo

* [x] tablet 单测case丰富
* [ ] 大小端编码优化
* [ ] sdk支持LengthWindow, SlidingTimeWindow, Session
* [ ] 支持主从复制
* [x] 支持数据落地，重启恢复
* [ ] 支持批量导入，方便离线快速导入数据或者故障快速恢复数据
* [ ] 支持hashtable存储引擎，在反欺诈场景，获取card/账户信息 这个场景是seek操作，如果使用skiplist，会有性能损失[map vs unordered_map](http://kariddi.blogspot.jp/2012/07/c11-unorderedmap-vs-map.html)
* [ ] 支持手动rebalance

# 特点

rtidb是面向机器学习场景的高性能内存时序数据db, 摒弃了传统在线数据库部分功能比如事务，它的特点
* 局部有序存储, 能够很好满足反欺诈在线针对单张卡片交易历史分析需求， 与redis相比，redis无法保存时序数据
* 读写互相不影响，rtidb采用skiplist做为核心数据结构，对并发读写非常友好，读取数据时不需要加锁
* 支持TTL, 因为rtidb使用skiplist作为核心数据结构，内存回收时，对读操作零影响
* 高性能内存池，rtidb使用tcmalloc管理内存分配，对比jvm 内存管理，rtidb内存创建和释放需要手动, 避免了jvm gc带来的问题，也能避免频繁调用内核的系统调用[性能指标](http://goog-perftools.sourceforge.net/doc/tcmalloc.html)
* 高度定制化序列化协议，encode比protobuf快10倍，decode比protobuf快2倍,[详细见](src/base/codec_bench_test.cc)
* 使用c++开发，保证服务高稳定的响应时间，不会存在类似java gc问题
* 支持高级时序数据结构Lengthwindow, SlidingTimewindow, Session

# 名词解释

* 时序记录提取，按照时间区间从数据库获取相关数据，比如反欺诈中查询单张信用卡最近一个月的交易记录
* java gc，java gc是指java 内存回收操作，在进行内存回收过程中，java 服务会暂停运行，直到操作完成
