# rtidb
Database for Real Time Intelligence

# build
```
sh build.sh
```

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
* [ ] 支持数据落地，重启恢复
* [ ] 支持批量导入，方便离线快速导入数据或者故障快速恢复数据

# 特点

rtidb是面向机器学习场景的高性能内存db, 摒弃了传统在线数据库部分功能比如事务，它的特点
* 局部有序存储, 能够很好满足反欺诈在线针对单张卡片交易历史分析需求， 与redis相比，redis list 存储容量在千条级别，rtidb 存储容量无上限，而且还能够按照key形成一个有序list, 支持seek 和scan 操作
* 读写互相不影响，rtidb采用skiplist做为核心数据结构，对并发读写非常友好，读取数据时不需要加锁
* 支持TTL, 因为rtidb使用skiplist作为核心数据结构，内存回收时，对读操作零影响
* 高性能内存池，rtidb使用tcmalloc管理内存分配，[性能指标](http://goog-perftools.sourceforge.net/doc/tcmalloc.html)
* 高度定制化序列化协议，encode比protobuf快10倍，decode比protobuf快1倍,[详细见](src/base/codec_bench_test.cc)
* 使用c++开发，保证服务高稳定的响应时间，不会存在类似java gc问题
