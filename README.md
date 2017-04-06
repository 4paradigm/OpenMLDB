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

* update sofa rpc to 1.1.1
* capnproto flatbuffer 性能对比
* 基于boost asio 和以上性能优势的序列化工具实现网络通讯
* 主从同步
* 支持分布式横向扩展
