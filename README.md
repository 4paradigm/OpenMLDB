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

* tablet 单测case丰富
