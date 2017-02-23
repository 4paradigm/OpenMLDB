# rtidb
Database for Real Time Intelligence

# build
```
# install thirdparty
sh build.sh
# gen cpp
sh gen_cpp.sh
# complie
mkdir build
cd build 
cmake ..
make -j4
```

# start tablet server

```
cd build/bin
./rtidb >log 2>&1 &
```

# start cli

```
cd build/bin
./rtidb_cli
>put 3 1 1
Put 1 ok, latency 0 ms
>put 3 2 2
Put 2 ok, latency 0 ms
>put 3 3 3
Put 3 ok, latency 0 ms
>put 3 4 4
Put 4 ok, latency 0 ms
>scan 3 2 3
key:3/2  value:2
key:3/3  value:3
scan 2 records, latency 0 ms
```

# todo

* update sofa rpc to 1.1.1
