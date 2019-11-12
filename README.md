# FeSQL

The first and fastest AI Native Database in the world

## build 

安装基础工具
* wget
* unzip
* texinfo
* gcc 8.3.1

Linux安装依赖库

```
sh tools/get_deps.sh
```

Mac下安装依赖库(缺RocksDB和zookeepr)

```shell
brew tap iveney/mocha
brew install realpath
brew install gettext
brew install gettext autoconf
brew install libtool
brew install pinfo
ln -s `brew ls gettext | grep bin/autopoint` /usr/local/bin

sh tools/get_deps.sh mac
```

编译

```
mkdir build && cd build && cmake .. && make -j4
```

运行测试

```
cd build && make test
```

运行覆盖统计

```
cd build && make coverage
```

## 添加测试

按照如下添加测试，方便make test能够运行
```
add_executable(flatbuf_ir_builder_test flatbuf_ir_builder_test.cc)
target_link_libraries(flatbuf_ir_builder_test gflags fesql_codegen fesql_proto ${llvm_libs} protobuf glog gtest pthread)
add_test(flatbuf_ir_builder_test flatbuf_ir_builder_test --gtest_output=xml:${CMAKE_BINARY_DIR}/flatbuf_ir_builder_test.xml)
```

## 添加覆盖率

将测试target append到test_list里面，方便make coverage能够运行
```
list(APPEND test_list flatbuf_ir_builder_test)
```

## 规范检查

请使用tools/cpplint.py 检查代码规范,
如果有一些格式问题，可以使用clang-format去格式化代码
```
tools/cpplint.py  filename
clang-format filename
```

## CMD使用说明

### 启动服务端

```shell script
./fesql --role=dbms --port=9111 --log_dir=./log/dbms &
```

### 启动客户端

```shell script
./fesql --role=client --endpoint=127.0.0.1:9111 --log_dir=./log/client
```

### CMD SQL

#### 创建数据库

```shell script
> CREATE DATABAE db_name
```
#### 查看所有数据库列表信息
```shell script
> SHOW DATABASES;
```
#### 进入数据库
```shell script
> USE db_name;
```

#### 查看当前数据库下表信息
```shell script
> SHOW TABLES;
```

#### 从文件创建schema

```shell script
> .CREATE TABLE schema_file_path
```

#### 创建表
```shell script
> CREATE TABLE test (
... column1 int NOT NULL,
... column2 int NOT NULL
... );
```

#### 查看表schema 

```SQL
> DESC table_name
```

### 查询SQL
```shell script
> SELECT col1, col2, col3 from t1;
```





