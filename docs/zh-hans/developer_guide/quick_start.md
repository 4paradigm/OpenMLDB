## 编译

### 使用Docker镜像。
```shell
docker run -it develop-registry.4pd.io/centos6_gcc7_fesql:master bash
```


## 从源码编译项目
```shell
git clone git@github.com:4paradigm/hybridse.git

mkdir -p ./hybridse/build
cd ./hybridse/build/

cmake ..
make -j4
```

## C++编程接口
HybridSE提供C++编程接口，用户可以在C/C++项目中使用来编译SQL以及生成最终的可执行代码
## Java编程接口
HybridSE也提供Java编程接口，基于Java/Scala的项目也可以使用来实现SQL语法支持，详情参考HybridSE Java SDK。


## 示例： 一个简单NewSQL数据库
开发者使用HybridSE可以快速实现一个支持SQL的实时数据库。examples/toydb就是一个简易的单机版面向实时决策的NewSQL数据库。

[使用HybridSE实现一个简易数据库](./toydb_tutorial/reference.md)
