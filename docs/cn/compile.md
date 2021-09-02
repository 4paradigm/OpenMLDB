# 编译OpenMLDB

## Linux环境编译
1. 下载代码
    ```
    git clone git@github.com:4paradigm/OpenMLDB.git
    cd OpenMLDB
    ```
2. 下载docker镜像
    ```
    docker pull ghcr.io/4paradigm/hybridsql:0.4.0
    ```
3. 启动docker，并绑定本地OpenMLDB目录到docker中
    ```
    docker run -v `pwd`:/OpenMLDB -it ghcr.io/4paradigm/hybridsql:0.4.0 bash
    ```
4. 下载依赖包并初始化环境(只需要初始化一次)
    ```
    cd /OpenMLDB
    bash steps/init_env.sh  
    ``` 
5. 编译OpenMLDB
    ```
    mkdir build && cd build
    cmake ..
    make -j5 openmldb
    ```
