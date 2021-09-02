# Compile OpenMLDB

## Compile on Linux
1. Download source code
    ```
    git clone git@github.com:4paradigm/OpenMLDB.git
    cd OpenMLDB
    ```
2. Download docker image
    ```
    docker pull ghcr.io/4paradigm/hybridsql:0.4.0
    ```
3. Start docker and map local dir in docker
    ```
    docker run -v `pwd`:/OpenMLDB -it ghcr.io/4paradigm/hybridsql:0.4.0 bash
    ```
4. DownLoad dependencies and init env(init only once)
    ```
    cd /OpenMLDB
    bash steps/init_env.sh  
    ``` 
5. Compile OpenMLDB
    ```
    mkdir build && cd build
    cmake ..
    make -j5 openmldb
    ```
