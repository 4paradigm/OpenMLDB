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
4. 编译OpenMLDB
    ```
    make
    ```

## make 额外参数

控制 `make` 的行为. 例如，将默认编译模式改成 Debug:

```bash
make CMAKE_BUILD_TYPE=Debug
```

- CMAKE_BUILD_TYPE

  Default: Release

- SQL_PYSDK_ENABLE

  enable build python sdk

  Default: ON

- SQL_JAVASDK_ENABLE

  enable build java sdk

  Default: ON

- TESTING_ENABLE

  enable build test targets

  Default: ON

