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
4. Compile OpenMLDB
    ```
    make
    ```

## Extra Options for make

customize make behavior by passing following arguments. E.g, change the build type to Debug:

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

