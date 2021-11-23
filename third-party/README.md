# Cmake module to download, compile and install required dependencies by OpenMLDB

## Usage
Use cmake to create a build directory and it will setup dependencies into build directory. e.g

```bash
cmake -S . -Bbuild
cmake --build build
```

By default, the built directory is
```bash
third-party/
└── build/
    ├── build/
    │   ├── downloads        # dependencies artifacts download directory
    │   ├── src              # dependencies source code (from VCS) download directory, or unpacked directory from downloaded artifacts
    │   └── tmp              # temporary files directory
    ├── src/                 # source code install directory
    └── usr/                 # compiled libraries install directory
        ├── bin
        ├── certs
        ├── include
        ├── lib
        ├── misc
        ├── openssl.cnf
        ├── openssl.cnf.dist
        ├── private
        └── share
```
where compiled libraries are installed into `build/usr`, source code installed into `build/src`

## Available cmake options

- BUILD_BUNDLED:BOOL=OFF

    Build dependencies from source

- BUILD_BUNDLED_HYBRIDSQL_ASSERTS:BOOL=OFF

    ***Note: setting to ON not supported***

    Build hybridsql asserts from source.

- BUILD_BUNDLED_ZETASQL:BOOL=OFF

    Build zetasql from source

- DEPS_BUILD_DIR:PATH=${CMAKE_BINARY_DIR}/build

    Dependencies build directory.

- DEPS_DOWNLOAD_DIR:PATH=${DEPS_BUILD_DIR}/downloads

    Dependencies download directory.

- DEPS_INSTALL_DIR:PATH=${CMAKE_BINARY_DIR}/usr

    Dependencies install directory.

- SRC_INSTALL_DIR:PATH=${CMAKE_BINARY_DIR}/src

    Source code install directory.

- USE_BUNDLED_SRC:BOOL=ON

    Use bundled hybridsql asserts source
