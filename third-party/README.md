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

- BUILD_BUNDLED_ZETASQL:BOOL=OFF

    Build zetasql from source

- BUILD_BUNDLED_XXX:BOOL=OFF

    Build a thridparty component from source

- DEPS_BUILD_DIR:PATH=${CMAKE_BINARY_DIR}/build

    Dependencies build directory.

- DEPS_DOWNLOAD_DIR:PATH=${DEPS_BUILD_DIR}/downloads

    Dependencies download directory.

- DEPS_INSTALL_DIR:PATH=${CMAKE_BINARY_DIR}/usr

    Dependencies install directory.

- SRC_INSTALL_DIR:PATH=${CMAKE_BINARY_DIR}/src

    Source code install directory.

- WITH_ZETASQL:BOOL=ON

    Download and build zetasql

## Guide to compile thirdparty all from source

By default, cmake will download pre-compiled thirdparty to speedup OpenMLDB building. However that may not always goes well since the pre-compiled thirdparty is not platform compatible.
The pre-compiled thirdparty for Linux is built on Centos7 with gcc8, and macOS is built on macOS 11. If that thirdparty not suit your platform, you need compile thirdparty from source.

### Requirements

- gcc8 or later
- cmake
- python3-devel (python3-dev on Debian) and set python3 as default python
- libcppunit-devel (libcppunit-dev on Debian)
- bison
- libtool
- bazel 1.0.0 or bazelisk
- tcl
- make, autoreconf
- pkg-config


For Debian:
```sh
sudo apt-get install bison python3-dev libcppunit-dev build-essential cmake autoconf tcl pkg-config git curl patch libtool-bin unzip 
# ensure python3 is the default, you may skip if it already is
sudo update-alternatives --install /usr/bin/python python /usr/bin/python3 100

curl --create-dirs -SLo /usr/local/bin/bazel https://github.com/bazelbuild/bazelisk/releases/download/v1.19.0/bazelisk-linux-amd64
chmod +x /usr/local/bin/bazel
```

### Build thirdparty

```bash
cmake -S third-party -B .deps -DBUILD_BUNDLED=ON
cmake --build .deps
```

### Troubleshooting

1. zookeeper fail to compile
   
   If gcc >= 9, try apply the experimental patch by pass `-DBUILD_ZOOKEEPER_PATH=ON` to cmake
