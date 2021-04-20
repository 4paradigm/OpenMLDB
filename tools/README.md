# 打包测试相关工具

## 脚本目录结构

```bash
tools
├── autotest/
├── benchmark_report/
├── cicd/
│   ├── extract_intermediate_cicd_artifacts.sh  # get compile artifacts
│   └── gen_intermediate_cicd_artifacts.sh      # pack compile result
├── documentation/
│   ├── export_udf_doc.sh        # export udf documentations
│   └── api_doxygen/             # generate c++ api documentation
│   └── java_api/                # generate java api documentation
├── clang_format.sh              # format code
├── init_env.profile.sh          # init running environment
├── get_deps_for_sdk.sh          # install required dependencies
├── style_check.sh               # run cpplint
├── compile_and_test.sh          # compile & ut
├── compile_and_coverage.sh      # compile & coverage
├── gen_auto_case.sh             # gen sql case
├── micro_bench.sh               # micro benchmark
├── gen_micro_bench_compare.sh   # micro benchmark result comparison
└── test_java_sdk.sh             # java test
```

## 详细功能

### 1. 编译前

#### clang_format.sh

用`clang-format`格式化src目录下的文件

#### style_check.sh

代码 source lint 检测，使用 [cpplint](https://github.com/cpplint/cpplint)

#### get_deps_for_sdk.sh

下载并安装工程依赖到 $PROJECT_ROOT/{thirdparty, thirdsrc} (实验性)

#### init_env.profile.sh

初始化编译运行环境

### 2. 编译&功能测试

#### compile_and_test.sh

编译并运行测试

#### compile_and_coverage.sh

编译并运行覆盖测试

#### test_java_sdk.sh

java sdk 编译测试

#### cicd/gen_intermediate_cicd_artifacts.sh

生成编译产物

#### cicd/extract_intermediate_cicd_artifacts.sh

解压编译产物

#### documentation/export_udf_doc.sh

生成 udf 文档

### 3. 集成测试&性能测试

#### micro_bench.sh

运行 micro_benchmark 测试

#### gen_micro_bench_compare.sh

生成 micro benchmark 对比报告

#### gen_auto_case.sh

生成自动化的SQL Case

### 4. Generate API Doc

#### C++ API Doc 

1. Downlaod doxybook2 tools

```shell
cd documentation/api_doxygen
# download mac os doxygen2 
# wget https://github.com/matusnovak/doxybook2/releases/download/v1.3.3/doxybook2-linux-amd64-v1.3.3.zip
wget https://github.com/matusnovak/doxybook2/releases/download/v1.3.3/doxybook2-osx-amd64-v1.3.3.zip
unzip doxybook2-osx-amd64-v1.3.3.zip -d doxybook_home
```
2. Generate c++ api markdown into `c++` directory and generate summary into SUMMARY.md
```shell
doxygen
sh doxybook2.sh doxybook_home/bin/doxybook2
```

#### Java API Doc

1. Downlaod doxybook2 tools
```shell
cd documentation/java_api
# downlaod mac os doxygen2 
# wget https://github.com/matusnovak/doxybook2/releases/download/v1.3.3/doxybook2-osx-amd64-v1.3.3.zip
# unzip doxybook2-osx-amd64-v1.3.3.zip -d doxybook_home
wget https://github.com/matusnovak/doxybook2/releases/download/v1.3.3/doxybook2-osx-amd64-v1.3.3.zip
unzip doxybook2-osx-amd64-v1.3.3.zip -d doxybook_home
```

2. Generate java api markdown into `java` directory and generate summary into SUMMARY.md

```shell
mkdir java
doxygen
sh doxybook2.sh doxybook_home/bin/doxybook2
```