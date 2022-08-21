# UDF 函数文档生成

## 要求和准备

- 熟悉如何[编译 OpenMLDB](../deploy/compile.md)
- Host 已安装 [doxygen](https://doxygen.nl/), [doxybook2](https://github.com/matusnovak/doxybook2)


## 步骤
### 1. 编译 export_udf_info

```bash
cmake --build build --target export_udf_info
```

### 2. 生成 Doxygen 文档

```bash
cd ${project_root}/hybridse/tools/documentation/udf_doxygen/
python3 export_udf_doc.py
```

会在 udf_doxygen 目录下生成文件:

```bash
udf_doxygen/
├── html/   # doxygen 生成的网页文件
├── udfs/   # udfs.h
└── xml/    # doxygen 生成的 XML 文件
```



### 3. Doxygen 文档生成 Markdown

```bash
cd udf_doxygen
mkdir -p udfgen
doxybook2 --input xml --output udfgen/ --config config.json --templates ../template --summary-input SUMMARY.md.tmpl --summary-output SUMMARY.md
```

将生成 `udf_doxygen/udfgen` 目录

### 4. 将生成的 Markdown 文件更新到正确位置

```bash
cp udfgen/Files/udfs_8h.md ${project_root}/docs/zh/reference/sql/functions_and_operators/Files/udfs_8h.md
```

可能需要手动检查 `udfs_8h.md` 并去除一下不必要的改动，例如 Header。

### Commit change and pull request

