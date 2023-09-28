# Generate UDF documents for OpenMLDB

## Requirements

- [compile OpenMLDB](../deploy/compile.md)

- [doxygen](https://doxygen.nl/), [doxybook2](https://github.com/matusnovak/doxybook2) installed in host

- [poetry](https://python-poetry.org/) optional
  - or have [pyyaml](https://pypi.org/project/PyYAML/) >= 6.0 installed in host


## Brief

Simiply type `make udf_doc_gen` in top directory of OpenMLDB.

```bash
cd $(git rev-parse --show-toplevel)
make udf_doc_gen
```

## Detailed Steps

Here are the detailed steps inside `make udf_doc_gen`.

### 1. Compile export_udf_info

```bash
cd ${project_root}
cmake --build build --target export_udf_info
```

### 2. Generate Documents

Just type 

```bash
make
```

and all files will generated in current directory. In detail, it will execute two jobs as following:

#### 2.1. Generate Doxygen files

```bash
make doxygen
```

will output files:

```bash
udf_doxygen/
├── html/   # doxygen 生成的网页文件
├── udfs/   # udfs.h
└── xml/    # doxygen 生成的 XML 文件
```


#### 2.2 Convert doxygen xml files to markdown

```bash
make doxygen2md
```

will output `udf_doxygen/udfgen`.

### 3. Put the document into proper position

```bash
cp udfgen/Files/udfs_8h.md ${project_root}/docs/zh/openmldb_sql/udfs_8h.md
```

You may checkout changes manully and discard anything unnecessary like header.

### Commit change and pull request

