# 自定义函数（UDF）开发
## 背景
虽然OpenMLDB内置了上百个函数，以供数据科学家作数据分析和特征抽取。但是在某些场景下还是不能很好的满足要求，为了便于用户快速灵活实现特定的特征计算需求，我们支持了基于 C++ 的用户自定义函数（UDF）开发，以及动态用户自定义函数库的加载。

```{seealso}
用户也可以使用内置函数开发的方式扩展 OpenMLDB 的计算函数库。但是内置函数开发需要修改源代码和重新编译。如果用户希望贡献扩展函数到 OpenMLDB 代码库，那么可以参考[内置函数的开发文档](../developer/built_in_function_develop_guide.md)。
```

## 开发步骤
### 开发自定义函数
#### C++函数名规范
- C++内置函数名统一使用[snake_case](https://en.wikipedia.org/wiki/Snake_case)风格
- 要求函数名能清晰表达函数功能
- 函数不能重名。函数名不能和内置函数及其他自定义函数重名。所有内置函数的列表参考[这里](../openmldb_sql/udfs_8h.md)
#### C++类型与SQL类型对应关系

内置C++函数的参数类型限定为：BOOL类型，数值类型，时间戳日期类型和字符串类型。C++类型SQL类型对应关系如下：

| SQL类型   | C/C++ 类型  |
| :-------- | :---------- |
| BOOL      | `bool`      |
| SMALLINT  | `int16_t`   |
| INT       | `int32_t`   |
| BIGINT    | `int64_t`   |
| FLOAT     | `float`     |
| DOUBLE    | `double`    |
| STRING    | `StringRef` |
| TIMESTAMP | `Timestamp` |
| DATE      | `Date`      |
#### 函数参数和返回值
返回值:  
* 如果udf输出类型是基本类型，并且`return_nullable`设置为false, 则通过函数返回值返回
* 如果udf输出类型是基本类型，并且`return_nullable`设置为true, 则通过函数参数返回
* 如果udf输出类型是string, timestamp, date, 通过函数**最后一个参数**返回

参数: 
* 如果输入字段是基本类型，通过值传递
* 如果输入字段是string, timestamp, date, 通过指针传递
* 函数的第一个参数必须是UDFContext* ctx，不能更改。[UDFContext](../../../include/udf/openmldb_udf.h)的定义如下:
    ```c++
    struct UDFContext {
        ByteMemoryPool* pool;  // 用来分配内存
        void* ptr;             // 开发聚合函数时用来存储临时变量
    };
    ```

- 如果参数声明为nullable的，那么所有参数都是nullable的，每一个输入参数其后需要添加bool参数（通常命名为is_null），其顺序为`arg1, arg1_is_null, arg2, arg2_is_null, ...`。不可以随意调整参数顺序。
- 如果返回值声明为nullable的，那么通过参数来返回，并且添加bool参数（通常命名为is_null）来表示返回值是否为null

例如，函数sum有俩个参数，如果参数和返回值设置为nullable的话，单行函数原型如下:
```c++
extern "C"
void sum(::openmldb::base::UDFContext* ctx, int64_t input1, bool input1_is_null, int64_t input2, bool input2_is_null, int64_t* output, bool* is_null) {
```

函数声明:  
* 函数必须用extern "C"来声明

#### 内存管理

- 在单行函数中，不允许使用`new`和`malloc`给输入和输出参数开辟空间。函数内部可以使用`new`和`malloc`申请临时空间, 申请的空间在函数返回前需要释放掉。
- 在聚合函数中，在init函数中可以使用`new`/`malloc`开辟空间，但是必须在output函数中释放。最后的返回值如果是string需要保存在mempool开辟的空间中
- 若需要动态开辟空间，可以使用OpenMLDB提供的内存管理接口。函数执行完OpenMLDB会自动释放内存.
    ```c++
    char *buffer = ctx->pool->Alloc(size);
    ```
- 一次分配空间的最大长度不能超过2M字节

#### 单行函数开发

单行函数（scalar function）对单行数据进行处理，返回单个值，比如 `abs`, `sin`, `cos`, `date`, `year` 等。

具体开发步骤为：

1. 包含头文件udf/openmldb_udf.h 
2. 实现自定义函数逻辑

```c++
#include "udf/openmldb_udf.h"  // 必须包含此头文件
 
// 实现一个udf，截取字符串的前两个字符
extern "C"
void cut2(::openmldb::base::UDFContext* ctx, ::openmldb::base::StringRef* input, ::openmldb::base::StringRef* output) {
    if (input == nullptr || output == nullptr) {
        return;
    }
    uint32_t size = input->size_ <= 2 ? input->size_ : 2;
    // 在udf函数中申请内存空间需要用ctx->pool
    char *buffer = ctx->pool->Alloc(size);
    memcpy(buffer, input->data_, size);
    output->size_ = size;
    output->data_ = buffer;
}
```

因为返回值是string类型，所以此处需要通过函数最后一个参数返回。如果返回值是基本类型，通过函数返回值返回，可参考[test_udf.cc](https://github.com/4paradigm/OpenMLDB/blob/main/src/examples/test_udf.cc)中的strlength。

#### 聚合函数开发

聚合函数（aggregate function）对一个数据集（比如一列数据）执行计算，返回单个值，比如 `sum`, `avg`, `max`, `min`, `count` 等。

具体开发步骤为：

1. 包含头文件udf/openmldb_udf.h 
2. 实现自定义函数逻辑

开发一个聚合函数需要实现如下三个C++方法：
- init函数。 在init函数中做一些初始化工作，如开辟中间变量的空间等。函数命名格式为："聚合函数名_init"。
- update函数。对每一行响应字段的处理逻辑在update函数中。函数命名格式为："聚合函数名_update"。
- output函数。处理最后的聚合值，并返回结果。函数命名格式为："聚合函数名_output"。

**注**：在init函数和update函数中需要把`UDFContext*`作为返回值返回

```c++
#include "udf/openmldb_udf.h"  // 必须包含此头文件
// 实现名称为special_sum的聚合函数

extern "C"
::openmldb::base::UDFContext* special_sum_init(::openmldb::base::UDFContext* ctx) {
    // 开辟中间变量空间，并赋值給UDFContext中的ptr
    ctx->ptr = ctx->pool->Alloc(sizeof(int64_t));
    // 初始化中间变量的值
    *(reinterpret_cast<int64_t*>(ctx->ptr)) = 10;
    // 返回UDFContext指针，不能省略
    return ctx;
}

extern "C"
::openmldb::base::UDFContext* special_sum_update(::openmldb::base::UDFContext* ctx, int64_t input) {
    // 从UDFContext中的ptr取出中间变量并更新
    int64_t cur = *(reinterpret_cast<int64_t*>(ctx->ptr));
    cur += input;
    *(reinterpret_cast<int*>(ctx->ptr)) = cur;
    // 返回UDFContext指针，不能省略
    return ctx;
}

// 在output函数中处理聚合计算结果，并返回
extern "C"
int64_t special_sum_output(::openmldb::base::UDFContext* ctx) {
    return *(reinterpret_cast<int64_t*>(ctx->ptr)) + 5;
}

// Get the third non-null value of all values
extern "C"
::openmldb::base::UDFContext* third_init(::openmldb::base::UDFContext* ctx) {
    ctx->ptr = reinterpret_cast<void*>(new std::vector<int64_t>());
    return ctx;
}

extern "C"
::openmldb::base::UDFContext* third_update(::openmldb::base::UDFContext* ctx, int64_t input, bool is_null) {
    auto vec = reinterpret_cast<std::vector<int64_t>*>(ctx->ptr);
    if (!is_null && vec->size() < 3) {
        vec->push_back(input);
    }
    return ctx;
}

extern "C"
void third_output(::openmldb::base::UDFContext* ctx, int64_t* output, bool* is_null) {
    auto vec = reinterpret_cast<std::vector<int64_t>*>(ctx->ptr);
    if (vec->size() != 3) {
        *is_null = true;
    } else {
        *is_null = false;
        *output = vec->at(2);
    }
    // free the memory allocated in init function with new/malloc
    delete vec;
}

// Get the first non-null value >= threshold
extern "C"
::openmldb::base::UDFContext* first_ge_init(::openmldb::base::UDFContext* ctx) {
    // threshold init in update
    // threshold, thresh_flag, first_ge, first_ge_flag
    ctx->ptr = reinterpret_cast<void*>(new std::vector<int64_t>(4, 0));
    return ctx;
}

extern "C"
::openmldb::base::UDFContext* first_ge_update(::openmldb::base::UDFContext* ctx, int64_t input, bool is_null, int64_t threshold, bool threshold_is_null) {
    auto pair = reinterpret_cast<std::vector<int64_t>*>(ctx->ptr);
    if (!threshold_is_null && pair->at(1) == 0) {
        pair->at(0) = threshold;
        pair->at(1) = 1;
    }
    if (!is_null && pair->at(3) == 0 && input >= pair->at(0)) {
        pair->at(2) = input;
        pair->at(3) = 1;
    }
    return ctx;
}

extern "C"
void first_ge_output(::openmldb::base::UDFContext* ctx, int64_t* output, bool* is_null) {
    auto pair = reinterpret_cast<std::vector<int64_t>*>(ctx->ptr);
    // threshold is null or no value >= threshold
    if (pair->at(1) == 0 || pair->at(3) == 0) {
        *is_null = true;
    } else {
        *is_null = false;
        *output = pair->at(2);
    }
    // *is_null = true;
    // free the memory allocated in init function with new/malloc
    delete pair;
}
```

如上所示，聚合函数init函数仅单参数，无论是几个参数的聚合函数，init中都只有一个参数UDFContext。update函数参数个数和类型，与聚合函数的参数个数和类型一致。同样的，如果想要聚合函数nullable，每个参数都需要添加一个bool参数，表示该参数是否为null。output函数只会有一个输出参数或返回值，nullable同理。更多udf/udaf实现参考[这里](../../../src/examples/test_udf.cc)。

### 编译动态库
- 拷贝include目录 `https://github.com/4paradigm/OpenMLDB/tree/main/include` 到某个路径下，下一步编译会用到。如/work/OpenMLDB/
- 执行编译命令，其中 -I 指定inlcude目录的路径 -o 指定产出动态库的名称
  
```shell
g++ -shared -o libtest_udf.so examples/test_udf.cc -I /work/OpenMLDB/include -std=c++17 -fPIC
```

### 拷贝动态库
编译过的动态库需要被拷贝到 TaskManager 和 tablets中。如果 TaskManager 和 tablets中不存在`udf`目录，请先创建并重启这些进程（保证环境变量生效）。
- tablet的UDF目录是 `path_to_tablet/udf`。
- TaskManager的UDF目录是 `path_to_taskmanager/taskmanager/bin/udf`。

例如，假设tablet和TaskManager的部署路径都是 `/work/openmldb`，其目录结构将如下所示：

```
    /work/openmldb/
    ├── bin
    ├── conf
    ├── taskmanager
    │   ├── bin
    │   │   ├── taskmanager.sh
    │   │   └── udf
    │   │       └── libtest_udf.so
    │   ├── conf
    │   └── lib
    ├── tools
    └── udf
        └── libtest_udf.so
```


```{note}
- 对于有多个tablet场景，每个tablet都需要拷贝动态库。
- 在执行' DROP FUNCTION '之前请勿删除动态库。
```

### 注册、删除和查看函数
注册函数使用[CREATE FUNCTION](../openmldb_sql/ddl/CREATE_FUNCTION.md)

注册单行函数，cut2函数将字符串的前两个字符返回：
```sql
CREATE FUNCTION cut2(x STRING) RETURNS STRING OPTIONS (FILE='libtest_udf.so');
```
注册聚合函数，special_sum函数初始为10，再将输入的值累加，并且最后加上5返回（演示函数，无特殊意义）：
```sql
CREATE AGGREGATE FUNCTION special_sum(x BIGINT) RETURNS BIGINT OPTIONS (FILE='libtest_udf.so');
```
注册聚合函数，并且输入参数和返回值都支持null，third函数返回第三个非null的值，如果非null的值不足3个，返回null：
```sql
CREATE AGGREGATE FUNCTION third(x BIGINT) RETURNS BIGINT OPTIONS (FILE='libtest_udf.so', ARG_NULLABLE=true, RETURN_NULLABLE=true);
```
**注**:
- 参数类型和返回值类型必须和代码的实现保持一致
- 一个udf函数只能对一种类型起作用。如果想用于多种类型，需要创建多个函数
- `FILE` 指定动态库的文件名，不需要包含路径

成功注册后就可在 OpenMLDB 中使用该自定义函数：`SELECT cut2(c1) FROM t1` 

可以通过 `SHOW FUNCTIONS` 查看已注册的 UDF
```
SHOW FUNCTIONS;
```
通过 `DROP FUNCTION` 删除已注册的 UDF
```
DROP FUNCTION cut2;
```

```{warning}
同一个udf so如果注册了多个函数，只删除一个函数时，该so不会从Tablet Server内存中删除。此时替换so文件是无用的，并且如果此时增删udf，有一定危险影响Tablet Server运行。

建议：**删除所有udf后，替换udf so文件**。
```