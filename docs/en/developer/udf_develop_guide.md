# 自定义函数开发
## 1. 背景
虽然OpenMLDB内置了上百个函数，以供数据科学家作数据分析和特征抽取。但是在某些场景下还是不能很好的满足要求，以往只能通过开发内置函数来实现。内置函数开发需要重新编译二进制文件等待版本发布，周期相对较长。为了便于用户快速灵活实现特定的特征计算需求，我们实现了用户动态注册函数的机制。

一般SQL函数分为单行函数和聚合函数，目前我们只支持单行函数的自定义开发，预计下个版本会支持开发自定义聚合函数。关于单行函数和聚合函数的介绍可以参考[这里](./built_in_function_develop_guide.md)
## 2. 开发步骤
### 2.1 开发自定义函数
#### 2.1.1 C++函数名规范
- C++内置函数名统一使用[snake_case](https://en.wikipedia.org/wiki/Snake_case)风格
- 要求函数名能清晰表达函数功能
- 函数不能重名。函数名不能和内置函数及其他自定义函数重名。所有内置函数的列表参考[这里](../reference/sql/functions_and_operators/Files/udfs_8h.md)
#### 2.1.2 C++类型与SQL类型对应关系
内置C++函数的参数类型限定为：BOOL类型，数值类型，时间戳日期类型和字符串类型。C++类型SQL类型对应关系如下：

| SQL类型   | C/C++ 类型         |
| :-------- | :----------------- |
| BOOL      | `bool`             |
| SMALLINT  | `int16_t`          |
| INT       | `int32_t`          |
| BIGINT    | `int64_t`          |
| FLOAT     | `float`            |
| DOUBLE    | `double`           |
| STRING    | `StringRef` |
| TIMESTAMP | `Timestamp` |
| DATE      | `Date`      |


#### 2.1.3 函数参数和返回值

**Return Value**:

* If the output type of the UDF is basic type, it will be processed as return value.
* If the output type of the UDF is STRING, TIMESTAMP or DATE, it will return through the last parameter of the function.

**Parameters**: 

* 如果输入字段是基本类型，通过值传递
* 如果输入字段是string, timestamp, date, 通过指针传递
* 函数的第一个参数必须是UDFContext* ctx，不能更改。[UDFContext](../../../include/udf/openmldb_udf.h)的定义如下:
    ```c++
    struct UDFContext {
        ByteMemoryPool* pool;  // 用来分配内存
        void* ptr;             // 用来存储临时变量。目前单行函数用不到
    };
    ```

**Function Declaration**:
  
* The functions must be declared by extern "C".

#### 2.1.4 Memory Management

- It is not allowed to use `new` operator or `malloc` function to request new memory space in UDF functions.
- If you need to request additional memory space dynamically, please use the memory management interface provided by OpenMLDB. OpenMLDB will automatically free the memory space after the function is executed. 

```c++
    char *buffer = ctx->pool->Alloc(size);
```

- The maximum size of the space allocated at a time cannot exceed 2M bytes.


#### 2.1.5 Implement the Function
- The head file `udf/openmldb_udf.h` should be included.
- Realize the logic of the function.

```c++
#include "udf/openmldb_udf.h"  // 必须包含此头文件
 
// 实现一个udf，截取字符串的前两个字符
extern "C"
void cut2(UDFContext* ctx, StringRef* input, StringRef* output) {
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

For more UDF implementation, see [here](../../../src/examples/test_udf.cc).


### 2.2 Compile the Dynamic Library 

- Copy the `include` directory (https://github.com/4paradigm/OpenMLDB/tree/main/include) to a certain path (like `/work/OpenMLDB/`) for later compiling. 
- Run the compiling command. `-I` specifies the path of `include` directory. `-o` specifies the name of the dynamic library.

```shell
g++ -shared -o libtest_udf.so examples/test_udf.cc -I /work/OpenMLDB/include -std=c++11 -fPIC
```

### 2.3 Copy the Dynamic Library
- The compiled dynamic libraries should be copied into the `udf` directory of the path which OpenMLDB `tablet/taskmanager` is deployed. Please create one if this directory does not exist.  
- Please note that the `udf` directory of `tablet` is level with the `bin/conf` directory.
- The `taskmanager` should be placed in `taskmanager/bin/udf`. 
- If the deployment paths of `tablet` and `taskmanager` are both `/work/openmldb`, the structure of the directory is shown below:

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
- Each tablet's directory needs to be copied.
- Dynamic libraries in this directory cannot be dropped before the execution of `DROP FUNCTION`.



### 2.4 Register, Drop and Show the Functions
For registering, please use [CREATE FUNCTION](../reference/sql/ddl/FUNCTION_STATEMENT.md).
```sql
CREATE FUNCTION cut2(x STRING) RETURNS STRING OPTIONS (FILE='libtest_udf.so');
```

```{note}
- The types of parameters and return values must be consistent with the implementation of the code.
- `FILE` specifies the file name of the dynamic library. It is not necessary to include a path.
- A UDF function can only work on one type. Please create multiple functions for multiple types.
```

After successful registration, the function can be used.
```sql
SELECT cut2(c1) FROM t1;
```

You can view registered functions through `SHOW FUNCTIONS`.
```sql
SHOW FUNCTIONS;
```

Please use the `DROP FUNCTION` to delete a registered function.
```sql
DROP FUNCTION cut2;
```