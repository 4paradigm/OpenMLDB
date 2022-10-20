# UDF Function Development Guideline
## 1. Background
Although there are already hundreds of built-in functions, they can not satisfy the needs in some cases. In the past, this could only be done by developing new built-in functions. Built-in function development requires a relatively long cycle because it needs to recompile binary files and users have to wait for new version release.
In order to help users to quickly develop computing functions that are not provided by OpenMLDB, we develop the mechanism of user dynamic registration function.

SQL functions can be categorised into single-line functions and aggregate functions. At present, only single-line UDFs are supported. An introduction to single-line functions and aggregate functions can be seen [here](./built_in_function_develop_guide.md).
## 2. Development Procedures
### 2.1 Develop UDF functions
#### 2.1.1 Naming Specification of C++ Built-in Function
- The naming of C++ built-in function should follow the [snake_case](https://en.wikipedia.org/wiki/Snake_case) style.
- The name should clearly express the function's purpose.
- The name of a function should not be the same as the name of a built-in function or other custom functions. The list of all built-in functions can be seen [here](../reference/sql/functions_and_operators/Files/udfs_8h.md).

#### 2.1.2 
The types of the built-in C++ functions' parameters should be BOOL, NUMBER, TIMESTAMP, DATE, or STRING.
The SQL types corresponding to C++ types are shown as follows:

| SQL Type  | C/C++ Type  |
|:----------|:------------|
| BOOL      | `bool`      |
| SMALLINT  | `int16_t`   |
| INT       | `int32_t`   |
| BIGINT    | `int64_t`   |
| FLOAT     | `float`     |
| DOUBLE    | `double`    |
| STRING    | `StringRef` |
| TIMESTAMP | `Timestamp` |
| DATE      | `Date`      |


#### 2.1.3 Parameters and Return Values

**Return Value**:

* If the output type of the UDF is a basic type, it will be processed as a return value.
* If the output type of the UDF is STRING, TIMESTAMP or DATE, it will return through the last parameter of the function.

**Parameters**: 

* If the parameter is a basic type, it will be passed by value. 
* If the output type of the UDF is STRING, TIMESTAMP or DATE, it will be passed by pointer. 
* The first parameter must be `UDFContext* ctx`. The definition of [UDFContext](../../../include/udf/openmldb_udf.h) is:

```c++
    struct UDFContext {
        ByteMemoryPool* pool;  // Used for memory allocation.
        void* ptr;             // Used for the storage of temporary variables. Single-line functions don't use it at present.
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
#include "udf/openmldb_udf.h"  // The headfile 
 
// Realize a UDF which slices the first 2 characters of a given string. 
extern "C"
void cut2(UDFContext* ctx, StringRef* input, StringRef* output) {
    if (input == nullptr || output == nullptr) {
        return;
    }
    uint32_t size = input->size_ <= 2 ? input->size_ : 2;
    //To apply memory space in UDF functions, please use ctx->pool.
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
The compiled dynamic libraries should be copied into the `udf` directories for both TaskManager and tablets. Please create a new `udf` directory if it does not exist. 
- The `udf` directory of a tablet is `path_to_tablet/udf`.
- The `udf` directory of TaskManager is `path_to_taskmanager/taskmanager/bin/udf`. 

For example, if the deployment paths of a tablet and TaskManager are both `/work/openmldb`, the structure of the directory is shown below:

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
- Note that, for multiple tablets, the library needs to be copied to every one. 
- Moreover, dynamic libraries should not be deleted before the execution of `DROP FUNCTION`.
```


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
