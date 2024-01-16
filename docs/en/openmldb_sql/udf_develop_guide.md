# UDF Development Guideline
## Background
Although OpenMLDB provides over a hundred built-in functions for data scientists to perform data analysis and feature extraction, there are scenarios where these functions might not fully meet the requirements. To facilitate users in quickly and flexibly implementing specific feature computation needs, we have introduced support for user-defined functions (UDFs) based on C++ development. Additionally, we enable the loading of dynamically generated user-defined function libraries.

```{seealso}
Users can also extend OpenMLDB's computation function library using the method of developing built-in functions. However, developing built-in functions requires modifying the source code and recompiling. If users wish to contribute extended functions to the OpenMLDB codebase, they can refer to [Built-in Function Develop Guide](./built_in_function_develop_guide.md).
```

## Development Procedures
### Develop UDF functions
#### Naming Convention of C++ Built-in Function
- The naming of C++ built-in function should follow the [snake_case](https://en.wikipedia.org/wiki/Snake_case) style.
- The name should clearly express the function's purpose.
- The name of a function should not be the same as the name of a built-in function or other custom functions. The list of all built-in functions can be seen [here](../openmldb_sql/udfs_8h.md).

#### C++ Type and SQL Type Correlation
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


#### Parameters and Return Values

**Return Value**:

* If the output type of the UDF is a basic type and `return_nullable` set to false, it will be processed as a return value.
* If the output type of the UDF is a basic type and `return_nullable` set to true, it will be processed as a function parameter.
* If the output type of the UDF is STRING, TIMESTAMP or DATE, it will return through the **last parameter** of the function.

**Parameters**: 

* If the parameter is a basic type, it will be passed by value. 
* If the output type of the UDF is STRING, TIMESTAMP or DATE, it will be passed by a pointer. 
* The first parameter must be `UDFContext* ctx`. The definition of [UDFContext](../../../include/udf/openmldb_udf.h) is:

```c++
    struct UDFContext {
        ByteMemoryPool* pool;  // Used for memory allocation.
        void* ptr;             // Used for the storage of temporary variables for aggregate functions.
    };
```

**Function Declaration**:
  
* The functions must be declared by extern "C".

#### Memory Management

- In scalar functions, the use of 'new' and 'malloc' to allocate space for input and output parameters is not allowed. However, temporary space allocation using 'new' and 'malloc' is permissible within the function, and the allocated space must be freed before the function returns.

- In aggregate functions, space allocation using 'new' or 'malloc' can be performed in the 'init' function but must be released in the 'output' function. The final return value, if it is a string, needs to be stored in the space allocated by mempool.

- If dynamic memory allocation is required, OpenMLDB provides memory management interfaces. Upon function execution completion, OpenMLDB will automatically release the memory.
```c++
char *buffer = ctx->pool->Alloc(size);
```
- The maximum size allocated at once cannot exceed 2M.

**Note**:
- If the parameters are declared as nullable, then all parameters are nullable, and each input parameter will have an additional `is_null` parameter.
- If the return value is declared as nullable, it will be returned through parameters, and an additional `is_null` parameter will indicate whether the return value is null.
  
For instance, to declare a UDF scalar function, sum, which has two parameters, if the input and return value are nullable:
```c++
extern "C"
void sum(::openmldb::base::UDFContext* ctx, int64_t input1, bool is_null, int64_t input2, bool is_null, int64_t* output, bool* is_null) {
```
#### Scalar Function Implementation

Scalar functions process individual data rows and return a single value, such as abs, sin, cos, date, year.
The process is as follows:
- The head file `udf/openmldb_udf.h` should be included.
- Develop the logic of the function.

```c++
#include "udf/openmldb_udf.h"  // must include this header file
 
// Develop a UDF that slices the first 2 characters of a given string. 
extern "C"
void cut2(::openmldb::base::UDFContext* ctx, ::openmldb::base::StringRef* input, ::openmldb::base::StringRef* output) {
    if (input == nullptr || output == nullptr) {
        return;
    }
    uint32_t size = input->size_ <= 2 ? input->size_ : 2;
    //use ctx->pool for memory allocation
    char *buffer = ctx->pool->Alloc(size);
    memcpy(buffer, input->data_, size);
    output->size_ = size;
    output->data_ = buffer;
}
```


#### Aggregation Function Implementation

Aggregate functions process a dataset (such as a column of data) and perform computations, returning a single value, such as sum, avg, max, min, count.
The process is as follows:
- The head file `udf/openmldb_udf.h` should be included.
- Develop the logic of the function.

To develop an aggregate function, you need to implement the following three C++ methods:

- init function: Perform initialization tasks such as allocating space for intermediate variables. Function naming format: 'aggregate_function_name_init'.

- update function: Implement the logic for processing each row of the respective field in the update function. Function naming format: 'aggregate_function_name_update'.

- output function: Process the final aggregated value and return the result. Function naming format: 'aggregate_function_name_output'."

**Node**: Return `UDFContext*` as the return value in the init and update function.

```c++
#include "udf/openmldb_udf.h"  //must include this header file
// implementation of aggregation function special_sum
extern "C"
::openmldb::base::UDFContext* special_sum_init(::openmldb::base::UDFContext* ctx) {
    // allocate space for intermediate variables and assign to 'ptr' in UDFContext.
    ctx->ptr = ctx->pool->Alloc(sizeof(int64_t));
    // init the value
    *(reinterpret_cast<int64_t*>(ctx->ptr)) = 10;
    // return pointer of UDFContext, cannot be omitted
    return ctx;
}

extern "C"
::openmldb::base::UDFContext* special_sum_update(::openmldb::base::UDFContext* ctx, int64_t input) {
    // get the value from ptr in UDFContext
    int64_t cur = *(reinterpret_cast<int64_t*>(ctx->ptr));
    cur += input;
    *(reinterpret_cast<int*>(ctx->ptr)) = cur;
    // return the pointer of UDFContext, cannot be omitted
    return ctx;
}

// get the aggregation result from ptr in UDFcontext and return
extern "C"
int64_t special_sum_output(::openmldb::base::UDFContext* ctx) {
    return *(reinterpret_cast<int64_t*>(ctx->ptr)) + 5;
}

```


For more UDF implementation, see [here](../../../src/examples/test_udf.cc).


### Compile Dynamic Library 

- Copy the `include` directory (`https://github.com/4paradigm/OpenMLDB/tree/main/include`) to a certain path (like `/work/OpenMLDB/`) for later compiling. 
- Run the compiling command. `-I` specifies the path of the `include` directory. `-o` specifies the name of the dynamic library.

```shell
g++ -shared -o libtest_udf.so examples/test_udf.cc -I /work/OpenMLDB/include -std=c++11 -fPIC
```

### Copy Dynamic Library
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
- For multiple tablets, the library needs to be copied to every tablet. 
- Dynamic libraries should not be deleted before the execution of `DROP FUNCTION`.
```


### Register, Drop and Show the Functions
For registering, please use [CREATE FUNCTION](../openmldb_sql/ddl/CREATE_FUNCTION.md).

Register an scalar function：
```sql
CREATE FUNCTION cut2(x STRING) RETURNS STRING OPTIONS (FILE='libtest_udf.so');
```
Register an aggregation function:
```sql
CREATE AGGREGATE FUNCTION special_sum(x BIGINT) RETURNS BIGINT OPTIONS (FILE='libtest_udf.so');
```
Register an aggregation function with input value and return value support null:
```sql
CREATE AGGREGATE FUNCTION third(x BIGINT) RETURNS BIGINT OPTIONS (FILE='libtest_udf.so', ARG_NULLABLE=true, RETURN_NULLABLE=true);
```

**note**:
- The types of parameters and return values must be consistent with the implementation of the code.
- `FILE` specifies the file name of the dynamic library. It is not necessary to include a path.
- A UDF function can only work on one type. Please create multiple functions for multiple types.


After successful registration, the function can be used.
```sql
SELECT cut2(c1) FROM t1;
```

You can view registered functions through `SHOW FUNCTIONS`.
```sql
SHOW FUNCTIONS;
```

Use the `DROP FUNCTION` to delete a registered function.
```sql
DROP FUNCTION cut2;
```
