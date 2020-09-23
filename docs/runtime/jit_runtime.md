# JIT Runtime

JIT运行时环境负责为JIT函数提供配置管理、内存管理等功能支持. 目前它包括
- 全局的MemoryPool
- 全局的对象池

## 特性
- Runtime是Thread Local对象，每个线程持有独立的runtime
    - 禁止在JIT内使用可导致协程sched的调用
    
- One step资源管理
    - 提供资源分配接口，分配声明周期为一次JIT run的资源
    - benefit: simple

## 接口使用
- 目前，runtime为一次JIT函数运行周期内分配的全部资源进行回收
    ```c++
    JITRuntime::get()->InitRunStep()
    // Call JIT function...
    JITRuntime::get()->ReleaseRunStep()
    ```

- 在JIT函数（及嵌套调用的C函数内)申请托管资源
    - new fe对象（基类必须为FeBaseObject）
        ```
        SomeFeObj obj = new SomeFeObj()
        JITRuntime::get()->AddManagedObject((FeBaseObject*)obj);
        ```
    - 申请裸托管内存
        ```c++
        int8_t* buf = JITRuntime::get()->AllocManaged(4096);
        ```
    - 申请字符串buf util
        ```c++
        char* buf = udf::v1::AllocManagedStringBuf(4096);
        ```