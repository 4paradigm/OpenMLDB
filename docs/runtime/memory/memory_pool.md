# Memory Pools

## 内存池设计

内存管理器：负责FeSQL引擎执行过程中的内存分配以及释放。内存管理器维护一批内存块。系统通过内存管理器申请一片分配内存区域（memory chuck)。当内存块用光时，内存管理器再扩展内存（从堆上申请新内存块）。内存管理器析构后，自动释放所有内存块。

[内存分配示意图]

![image-20200722114338949](./img/memory_pool.png)

```C++
class MemoryChunk {
 public:
    MemoryChunk(MemoryChunk* next, size_t request_size)
        : next_(next),
          chuck_size_(request_size > DEFAULT_CHUCK_SIZE ? request_size
                                                        : DEFAULT_CHUCK_SIZE),
          allocated_size_(0),
          mem_(new char[chuck_size_]) {}
    ~MemoryChunk() { delete[] mem_; }
    char* Alloc(size_t request_size) {
        if (request_size > available_size()) {
            return nullptr;
        }
        char* addr = mem_ + allocated_size_;
        allocated_size_ += request_size;
        return addr;
    }  
    enum { DEFAULT_CHUCK_SIZE = 4096 };

 private:
    MemoryChunk* next_;
    size_t chuck_size_;
    size_t allocated_size_;
    char* mem_;
};

class ByteMemoryPool {
 public:
    ByteMemoryPool(size_t init_size = MemoryChunk::DEFAULT_CHUCK_SIZE)
        : chucks_(nullptr) {
        ExpandStorage(init_size);
    }
    ~ByteMemoryPool() {
       // free each chuck
    }
    char* Alloc(size_t request_size) {
        if (chucks_->available_size() < request_size) {
            ExpandStorage(request_size);
        }
        return chucks_->Alloc(request_size);
    }

    void ExpandStorage(size_t request_size) {
        chucks_ = new MemoryChunk(chucks_, request_size);
    }

 private:
    MemoryChunk* chucks_;
};
```





## 内存池使用

```C++
{
ByteMemoryPool mem_pool;
uint32_t ret = udf(row_ptrs, window_ptr, row_sizes, &mem_pool, &out_buf); // udf中将使用mem_pool申请临时内存
}

udf(...) {
  // ...
  mem_pool->Alloc(request_size1);
  mem_pool->Alloc(request_size2);
  mem_pool->Alloc(request_size3);
  // ...
}
```



### ThreadLocal的内存分配和释放：

### 提供两个内置的函数:

```c++
inline int8_t *ThreadLocalMemoryPoolAlloc(int32_t request_size); //从Local内存池申请request_size大小的内存
void ThreadLocalMemoryPoolFree(); // 重置Local内存池（保留一个chuck的内存）
```



**BThread的问题**

> 调用阻塞的bthread函数后，所在的pthread很可能改变，这使[pthread_getspecific](http://linux.die.net/man/3/pthread_getspecific)，[gcc __thread](https://gcc.gnu.org/onlinedocs/gcc-4.2.4/gcc/Thread_002dLocal.html)和c++11 thread_local变量，pthread_self()等的值变化了，如下代码的行为是不可预计的：
>
> ```c++
> thread_local SomeObject obj;
> ...
> SomeObject* p = &obj;
> p->bar();
> bthread_usleep(1000);
> p->bar();
> ```



因此，考虑使用`使用bthread_key_create和bthread_getspecific`来支持ThreadLocalMemoryPoolAlloc和ThreadLocalMemoryPoolFree

但是这里有一个问题，就是

#### 参考：

【bthread-local】https://zhangxiao9999.gitbooks.io/ttt/content/docs/cn/server.html 

【thread-local问题】https://zhangxiao9999.gitbooks.io/ttt/content/docs/cn/thread_local.html



## 字节内存池的性能分析

未使用tcmalloc的情况下

```tex
----------------------------------------------------------------------------
Benchmark                                  Time             CPU   Iterations
----------------------------------------------------------------------------
BM_AllocFromByteMemPool1000/10         10214 ns        10213 ns        68345
BM_AllocFromByteMemPool1000/100        16930 ns        16928 ns        41018
BM_AllocFromByteMemPool1000/1000       76148 ns        76141 ns         9031
BM_AllocFromByteMemPool1000/10000     473221 ns       472960 ns         1515
BM_AllocFromNewFree1000/10            146773 ns       146754 ns         4707
BM_AllocFromNewFree1000/100           148141 ns       148134 ns         4701
BM_AllocFromNewFree1000/1000          149020 ns       149000 ns         4724
BM_AllocFromNewFree1000/10000         741939 ns       741827 ns          978

```



使用tcmalloc:

```
----------------------------------------------------------------------------
Benchmark                                  Time             CPU   Iterations
----------------------------------------------------------------------------
BM_AllocFromByteMemPool1000/10          8980 ns         8967 ns        80173
BM_AllocFromByteMemPool1000/100        15076 ns        15056 ns        46363
BM_AllocFromByteMemPool1000/1000       66856 ns        66844 ns        10434
BM_AllocFromByteMemPool1000/10000     386635 ns       386601 ns         1811
BM_AllocFromNewFree1000/10            127120 ns       127067 ns         5329
BM_AllocFromNewFree1000/100           128021 ns       127985 ns         5416
BM_AllocFromNewFree1000/1000          128419 ns       128410 ns         5468
BM_AllocFromNewFree1000/10000         602516 ns       602429 ns         1128

```



在打开tcmalloc的情况下，我们对比了ByteMemPool和NewFree的内存分配以及回收性能。

>BM_ByteMemPoolAlloc1000/X: ByteMemPool Alloc 1000块长度为X的区域。
>
>BM_NewFree1000/X: New 1000块长度为X的空间，并最后释放这1000块内存空间

比较明显的可以看出ByteMemPool在大量小内存申请和释放上有优势。

## 参考文献：

http://cplusplus.wikidot.com/cn:memory-management