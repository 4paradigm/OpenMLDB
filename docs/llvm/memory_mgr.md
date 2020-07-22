# memory_mgr

一次查询生成的代码在一个函数上。FESQL的内存管理策略是，尽可能在函数栈上分配空间，函数结束后自动释放空间。这样做有两个好处：
1. 不需要额外管理空间的释放
2. 栈上分配空间性能更好

但也有若干风险：
1. 栈上分配存在空间溢出的问题，需要配置好栈空间大小。特别是查询脚本非常复杂的情况下。极端情况下，需要将一次查询拆分成多个函数
2. llvm栈上可以分配c++对象空间，但回收时，不会自动调用对象的析构函数函数。需要谨慎处理c++对象的内存管理

## 常量内存管理


## 迭代器内存管理

### 列迭代器内存管理
 * 根据迭代器所占字节数直接在栈上分配迭代器所需内存
 * 创建时，传入栈空间地址，在该地址上new迭代器对象
 
#### 列迭代器结构
```c++
class IteratorImpl : public IteratorV<V> {
 public:
   // 构造函数和西沟函数

 protected:
    const std::vector<V> &list_;
    const int start_;
    const int end_;
    int pos_;
};
```

#### llvm栈上开辟迭代器空间
```c++
	
	// 获取列迭代器引用list_ref
    ::llvm::Type* list_ref_type = NULL;
    bool ok = GetLLVMListType(block_->getModule(), type, &list_ref_type);
    if (!ok) {
        LOG(WARNING) << "fail to get list type";
        return false;
    }

    // 获取列迭代器对象空间大小
    uint32_t col_iterator_size;
    ok = GetLLVMColumnIteratorSize(type, &col_iterator_size);
    if (!ok) {
        LOG(WARNING) << "fail to get col iterator size";
    }


    // 为列迭代器在栈上开辟空间alloca
    ::llvm::ArrayType* array_type =
        ::llvm::ArrayType::get(i8_ty, col_iterator_size);
    ::llvm::Value* col_iter = builder.CreateAlloca(array_type);
    // 为列迭代器引用开辟空间
    ::llvm::Value* list_ref = builder.CreateAlloca(list_ref_type);

    // 将列迭代器地址存入list_ref中
    ::llvm::Value* data_ptr_ptr =
        builder.CreateStructGEP(list_ref_type, list_ref, 0);
    data_ptr_ptr = builder.CreatePointerCast(
        data_ptr_ptr, col_iter->getType()->getPointerTo());
    builder.CreateStore(col_iter, data_ptr_ptr, false);
    col_iter = builder.CreatePointerCast(col_iter, i8_ptr_ty);

    // 获取列偏移、列元素类型等
    ::llvm::Value* val_offset = builder.getInt32(offset);
    ::llvm::Value* val_type_id = builder.getInt32(static_cast<int32_t>(type));

    // 调用相关函数，在指定地址上创建迭代器
    ::llvm::FunctionCallee callee = block_->getModule()->getOrInsertFunction(
        fn_name, i32_ty, i8_ptr_ty, i32_ty, i32_ty, i8_ptr_ty);
    builder.CreateCall(callee, ::llvm::ArrayRef<::llvm::Value*>{
                                   row_ptr, val_offset, val_type_id, col_iter});
    *output = list_ref;

```

#### 在指定地址上创建迭代器
```c++

int32_t GetCol(int8_t* input, int32_t offset, int32_t type_id, int8_t* data) {
    fesql::type::Type type = static_cast<fesql::type::Type>(type_id);
    if (nullptr == input || nullptr == data) {
        return -2;
    }
    WindowIteratorImpl* w = reinterpret_cast<WindowIteratorImpl*>(input);
    switch (type) {
        case fesql::type::kInt32: {
        	// 用data指引的这块地址空间new一个ColumnIteratorImpl对象
            new (data) ColumnIteratorImpl<int>(*w, offset);
            break;
        }
        // ... 省略
        default: {
            LOG(WARNING) << "cannot get col for type "
                         << ::fesql::type::Type_Name(type);
            return -2;
        }
    }
    return 0;
}
```

#### 迭代器的空间释放

由于列迭代器在函数栈上开辟，所以函数结束时，会自动释放这块空间。值得注意的是，空间释放时，并不会调用ColumnIteratorImpl的析构函数。因此，要特别注意，对象使用过程中，不会额外开辟堆空间。



