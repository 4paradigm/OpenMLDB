/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * udf.cc
 *
 * Author: chenjing
 * Date: 2019/11/26
 *--------------------------------------------------------------------------
 **/
#include "udf/udf.h"
#include "jit/jit.h"
#include <stdint.h>
#include "proto/type.pb.h"
#include "storage/type_ir_builder.h"
#include "storage/window.h"
namespace fesql {
namespace udf {
namespace v1 {
using fesql::storage::ColumnIteratorImpl;
using fesql::storage::ColumnStringIteratorImpl;
using fesql::storage::Row;
using fesql::storage::WindowIteratorImpl;
template <class V>
int32_t current_time() {
    return 5;
}

template <class V>
inline V inc(V i) {
    return i + 1;
}
int32_t inc_int32(int32_t i) { return inc<int32_t>(i); }

template <class V>
V sum(int8_t *input) {
    V result = 0;
    if (nullptr == input) {
        return result;
    }
    ::fesql::storage::ListRef *list_ref =
        reinterpret_cast<::fesql::storage::ListRef *>(input);
    if (nullptr == list_ref->iterator) {
        return result;
    }
    ColumnIteratorImpl<V> *col = (ColumnIteratorImpl<V> *)(list_ref->iterator);
    while (col->Valid()) {
        result += col->Next();
    }
    return result;
}

int16_t sum_int16(int8_t *input) { return sum<int16_t>(input); }
int32_t sum_int32(int8_t *input) { return sum<int32_t>(input); }
int64_t sum_int64(int8_t *input) { return sum<int64_t>(input); }
float sum_float(int8_t *input) { return sum<float>(input); }
double sum_double(int8_t *input) { return sum<double>(input); }

}  // namespace v1
void InitUDFSymbol(vm::FeSQLJIT *jit_ptr) {
    ::llvm::orc::MangleAndInterner mi(jit_ptr->getExecutionSession(),
                                      jit_ptr->getDataLayout());
    InitUDFSymbol(jit_ptr->getMainJITDylib(), mi);
}  // NOLINT
void InitUDFSymbol(::llvm::orc::JITDylib &jd,             // NOLINT
                   ::llvm::orc::MangleAndInterner &mi) {  // NOLINT
    // decode
    AddSymbol(jd, mi, "inc_int32", reinterpret_cast<void *>(&v1::inc_int32));
    AddSymbol(jd, mi, "sum_int16", reinterpret_cast<void *>(&v1::sum_int16));
    AddSymbol(jd, mi, "sum_int32", reinterpret_cast<void *>(&v1::sum_int32));
    AddSymbol(jd, mi, "sum_int64", reinterpret_cast<void *>(&v1::sum_int64));
    AddSymbol(jd, mi, "sum_double", reinterpret_cast<void *>(&v1::sum_double));
    AddSymbol(jd, mi, "sum_float", reinterpret_cast<void *>(&v1::sum_float));
}
bool AddSymbol(::llvm::orc::JITDylib &jd, ::llvm::orc::MangleAndInterner &mi,
               const std::string &fn_name, void *fn_ptr) {
    return ::fesql::vm::FeSQLJIT::AddSymbol(jd, mi, fn_name, fn_ptr);
}

void RegisterUDFToModule(::llvm::Module *m) {
    ::llvm::Type *i16_ty = ::llvm::Type::getInt16Ty(m->getContext());
    ::llvm::Type *i32_ty = ::llvm::Type::getInt32Ty(m->getContext());
    ::llvm::Type *i64_ty = ::llvm::Type::getInt64Ty(m->getContext());
    ::llvm::Type *float_ty = ::llvm::Type::getFloatTy(m->getContext());
    ::llvm::Type *double_ty = ::llvm::Type::getDoubleTy(m->getContext());
    ::llvm::Type *i8_ptr_ty = ::llvm::Type::getInt8PtrTy(m->getContext());
    m->getOrInsertFunction("inc_int32", i32_ty, i32_ty);
    m->getOrInsertFunction("sum_int16", i16_ty, i8_ptr_ty);
    m->getOrInsertFunction("sum_int32", i32_ty, i8_ptr_ty);
    m->getOrInsertFunction("sum_int64", i64_ty, i8_ptr_ty);
    m->getOrInsertFunction("sum_float", float_ty, i8_ptr_ty);
    m->getOrInsertFunction("sum_double", double_ty, i8_ptr_ty);
}
}  // namespace udf
}  // namespace fesql
