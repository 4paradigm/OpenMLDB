/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * udf.cc
 *
 * Author: chenjing
 * Date: 2019/11/26
 *--------------------------------------------------------------------------
 **/
#include "udf/udf.h"
#include <stdint.h>
#include <algorithm>
#include <vector>
#include "proto/type.pb.h"
#include "storage/type_ir_builder.h"
#include "storage/window.h"
namespace fesql {
namespace udf {
namespace v1 {
using fesql::storage::ColumnImpl;
using fesql::storage::IteratorImpl;
using fesql::storage::Row;
using fesql::storage::StringColumnImpl;
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
    ::fesql::storage::ListRef *list_ref = (::fesql::storage::ListRef *)(input);
    ::fesql::storage::ListV<V> *col =
        (::fesql::storage::ListV<V> *)(list_ref->list);
    IteratorImpl<V> iter(*col);
    while (iter.Valid()) {
        result += iter.Next();
    }
    return result;
}

template <class V>
V max(int8_t *input) {
    V result = 0;
    if (nullptr == input) {
        return result;
    }
    ::fesql::storage::ListRef *list_ref = (::fesql::storage::ListRef *)(input);
    ::fesql::storage::ListV<V> *col =
        (::fesql::storage::ListV<V> *)(list_ref->list);
    IteratorImpl<V> iter(*col);

    if (iter.Valid()) {
        result = iter.Next();
    }
    while (iter.Valid()) {
        V v = iter.Next();
        if (v > result) {
            result = v;
        }
    }
    return result;
}

template <class V>
V min(int8_t *input) {
    V result = 0;
    if (nullptr == input) {
        return result;
    }
    ::fesql::storage::ListRef *list_ref = (::fesql::storage::ListRef *)(input);
    ::fesql::storage::ListV<V> *col =
        (::fesql::storage::ListV<V> *)(list_ref->list);
    IteratorImpl<V> iter(*col);

    if (iter.Valid()) {
        result = iter.Next();
    }
    while (iter.Valid()) {
        V v = iter.Next();
        if (v < result) {
            result = v;
        }
    }
    return result;
}

template <class V>
V list_at(int8_t *input, int32_t pos) {
    ::fesql::storage::ListRef *list_ref = (::fesql::storage::ListRef *)(input);
    ::fesql::storage::ListV<V> *list =
        (::fesql::storage::ListV<V> *)(list_ref->list);
    return list->At(pos);
}

int16_t list_at_int16(int8_t *input, int32_t pos) {
    return list_at<int16_t>(input, pos);
}
int16_t list_at_int32(int8_t *input, int32_t pos) {
    return list_at<int32_t>(input, pos);
}
int16_t list_at_int64(int8_t *input, int32_t pos) {
    return list_at<int64_t>(input, pos);
}
int16_t list_at_float(int8_t *input, int32_t pos) {
    return list_at<float>(input, pos);
}
int16_t list_at_double(int8_t *input, int32_t pos) {
    return list_at<double>(input, pos);
}


int16_t sum_int16(int8_t *input) { return sum<int16_t>(input); }
int32_t sum_int32(int8_t *input) { return sum<int32_t>(input); }
int64_t sum_int64(int8_t *input) { return sum<int64_t>(input); }
float sum_float(int8_t *input) { return sum<float>(input); }
double sum_double(int8_t *input) { return sum<double>(input); }

int16_t max_int16(int8_t *input) { return max<int16_t>(input); }
int32_t max_int32(int8_t *input) { return max<int32_t>(input); }
int64_t max_int64(int8_t *input) { return max<int64_t>(input); }
float max_float(int8_t *input) { return max<float>(input); }
double max_double(int8_t *input) { return max<double>(input); }

int16_t min_int16(int8_t *input) { return min<int16_t>(input); }
int32_t min_int32(int8_t *input) { return min<int32_t>(input); }
int64_t min_int64(int8_t *input) { return min<int64_t>(input); }
float min_float(int8_t *input) { return min<float>(input); }
double min_double(int8_t *input) { return min<double>(input); }

}  // namespace v1
void InitUDFSymbol(vm::FeSQLJIT *jit_ptr) {
    ::llvm::orc::MangleAndInterner mi(jit_ptr->getExecutionSession(),
                                      jit_ptr->getDataLayout());
    InitUDFSymbol(jit_ptr->getMainJITDylib(), mi);
}  // NOLINT
void InitUDFSymbol(::llvm::orc::JITDylib &jd,             // NOLINT
                   ::llvm::orc::MangleAndInterner &mi) {  // NOLINT
    AddSymbol(jd, mi, "inc_int32", reinterpret_cast<void *>(&v1::inc_int32));
    AddSymbol(jd, mi, "sum_int16", reinterpret_cast<void *>(&v1::sum_int16));
    AddSymbol(jd, mi, "sum_int32", reinterpret_cast<void *>(&v1::sum_int32));
    AddSymbol(jd, mi, "sum_int64", reinterpret_cast<void *>(&v1::sum_int64));
    AddSymbol(jd, mi, "sum_double", reinterpret_cast<void *>(&v1::sum_double));
    AddSymbol(jd, mi, "sum_float", reinterpret_cast<void *>(&v1::sum_float));

    AddSymbol(jd, mi, "max_int16", reinterpret_cast<void *>(&v1::max_int16));
    AddSymbol(jd, mi, "max_int32", reinterpret_cast<void *>(&v1::max_int32));
    AddSymbol(jd, mi, "max_int64", reinterpret_cast<void *>(&v1::max_int64));
    AddSymbol(jd, mi, "max_double", reinterpret_cast<void *>(&v1::max_double));
    AddSymbol(jd, mi, "max_float", reinterpret_cast<void *>(&v1::max_float));

    AddSymbol(jd, mi, "min_int16", reinterpret_cast<void *>(&v1::min_int16));
    AddSymbol(jd, mi, "min_int32", reinterpret_cast<void *>(&v1::min_int32));
    AddSymbol(jd, mi, "min_int64", reinterpret_cast<void *>(&v1::min_int64));
    AddSymbol(jd, mi, "min_double", reinterpret_cast<void *>(&v1::min_double));
    AddSymbol(jd, mi, "min_float", reinterpret_cast<void *>(&v1::min_float));

    AddSymbol(jd, mi, "list_at_int16",
              reinterpret_cast<void *>(&v1::list_at_int16));
    AddSymbol(jd, mi, "list_at_int32",
              reinterpret_cast<void *>(&v1::list_at_int32));
    AddSymbol(jd, mi, "list_at_int64",
              reinterpret_cast<void *>(&v1::list_at_int64));
    AddSymbol(jd, mi, "list_at_float",
              reinterpret_cast<void *>(&v1::list_at_float));
    AddSymbol(jd, mi, "list_at_double",
              reinterpret_cast<void *>(&v1::list_at_double));
}
bool AddSymbol(::llvm::orc::JITDylib &jd,           // NOLINT
               ::llvm::orc::MangleAndInterner &mi,  // NOLINT
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

    m->getOrInsertFunction("max_int16", i16_ty, i8_ptr_ty);
    m->getOrInsertFunction("max_int32", i32_ty, i8_ptr_ty);
    m->getOrInsertFunction("max_int64", i64_ty, i8_ptr_ty);
    m->getOrInsertFunction("max_float", float_ty, i8_ptr_ty);
    m->getOrInsertFunction("max_double", double_ty, i8_ptr_ty);

    m->getOrInsertFunction("min_int16", i16_ty, i8_ptr_ty);
    m->getOrInsertFunction("min_int32", i32_ty, i8_ptr_ty);
    m->getOrInsertFunction("min_int64", i64_ty, i8_ptr_ty);
    m->getOrInsertFunction("min_float", float_ty, i8_ptr_ty);
    m->getOrInsertFunction("min_double", double_ty, i8_ptr_ty);

    m->getOrInsertFunction("list_at_int16", i16_ty, i8_ptr_ty, i32_ty);
    m->getOrInsertFunction("list_at_int32", i32_ty, i8_ptr_ty, i32_ty);
    m->getOrInsertFunction("list_at_int64", i64_ty, i8_ptr_ty, i32_ty);
    m->getOrInsertFunction("list_at_float", float_ty, i8_ptr_ty, i32_ty);
    m->getOrInsertFunction("list_at_double", double_ty, i8_ptr_ty, i32_ty);
}
void InitCLibSymbol(::llvm::orc::JITDylib &jd,             // NOLINT
                    ::llvm::orc::MangleAndInterner &mi) {  // NOLINT
    AddSymbol(jd, mi, "fmod", (reinterpret_cast<void *>(&fmod)));
    AddSymbol(jd, mi, "fmodf", (reinterpret_cast<void *>(&fmodf)));
}
void InitCLibSymbol(vm::FeSQLJIT *jit_ptr) {  // NOLINT
    ::llvm::orc::MangleAndInterner mi(jit_ptr->getExecutionSession(),
                                      jit_ptr->getDataLayout());
    InitCLibSymbol(jit_ptr->getMainJITDylib(), mi);
}
}  // namespace udf
}  // namespace fesql
