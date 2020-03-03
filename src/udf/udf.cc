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
#include <utility>
#include <vector>
#include "codegen/ir_base_builder.h"
#include "proto/type.pb.h"
#include "storage/type_ir_builder.h"
#include "storage/window.h"
namespace fesql {
namespace udf {
namespace v1 {
using fesql::storage::ColumnImpl;
using fesql::storage::IteratorImpl;
using fesql::storage::IteratorRef;
using fesql::storage::ListRef;
using fesql::storage::ListV;
using fesql::storage::Row;
using fesql::storage::StringColumnImpl;
using fesql::storage::StringRef;
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
V sum_list(int8_t *input) {
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
V max_list(int8_t *input) {
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
V min_list(int8_t *input) {
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
V at_list(int8_t *input, int32_t pos) {
    ::fesql::storage::ListRef *list_ref = (::fesql::storage::ListRef *)(input);
    ::fesql::storage::ListV<V> *list =
        (::fesql::storage::ListV<V> *)(list_ref->list);
    return list->At(pos);
}

int16_t at_list_int16(int8_t *input, int32_t pos) {
    return at_list<int16_t>(input, pos);
}

int32_t at_list_int32(int8_t *input, int32_t pos) {
    return at_list<int32_t>(input, pos);
}

int64_t at_list_int64(int8_t *input, int32_t pos) {
    return at_list<int64_t>(input, pos);
}

float at_list_float(int8_t *input, int32_t pos) {
    return at_list<float>(input, pos);
}

double at_list_double(int8_t *input, int32_t pos) {
    return at_list<double>(input, pos);
}

template <class V>
bool iterator_list(int8_t *input, int8_t *output) {
    if (nullptr == input || nullptr == output) {
        return false;
    }
    ::fesql::storage::ListRef *list_ref = (::fesql::storage::ListRef *)(input);
    ::fesql::storage::IteratorRef *iterator_ref =
        (::fesql::storage::IteratorRef *)(output);
    ::fesql::storage::ListV<V> *col =
        (::fesql::storage::ListV<V> *)(list_ref->list);
    ::fesql::storage::IteratorImpl<V> *iter =
        (::fesql::storage::IteratorImpl<V> *)(iterator_ref->iterator);
    new (iter) IteratorImpl<V>(*col);
    return true;
}
bool iterator_list_int16(int8_t *input, int8_t *output) {
    return iterator_list<int16_t>(input, output);
}
bool iterator_list_int32(int8_t *input, int8_t *output) {
    return iterator_list<int32_t>(input, output);
}
bool iterator_list_int64(int8_t *input, int8_t *output) {
    return iterator_list<int64_t>(input, output);
}
bool iterator_list_float(int8_t *input, int8_t *output) {
    return iterator_list<float>(input, output);
}
bool iterator_list_double(int8_t *input, int8_t *output) {
    return iterator_list<double>(input, output);
}

template <class V>
bool has_next_iterator(int8_t *input) {
    if (nullptr == input) {
        return false;
    }
    ::fesql::storage::IteratorRef *iter_ref =
        (::fesql::storage::IteratorRef *)(input);
    ::fesql::storage::IteratorImpl<V> *iter =
        (::fesql::storage::IteratorImpl<V> *)(iter_ref->iterator);
    return iter == nullptr ? false : iter->Valid();
}
bool has_next_iterator_int16(int8_t *input) {
    return has_next_iterator<int16_t>(input);
}
bool has_next_iterator_float(int8_t *input) {
    return has_next_iterator<float>(input);
}
bool has_next_iterator_double(int8_t *input) {
    return has_next_iterator<double>(input);
}
bool has_next_iterator_int32(int8_t *input) {
    return has_next_iterator<int32_t>(input);
}
bool has_next_iterator_int64(int8_t *input) {
    return has_next_iterator<int64_t>(input);
}

template <class V>
V next_iterator(int8_t *input, V default_value) {
    if (nullptr == input) {
        return default_value;
    }
    ::fesql::storage::IteratorRef *iter_ref =
        (::fesql::storage::IteratorRef *)(input);
    if (nullptr == iter_ref) {
        return default_value;
    }
    ::fesql::storage::IteratorImpl<V> *iter =
        (::fesql::storage::IteratorImpl<V> *)(iter_ref->iterator);
    if (nullptr == iter) {
        return default_value;
    }
    return iter->Next();
}
int16_t next_iterator_int16(int8_t *input) {
    return next_iterator<int16_t>(input, 0u);
}
float next_iterator_float(int8_t *input) {
    return next_iterator<float>(input, 0.0f);
}
double next_iterator_double(int8_t *input) {
    return next_iterator<double>(input, 0.0);
}
int32_t next_iterator_int32(int8_t *input) {
    return next_iterator<int32_t>(input, 0);
}
int64_t next_iterator_int64(int8_t *input) {
    return next_iterator<int64_t>(input, 0L);
}

int16_t sum_list_int16(int8_t *input) { return sum_list<int16_t>(input); }
int32_t sum_list_int32(int8_t *input) { return sum_list<int32_t>(input); }
int64_t sum_list_int64(int8_t *input) { return sum_list<int64_t>(input); }
float sum_list_float(int8_t *input) { return sum_list<float>(input); }
double sum_list_double(int8_t *input) { return sum_list<double>(input); }

int16_t max_list_int16(int8_t *input) { return max_list<int16_t>(input); }
int32_t max_list_int32(int8_t *input) { return max_list<int32_t>(input); }
int64_t max_list_int64(int8_t *input) { return max_list<int64_t>(input); }
float max_list_float(int8_t *input) { return max_list<float>(input); }
double max_list_double(int8_t *input) { return max_list<double>(input); }

int16_t min_list_int16(int8_t *input) { return min_list<int16_t>(input); }
int32_t min_list_int32(int8_t *input) { return min_list<int32_t>(input); }
int64_t min_list_int64(int8_t *input) { return min_list<int64_t>(input); }
float min_list_float(int8_t *input) { return min_list<float>(input); }
double min_list_double(int8_t *input) { return min_list<double>(input); }

}  // namespace v1
void InitUDFSymbol(vm::FeSQLJIT *jit_ptr) {
    ::llvm::orc::MangleAndInterner mi(jit_ptr->getExecutionSession(),
                                      jit_ptr->getDataLayout());
    InitUDFSymbol(jit_ptr->getMainJITDylib(), mi);
}  // NOLINT
void InitUDFSymbol(::llvm::orc::JITDylib &jd,             // NOLINT
                   ::llvm::orc::MangleAndInterner &mi) {  // NOLINT
    AddSymbol(jd, mi, "inc_int32", reinterpret_cast<void *>(&v1::inc_int32));
    AddSymbol(jd, mi, "sum_list_int16",
              reinterpret_cast<void *>(&v1::sum_list<int16_t>));
    AddSymbol(jd, mi, "sum_list_int32",
              reinterpret_cast<void *>(&v1::sum_list<int32_t>));
    AddSymbol(jd, mi, "sum_list_int64",
              reinterpret_cast<void *>(&v1::sum_list<int64_t>));
    AddSymbol(jd, mi, "sum_list_double",
              reinterpret_cast<void *>(&v1::sum_list<double>));
    AddSymbol(jd, mi, "sum_list_float",
              reinterpret_cast<void *>(&v1::sum_list<float>));

    AddSymbol(jd, mi, "max_list_int16",
              reinterpret_cast<void *>(&v1::max_list_int16));
    AddSymbol(jd, mi, "max_list_int32",
              reinterpret_cast<void *>(&v1::max_list_int32));
    AddSymbol(jd, mi, "max_list_int64",
              reinterpret_cast<void *>(&v1::max_list_int64));
    AddSymbol(jd, mi, "max_list_float",
              reinterpret_cast<void *>(&v1::max_list_float));
    AddSymbol(jd, mi, "max_list_double",
              reinterpret_cast<void *>(&v1::max_list_double));

    AddSymbol(jd, mi, "min_list_int16",
              reinterpret_cast<void *>(&v1::min_list_int16));
    AddSymbol(jd, mi, "min_list_int32",
              reinterpret_cast<void *>(&v1::min_list_int32));
    AddSymbol(jd, mi, "min_list_int64",
              reinterpret_cast<void *>(&v1::min_list_int64));
    AddSymbol(jd, mi, "min_list_float",
              reinterpret_cast<void *>(&v1::min_list_float));
    AddSymbol(jd, mi, "min_list_double",
              reinterpret_cast<void *>(&v1::min_list_double));

    AddSymbol(jd, mi, "at_list_int16",
              reinterpret_cast<void *>(&v1::at_list_int16));
    AddSymbol(jd, mi, "at_list_int32",
              reinterpret_cast<void *>(&v1::at_list_int32));
    AddSymbol(jd, mi, "at_list_int64",
              reinterpret_cast<void *>(&v1::at_list_int64));
    AddSymbol(jd, mi, "at_list_float",
              reinterpret_cast<void *>(&v1::at_list_float));
    AddSymbol(jd, mi, "at_list_double",
              reinterpret_cast<void *>(&v1::at_list_double));

    AddSymbol(jd, mi, "iterator_list_int16",
              reinterpret_cast<void *>(&v1::iterator_list_int16));
    AddSymbol(jd, mi, "iterator_list_int32",
              reinterpret_cast<void *>(&v1::iterator_list_int32));
    AddSymbol(jd, mi, "iterator_list_int64",
              reinterpret_cast<void *>(&v1::iterator_list_int64));
    AddSymbol(jd, mi, "iterator_list_float",
              reinterpret_cast<void *>(&v1::iterator_list_float));
    AddSymbol(jd, mi, "iterator_list_double",
              reinterpret_cast<void *>(&v1::iterator_list_double));

    AddSymbol(jd, mi, "has_next_iterator_int16",
              reinterpret_cast<void *>(&v1::has_next_iterator_int16));
    AddSymbol(jd, mi, "has_next_iterator_int32",
              reinterpret_cast<void *>(&v1::has_next_iterator_int32));
    AddSymbol(jd, mi, "has_next_iterator_int64",
              reinterpret_cast<void *>(&v1::has_next_iterator_int64));
    AddSymbol(jd, mi, "has_next_iterator_float",
              reinterpret_cast<void *>(&v1::has_next_iterator_float));
    AddSymbol(jd, mi, "has_next_iterator_double",
              reinterpret_cast<void *>(&v1::has_next_iterator_double));

    AddSymbol(jd, mi, "next_iterator_int16",
              reinterpret_cast<void *>(&v1::next_iterator_int16));
    AddSymbol(jd, mi, "next_iterator_int32",
              reinterpret_cast<void *>(&v1::next_iterator_int32));
    AddSymbol(jd, mi, "next_iterator_int64",
              reinterpret_cast<void *>(&v1::next_iterator_int64));
    AddSymbol(jd, mi, "next_iterator_float",
              reinterpret_cast<void *>(&v1::next_iterator_float));
    AddSymbol(jd, mi, "next_iterator_double",
              reinterpret_cast<void *>(&v1::next_iterator_double));
}
bool AddSymbol(::llvm::orc::JITDylib &jd,           // NOLINT
               ::llvm::orc::MangleAndInterner &mi,  // NOLINT
               const std::string &fn_name, void *fn_ptr) {
    return ::fesql::vm::FeSQLJIT::AddSymbol(jd, mi, fn_name, fn_ptr);
}

void RegisterUDFToModule(::llvm::Module *m) {
    ::llvm::Type *i1_ty = ::llvm::Type::getInt1Ty(m->getContext());
    ::llvm::Type *i16_ty = ::llvm::Type::getInt16Ty(m->getContext());
    ::llvm::Type *i32_ty = ::llvm::Type::getInt32Ty(m->getContext());
    ::llvm::Type *i64_ty = ::llvm::Type::getInt64Ty(m->getContext());
    ::llvm::Type *float_ty = ::llvm::Type::getFloatTy(m->getContext());
    ::llvm::Type *double_ty = ::llvm::Type::getDoubleTy(m->getContext());
    ::llvm::Type *i8_ptr_ty = ::llvm::Type::getInt8PtrTy(m->getContext());

    std::vector<std::pair<fesql::node::DataType, ::llvm::Type *>> number_types;
    number_types.push_back(std::make_pair(fesql::node::kInt16, i16_ty));
    number_types.push_back(std::make_pair(fesql::node::kInt32, i32_ty));
    number_types.push_back(std::make_pair(fesql::node::kInt64, i64_ty));
    number_types.push_back(std::make_pair(fesql::node::kFloat, float_ty));
    number_types.push_back(std::make_pair(fesql::node::kDouble, double_ty));

    m->getOrInsertFunction("inc_int32", i32_ty, i32_ty);

    {
        std::string prefix =
            "sum_" + node::DataTypeName(fesql::node::kList) + "_";

        for (auto type : number_types) {
            ::llvm::Type *llvm_type;
            ::fesql::codegen::GetLLVMListType(m, type.first, &llvm_type);
            m->getOrInsertFunction(prefix + node::DataTypeName(type.first),
                                   type.second, llvm_type->getPointerTo());
        }
    }
    {
        std::string prefix =
            "min_" + node::DataTypeName(fesql::node::kList) + "_";
        for (auto type : number_types) {
            ::llvm::Type *llvm_type;
            ::fesql::codegen::GetLLVMListType(m, type.first, &llvm_type);
            m->getOrInsertFunction(prefix + node::DataTypeName(type.first),
                                   type.second, llvm_type->getPointerTo());
        }
    }

    {
        std::string prefix =
            "max_" + node::DataTypeName(fesql::node::kList) + "_";
        for (auto type : number_types) {
            ::llvm::Type *llvm_type;
            ::fesql::codegen::GetLLVMListType(m, type.first, &llvm_type);
            m->getOrInsertFunction(prefix + node::DataTypeName(type.first),
                                   type.second, llvm_type->getPointerTo());
        }
    }

    {
        std::string prefix =
            "at_" + node::DataTypeName(fesql::node::kList) + "_";
        for (auto type : number_types) {
            ::llvm::Type *llvm_type;
            ::fesql::codegen::GetLLVMListType(m, type.first, &llvm_type);
            m->getOrInsertFunction(prefix + node::DataTypeName(type.first),
                                   type.second, llvm_type->getPointerTo(),
                                   i32_ty);
        }
    }

    {
        std::string prefix =
            "iterator_" + node::DataTypeName(fesql::node::kList) + "_";
        for (auto type : number_types) {
            ::llvm::Type *llvm_type;
            ::fesql::codegen::GetLLVMListType(m, type.first, &llvm_type);
            m->getOrInsertFunction(prefix + (node::DataTypeName(type.first)),
                                   i1_ty, llvm_type->getPointerTo(), i8_ptr_ty);
        }
    }
    {
        std::string prefix =
            "next_" + node::DataTypeName(fesql::node::kIterator) + "_";
        for (auto type : number_types) {
            ::llvm::Type *llvm_type;
            ::fesql::codegen::GetLLVMIteratorType(m, type.first, &llvm_type);
            m->getOrInsertFunction(prefix + (node::DataTypeName(type.first)),
                                   type.second, llvm_type->getPointerTo());
        }
    }
    {
        std::string prefix =
            "has_next_" + node::DataTypeName(fesql::node::kIterator) + "_";
        for (auto type : number_types) {
            ::llvm::Type *llvm_type;
            ::fesql::codegen::GetLLVMIteratorType(m, type.first, &llvm_type);
            m->getOrInsertFunction(prefix + (node::DataTypeName(type.first)),
                                   i1_ty, llvm_type->getPointerTo());
        }
    }
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
