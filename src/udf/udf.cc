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
#include "base/slice.h"
#include "codec/list_iterator_codec.h"
#include "codec/type_codec.h"
#include "codegen/ir_base_builder.h"
#include "proto/type.pb.h"

namespace fesql {
namespace udf {
namespace v1 {
using fesql::codec::ColumnImpl;
using fesql::codec::IteratorRef;
using fesql::codec::IteratorV;
using fesql::codec::ListRef;
using fesql::codec::ListV;
using fesql::codec::Row;
using fesql::codec::StringColumnImpl;
using fesql::codec::StringRef;

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
    ::fesql::codec::ListRef *list_ref = (::fesql::codec::ListRef *)(input);
    ::fesql::codec::ListV<V> *col =
        (::fesql::codec::ListV<V> *)(list_ref->list);
    auto iter = col->GetIterator();
    iter->SeekToFirst();
    while (iter->Valid()) {
        result += iter->GetValue();
        iter->Next();
    }
    return result;
}

template <class V>
double avg_list(int8_t *input) {
    V result = 0;
    if (nullptr == input) {
        return result;
    }
    ::fesql::codec::ListRef *list_ref = (::fesql::codec::ListRef *)(input);
    ListV<V> *col = (ListV<V> *)(list_ref->list);
    auto iter = col->GetIterator();
    int32_t cnt = 0;
    while (iter->Valid()) {
        result += iter->GetValue();
        iter->Next();
        cnt++;
    }
    return static_cast<double>(result) / cnt;
}
template <class V>
int64_t count_list(int8_t *input) {
    if (nullptr == input) {
        return 0L;
    }

    ::fesql::codec::ListRef *list_ref = (::fesql::codec::ListRef *)(input);
    ListV<V> *col = (ListV<V> *)(list_ref->list);
    return int64_t(col->GetCount());
}

template <class V>
V max_list(int8_t *input) {
    V result = 0;
    if (nullptr == input) {
        return result;
    }
    ::fesql::codec::ListRef *list_ref = (::fesql::codec::ListRef *)(input);
    ListV<V> *col = (ListV<V> *)(list_ref->list);
    auto iter = col->GetIterator();

    if (iter->Valid()) {
        result = iter->GetValue();
        iter->Next();
    }
    while (iter->Valid()) {
        V v = iter->GetValue();
        iter->Next();
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
    ::fesql::codec::ListRef *list_ref = (::fesql::codec::ListRef *)(input);
    ListV<V> *col = (ListV<V> *)(list_ref->list);
    auto iter = col->GetIterator();

    if (iter->Valid()) {
        result = iter->GetValue();
        iter->Next();
    }
    while (iter->Valid()) {
        V v = iter->GetValue();
        iter->Next();
        if (v < result) {
            result = v;
        }
    }
    return result;
}

template <class V>
V at_list(int8_t *input, int32_t pos) {
    ::fesql::codec::ListRef *list_ref = (::fesql::codec::ListRef *)(input);
    ListV<V> *list = (ListV<V> *)(list_ref->list);
    return list->At(pos);
}

template <class V>
bool iterator_list(int8_t *input, int8_t *output) {
    if (nullptr == input || nullptr == output) {
        return false;
    }
    ::fesql::codec::ListRef *list_ref = (::fesql::codec::ListRef *)(input);
    ::fesql::codec::IteratorRef *iterator_ref =
        (::fesql::codec::IteratorRef *)(output);
    ListV<V> *col = (ListV<V> *)(list_ref->list);
    auto col_iter = col->GetIterator(nullptr);
    col_iter->SeekToFirst();
    iterator_ref->iterator = reinterpret_cast<int8_t *>(col_iter);
    return true;
}

template <class V>
bool has_next_iterator(int8_t *input) {
    if (nullptr == input) {
        return false;
    }
    ::fesql::codec::IteratorRef *iter_ref =
        (::fesql::codec::IteratorRef *)(input);
    IteratorV<uint64_t, V> *iter =
        (IteratorV<uint64_t, V> *)(iter_ref->iterator);
    return iter == nullptr ? false : iter->Valid();
}

template <class V>
V next_iterator(int8_t *input) {
    ::fesql::codec::IteratorRef *iter_ref =
        (::fesql::codec::IteratorRef *)(input);
    IteratorV<uint64_t, V> *iter =
        (IteratorV<uint64_t, V> *)(iter_ref->iterator);
    V v = iter->GetValue();
    iter->Next();
    return v;
}
template <class V>
void delete_iterator(int8_t *input) {
    ::fesql::codec::IteratorRef *iter_ref =
        (::fesql::codec::IteratorRef *)(input);
    IteratorV<uint64_t, V> *iter =
        (IteratorV<uint64_t, V> *)(iter_ref->iterator);
    if (iter) {
        delete iter;
    }
}

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

    AddSymbol(jd, mi, "count_list_int16",
              reinterpret_cast<void *>(&v1::count_list<int16_t>));
    AddSymbol(jd, mi, "count_list_int32",
              reinterpret_cast<void *>(&v1::count_list<int32_t>));
    AddSymbol(jd, mi, "count_list_int64",
              reinterpret_cast<void *>(&v1::count_list<int64_t>));
    AddSymbol(jd, mi, "count_list_double",
              reinterpret_cast<void *>(&v1::count_list<double>));
    AddSymbol(jd, mi, "count_list_float",
              reinterpret_cast<void *>(&v1::count_list<float>));
    AddSymbol(jd, mi, "count_list_row",
              reinterpret_cast<void *>(&v1::count_list<fesql::codec::Row>));

    AddSymbol(jd, mi, "avg_list_int16",
              reinterpret_cast<void *>(&v1::avg_list<int16_t>));
    AddSymbol(jd, mi, "avg_list_int32",
              reinterpret_cast<void *>(&v1::avg_list<int32_t>));
    AddSymbol(jd, mi, "avg_list_int64",
              reinterpret_cast<void *>(&v1::avg_list<int64_t>));
    AddSymbol(jd, mi, "avg_list_double",
              reinterpret_cast<void *>(&v1::avg_list<double>));
    AddSymbol(jd, mi, "avg_list_float",
              reinterpret_cast<void *>(&v1::avg_list<float>));

    AddSymbol(jd, mi, "max_list_int16",
              reinterpret_cast<void *>(&v1::max_list<int16_t>));
    AddSymbol(jd, mi, "max_list_int32",
              reinterpret_cast<void *>(&v1::max_list<int32_t>));
    AddSymbol(jd, mi, "max_list_int64",
              reinterpret_cast<void *>(&v1::max_list<int64_t>));
    AddSymbol(jd, mi, "max_list_float",
              reinterpret_cast<void *>(&v1::max_list<float>));
    AddSymbol(jd, mi, "max_list_double",
              reinterpret_cast<void *>(&v1::max_list<double>));

    AddSymbol(jd, mi, "min_list_int16",
              reinterpret_cast<void *>(&v1::min_list<int16_t>));
    AddSymbol(jd, mi, "min_list_int32",
              reinterpret_cast<void *>(&v1::min_list<int32_t>));
    AddSymbol(jd, mi, "min_list_int64",
              reinterpret_cast<void *>(&v1::min_list<int64_t>));
    AddSymbol(jd, mi, "min_list_float",
              reinterpret_cast<void *>(&v1::min_list<float>));
    AddSymbol(jd, mi, "min_list_double",
              reinterpret_cast<void *>(&v1::min_list<double>));

    AddSymbol(jd, mi, "at_list_int16",
              reinterpret_cast<void *>(&v1::at_list<int16_t>));
    AddSymbol(jd, mi, "at_list_int32",
              reinterpret_cast<void *>(&v1::at_list<int32_t>));
    AddSymbol(jd, mi, "at_list_int64",
              reinterpret_cast<void *>(&v1::at_list<int64_t>));
    AddSymbol(jd, mi, "at_list_float",
              reinterpret_cast<void *>(&v1::at_list<float>));
    AddSymbol(jd, mi, "at_list_double",
              reinterpret_cast<void *>(&v1::at_list<double>));

    AddSymbol(jd, mi, "iterator_list_int16",
              reinterpret_cast<void *>(&v1::iterator_list<int16_t>));
    AddSymbol(jd, mi, "iterator_list_int32",
              reinterpret_cast<void *>(&v1::iterator_list<int32_t>));
    AddSymbol(jd, mi, "iterator_list_int64",
              reinterpret_cast<void *>(&v1::iterator_list<int64_t>));
    AddSymbol(jd, mi, "iterator_list_float",
              reinterpret_cast<void *>(&v1::iterator_list<float>));
    AddSymbol(jd, mi, "iterator_list_double",
              reinterpret_cast<void *>(&v1::iterator_list<double>));

    AddSymbol(jd, mi, "has_next_iterator_int16",
              reinterpret_cast<void *>(&v1::has_next_iterator<int16_t>));
    AddSymbol(jd, mi, "has_next_iterator_int32",
              reinterpret_cast<void *>(&v1::has_next_iterator<int32_t>));
    AddSymbol(jd, mi, "has_next_iterator_int64",
              reinterpret_cast<void *>(&v1::has_next_iterator<int64_t>));
    AddSymbol(jd, mi, "has_next_iterator_float",
              reinterpret_cast<void *>(&v1::has_next_iterator<float>));
    AddSymbol(jd, mi, "has_next_iterator_double",
              reinterpret_cast<void *>(&v1::has_next_iterator<double>));

    AddSymbol(jd, mi, "next_iterator_int16",
              reinterpret_cast<void *>(&v1::next_iterator<int16_t>));
    AddSymbol(jd, mi, "next_iterator_int32",
              reinterpret_cast<void *>(&v1::next_iterator<int32_t>));
    AddSymbol(jd, mi, "next_iterator_int64",
              reinterpret_cast<void *>(&v1::next_iterator<int64_t>));
    AddSymbol(jd, mi, "next_iterator_float",
              reinterpret_cast<void *>(&v1::next_iterator<float>));
    AddSymbol(jd, mi, "next_iterator_double",
              reinterpret_cast<void *>(&v1::next_iterator<double>));

    AddSymbol(jd, mi, "delete_iterator_int16",
              reinterpret_cast<void *>(&v1::delete_iterator<int16_t>));
    AddSymbol(jd, mi, "delete_iterator_int32",
              reinterpret_cast<void *>(&v1::delete_iterator<int32_t>));
    AddSymbol(jd, mi, "delete_iterator_int64",
              reinterpret_cast<void *>(&v1::delete_iterator<int64_t>));
    AddSymbol(jd, mi, "delete_iterator_float",
              reinterpret_cast<void *>(&v1::delete_iterator<float>));
    AddSymbol(jd, mi, "delete_iterator_double",
              reinterpret_cast<void *>(&v1::delete_iterator<double>));
}
bool AddSymbol(::llvm::orc::JITDylib &jd,           // NOLINT
               ::llvm::orc::MangleAndInterner &mi,  // NOLINT
               const std::string &fn_name, void *fn_ptr) {
    return ::fesql::vm::FeSQLJIT::AddSymbol(jd, mi, fn_name, fn_ptr);
}

void RegisterUDFToModule(::llvm::Module *m) {
    ::llvm::Type *v_ty = ::llvm::Type::getVoidTy(m->getContext());
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
            "count_" + node::DataTypeName(fesql::node::kList) + "_";

        for (auto type : number_types) {
            ::llvm::Type *llvm_type;
            ::fesql::codegen::GetLLVMListType(m, type.first, &llvm_type);
            m->getOrInsertFunction(prefix + node::DataTypeName(type.first),
                                   i64_ty, llvm_type->getPointerTo());
        }
    }

    {
        std::string prefix =
            "avg_" + node::DataTypeName(fesql::node::kList) + "_";

        for (auto type : number_types) {
            ::llvm::Type *llvm_type;
            ::fesql::codegen::GetLLVMListType(m, type.first, &llvm_type);
            m->getOrInsertFunction(prefix + node::DataTypeName(type.first),
                                   double_ty, llvm_type->getPointerTo());
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
            "delete_" + node::DataTypeName(fesql::node::kIterator) + "_";
        for (auto type : number_types) {
            ::llvm::Type *llvm_type;
            ::fesql::codegen::GetLLVMIteratorType(m, type.first, &llvm_type);
            m->getOrInsertFunction(prefix + (node::DataTypeName(type.first)),
                                   v_ty, llvm_type->getPointerTo());
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
    AddSymbol(jd, mi, "fmod",
              (reinterpret_cast<void *>(
                  static_cast<double (*)(double, double)>(&fmod))));
    AddSymbol(jd, mi, "fmodf", (reinterpret_cast<void *>(&fmodf)));
}
void InitCLibSymbol(vm::FeSQLJIT *jit_ptr) {  // NOLINT
    ::llvm::orc::MangleAndInterner mi(jit_ptr->getExecutionSession(),
                                      jit_ptr->getDataLayout());
    InitCLibSymbol(jit_ptr->getMainJITDylib(), mi);
}
}  // namespace udf
}  // namespace fesql
