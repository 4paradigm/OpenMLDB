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
#include <time.h>
#include <map>
#include <set>
#include <utility>
#include "absl/time/civil_time.h"
#include "absl/time/time.h"
#include "base/iterator.h"
#include "boost/date_time.hpp"
#include "codec/list_iterator_codec.h"
#include "codec/type_codec.h"
#include "codegen/udf_ir_builder.h"
#include "node/node_manager.h"
#include "node/sql_node.h"

namespace fesql {
namespace udf {
namespace v1 {
using fesql::base::ConstIterator;
using fesql::codec::ColumnImpl;
using fesql::codec::IteratorRef;
using fesql::codec::ListRef;
using fesql::codec::ListV;
using fesql::codec::Row;
using fesql::codec::StringColumnImpl;
using fesql::codec::StringRef;
const int32_t TZ = 8;
const time_t TZ_OFFSET = TZ * 3600000;
template <class V>
int32_t current_time() {
    return 5;
}

template <class V>
inline V inc(V i) {
    return i + 1;
}
int32_t dayofmonth(int64_t ts) {
    time_t time = (ts + TZ_OFFSET) / 1000;
    struct tm t;
    gmtime_r(&time, &t);
    return t.tm_mday;
}
int32_t dayofweek(int64_t ts) {
    time_t time = (ts + TZ_OFFSET) / 1000;
    struct tm t;
    gmtime_r(&time, &t);
    return t.tm_wday + 1;
}
int32_t week(int64_t ts) {
    time_t time = (ts + TZ_OFFSET) / 1000;
    struct tm t;
    gmtime_r(&time, &t);
    int32_t wday_of_first_day = (t.tm_wday + 7 - (t.tm_yday) % 7) % 7;
    int32_t days = t.tm_yday - (7 - wday_of_first_day) % 7 + 1;
    return ceil(days / 7.0);
}
int32_t month(int64_t ts) {
    time_t time = (ts + TZ_OFFSET) / 1000;
    struct tm t;
    gmtime_r(&time, &t);
    return t.tm_mon + 1;
}
int32_t year(int64_t ts) {
    time_t time = (ts + TZ_OFFSET) / 1000;
    struct tm t;
    gmtime_r(&time, &t);
    return t.tm_year + 1900;
}

int32_t dayofmonth(codec::Timestamp *ts) { return dayofmonth(ts->ts_); }
int32_t week(codec::Timestamp *ts) { return week(ts->ts_); }
int32_t month(codec::Timestamp *ts) { return month(ts->ts_); }
int32_t year(codec::Timestamp *ts) { return year(ts->ts_); }
int32_t dayofweek(codec::Timestamp *ts) { return dayofweek(ts->ts_); }
int32_t dayofweek(codec::Date *date) {
    int32_t day, month, year;
    if (!codec::Date::Decode(date->date_, &year, &month, &day)) {
        return 0;
    }
    boost::gregorian::date d(year, month, day);
    return d.day_of_week() + 1;
}
int32_t week(codec::Date *date) {
    int32_t day, month, year;
    if (!codec::Date::Decode(date->date_, &year, &month, &day)) {
        return 0;
    }
    boost::gregorian::date d(year, month, day);
    int32_t day_of_year = d.day_of_year();
    int32_t wday_of_first_day = (d.day_of_week() + 7 - day_of_year % 7) % 7;
    int32_t days = day_of_year - (7 - wday_of_first_day) % 7 + 1;
    return ceil(days / 7.0);
}

template <class V>
V sum_list(int8_t *input) {
    V result = V();
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
bool sum_struct_list(int8_t *input, V *v) {
    if (nullptr == input) {
        return false;
    }
    *v = sum_list<V>(input);
    return true;
}

template <class V>
double avg_list(int8_t *input) {
    V result = 0;
    if (nullptr == input) {
        return true;
    }
    ::fesql::codec::ListRef *list_ref = (::fesql::codec::ListRef *)(input);
    ListV<V> *col = (ListV<V> *)(list_ref->list);
    auto iter = col->GetIterator();
    int32_t cnt = 0;
    iter->SeekToFirst();
    while (iter->Valid()) {
        result += iter->GetValue();
        iter->Next();
        cnt++;
    }
    return static_cast<double>(result) / cnt;
}

template <class V>
bool avg_struct_list(int8_t *input, V *v) {
    if (nullptr == input) {
        return false;
    }
    V result = V();
    ::fesql::codec::ListRef *list_ref = (::fesql::codec::ListRef *)(input);
    ::fesql::codec::ListV<V> *col =
        (::fesql::codec::ListV<V> *)(list_ref->list);
    auto iter = col->GetIterator();
    iter->SeekToFirst();
    int32_t cnt = 0;
    while (iter->Valid()) {
        result += iter->GetValue();
        iter->Next();
        cnt++;
    }
    if (cnt == 0) {
        return false;
    }
    *v = result / cnt;
    return true;
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

template <typename V>
int64_t distinct_count(::fesql::codec::ListRef *list_ref) {
    if (nullptr == list_ref) {
        return 0L;
    }
    ListV<V> *col = (ListV<V> *)(list_ref->list);
    auto iter = col->GetIterator();
    int64_t cnt = 0;
    iter->SeekToFirst();
    std::set<V> value_set;
    while (iter->Valid()) {
        auto v = iter->GetValue();
        if (value_set.find(v) == value_set.cend()) {
            cnt++;
            value_set.insert(v);
        }
        iter->Next();
    }
    return cnt;
}

template <class V>
V max_list(int8_t *input) {
    V result = V();
    if (nullptr == input) {
        return result;
    }
    ::fesql::codec::ListRef *list_ref = (::fesql::codec::ListRef *)(input);
    ListV<V> *col = (ListV<V> *)(list_ref->list);
    auto iter = col->GetIterator();
    iter->SeekToFirst();
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
bool max_struct_list(int8_t *input, V *v) {
    if (nullptr == input) {
        return false;
    }
    *v = max_list<V>(input);
    return true;
}

template <class V>
V min_list(int8_t *input) {
    V result = V();
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
    iter->SeekToFirst();
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
bool min_struct_list(int8_t *input, V *v) {
    if (nullptr == input) {
        return false;
    }
    *v = min_list<V>(input);
    return true;
}
template <class V>
V at_list(int8_t *input, int32_t pos) {
    ::fesql::codec::ListRef *list_ref = (::fesql::codec::ListRef *)(input);
    ListV<V> *list = (ListV<V> *)(list_ref->list);
    return list->At(pos);
}
template <class V>
bool at_struct_list(int8_t *input, int32_t pos, V *v) {
    *v = at_list<V>(input, pos);
    return true;
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
bool has_next(int8_t *input) {
    if (nullptr == input) {
        return false;
    }
    ::fesql::codec::IteratorRef *iter_ref =
        (::fesql::codec::IteratorRef *)(input);
    ConstIterator<uint64_t, V> *iter =
        (ConstIterator<uint64_t, V> *)(iter_ref->iterator);
    return iter == nullptr ? false : iter->Valid();
}

template <class V>
V next_iterator(int8_t *input) {
    ::fesql::codec::IteratorRef *iter_ref =
        (::fesql::codec::IteratorRef *)(input);
    ConstIterator<uint64_t, V> *iter =
        (ConstIterator<uint64_t, V> *)(iter_ref->iterator);
    V v = iter->GetValue();
    iter->Next();
    return v;
}
template <class V>
bool next_struct_iterator(int8_t *input, V *v) {
    ::fesql::codec::IteratorRef *iter_ref =
        (::fesql::codec::IteratorRef *)(input);
    ConstIterator<uint64_t, V> *iter =
        (ConstIterator<uint64_t, V> *)(iter_ref->iterator);
    *v = iter->GetValue();
    iter->Next();
    return true;
}
template <class V>
void delete_iterator(int8_t *input) {
    ::fesql::codec::IteratorRef *iter_ref =
        (::fesql::codec::IteratorRef *)(input);
    ConstIterator<uint64_t, V> *iter =
        (ConstIterator<uint64_t, V> *)(iter_ref->iterator);
    if (iter) {
        delete iter;
    }
}

template <class V>
V minimum(V l, V r) {
    return l < r ? l : r;
}
template <class V>
V maximum(V l, V r) {
    return l > r ? l : r;
}

}  // namespace v1

std::map<std::string, void *> NATIVE_UDF_PTRS;
void InitUDFSymbol(vm::FeSQLJIT *jit_ptr) {
    ::llvm::orc::MangleAndInterner mi(jit_ptr->getExecutionSession(),
                                      jit_ptr->getDataLayout());
    InitUDFSymbol(jit_ptr->getMainJITDylib(), mi);
}  // NOLINT
void InitUDFSymbol(::llvm::orc::JITDylib &jd,             // NOLINT
                   ::llvm::orc::MangleAndInterner &mi) {  // NOLINT
    for (auto iter = NATIVE_UDF_PTRS.cbegin(); iter != NATIVE_UDF_PTRS.cend();
         iter++) {
        AddSymbol(jd, mi, iter->first, iter->second);
    }
    AddSymbol(jd, mi, "max_int16",
              reinterpret_cast<void *>(&v1::maximum<int16_t>));
    AddSymbol(jd, mi, "max_int32",
              reinterpret_cast<void *>(&v1::maximum<int32_t>));
    AddSymbol(jd, mi, "max_int64",
              reinterpret_cast<void *>(&v1::maximum<int64_t>));
    AddSymbol(jd, mi, "max_float",
              reinterpret_cast<void *>(&v1::maximum<float>));
    AddSymbol(jd, mi, "max_double",
              reinterpret_cast<void *>(&v1::maximum<double>));

    AddSymbol(jd, mi, "min_int16",
              reinterpret_cast<void *>(&v1::minimum<int16_t>));
    AddSymbol(jd, mi, "min_int32",
              reinterpret_cast<void *>(&v1::minimum<int32_t>));
    AddSymbol(jd, mi, "min_int64",
              reinterpret_cast<void *>(&v1::minimum<int64_t>));
    AddSymbol(jd, mi, "min_float",
              reinterpret_cast<void *>(&v1::minimum<float>));
    AddSymbol(jd, mi, "min_double",
              reinterpret_cast<void *>(&v1::minimum<double>));
}
bool AddSymbol(::llvm::orc::JITDylib &jd,           // NOLINT
               ::llvm::orc::MangleAndInterner &mi,  // NOLINT
               const std::string &fn_name, void *fn_ptr) {
    return ::fesql::vm::FeSQLJIT::AddSymbol(jd, mi, fn_name, fn_ptr);
}

bool RegisterMethod(::llvm::Module *module, const std::string &fn_name,
                    fesql::node::TypeNode *ret,
                    std::initializer_list<fesql::node::TypeNode *> args,
                    void *fn) {
    codegen::UDFIRBuilder udf_ir_builder(&NATIVE_UDF_PTRS);
    node::NodeManager nm;
    base::Status status;
    auto fn_args = nm.MakeFnListNode();
    for (auto &arg : args) {
        fn_args->AddChild(nm.MakeFnParaNode("", arg));
    }

    auto header = dynamic_cast<node::FnNodeFnHeander *>(
        nm.MakeFnHeaderNode(fn_name, fn_args, ret));
    return udf_ir_builder.BuildNativeCUDF(module, header, fn, status);
}
void RegisterNativeUDFToModule(::llvm::Module *module) {
    codegen::UDFIRBuilder udf_ir_builder(&NATIVE_UDF_PTRS);
    node::NodeManager nm;
    base::Status status;

    auto bool_ty = nm.MakeTypeNode(node::kBool);
    auto i32_ty = nm.MakeTypeNode(node::kInt32);
    auto i64_ty = nm.MakeTypeNode(node::kInt64);
    auto i16_ty = nm.MakeTypeNode(node::kInt16);
    auto float_ty = nm.MakeTypeNode(node::kFloat);
    auto double_ty = nm.MakeTypeNode(node::kDouble);
    auto time_ty = nm.MakeTypeNode(node::kTimestamp);
    auto date_ty = nm.MakeTypeNode(node::kDate);
    auto string_ty = nm.MakeTypeNode(node::kVarchar);

    auto list_i32_ty = nm.MakeTypeNode(node::kList, *i32_ty);
    auto list_i64_ty = nm.MakeTypeNode(node::kList, *i64_ty);
    auto list_i16_ty = nm.MakeTypeNode(node::kList, *i16_ty);
    auto list_float_ty = nm.MakeTypeNode(node::kList, *float_ty);
    auto list_double_ty = nm.MakeTypeNode(node::kList, *double_ty);
    auto list_time_ty = nm.MakeTypeNode(node::kList, *time_ty);
    auto list_date_ty = nm.MakeTypeNode(node::kList, *date_ty);
    auto list_string_ty = nm.MakeTypeNode(node::kList, *string_ty);

    auto iter_i32_ty = nm.MakeTypeNode(node::kIterator, *i32_ty);
    auto iter_i64_ty = nm.MakeTypeNode(node::kIterator, *i64_ty);
    auto iter_i16_ty = nm.MakeTypeNode(node::kIterator, *i16_ty);
    auto iter_float_ty = nm.MakeTypeNode(node::kIterator, *float_ty);
    auto iter_double_ty = nm.MakeTypeNode(node::kIterator, *double_ty);
    auto iter_time_ty = nm.MakeTypeNode(node::kIterator, *time_ty);
    auto iter_date_ty = nm.MakeTypeNode(node::kIterator, *date_ty);
    auto iter_string_ty = nm.MakeTypeNode(node::kIterator, *string_ty);

    // inc(int32):int32
    RegisterMethod(module, "inc", i32_ty, {i32_ty},
                   reinterpret_cast<void *>(v1::inc<int32_t>));

    // dayofmonth, year, month
    RegisterMethod(
        module, "year", i32_ty, {i64_ty},
        reinterpret_cast<void *>(static_cast<int32_t (*)(int64_t)>(v1::year)));
    RegisterMethod(
        module, "month", i32_ty, {i64_ty},
        reinterpret_cast<void *>(static_cast<int32_t (*)(int64_t)>(v1::month)));
    RegisterMethod(module, "dayofmonth", i32_ty, {i64_ty},
                   reinterpret_cast<void *>(
                       static_cast<int32_t (*)(int64_t)>(v1::dayofmonth)));
    RegisterMethod(module, "dayofweek", i32_ty, {i64_ty},
                   reinterpret_cast<void *>(
                       static_cast<int32_t (*)(int64_t)>(v1::dayofweek)));
    RegisterMethod(module, "dayofweek", i32_ty, {i64_ty},
                   reinterpret_cast<void *>(
                       static_cast<int32_t (*)(int64_t)>(v1::dayofweek)));
    RegisterMethod(
        module, "week", i32_ty, {i64_ty},
        reinterpret_cast<void *>(static_cast<int32_t (*)(int64_t)>(v1::week)));

    RegisterMethod(module, "year", i32_ty, {time_ty},
                   reinterpret_cast<void *>(
                       static_cast<int32_t (*)(codec::Timestamp *)>(v1::year)));

    RegisterMethod(
        module, "month", i32_ty, {time_ty},
        reinterpret_cast<void *>(
            static_cast<int32_t (*)(codec::Timestamp *)>(v1::month)));
    RegisterMethod(
        module, "dayofmonth", i32_ty, {time_ty},
        reinterpret_cast<void *>(
            static_cast<int32_t (*)(codec::Timestamp *)>(v1::dayofmonth)));
    RegisterMethod(
        module, "dayofweek", i32_ty, {time_ty},
        reinterpret_cast<void *>(
            static_cast<int32_t (*)(codec::Timestamp *)>(v1::dayofweek)));
    RegisterMethod(module, "week", i32_ty, {time_ty},
                   reinterpret_cast<void *>(
                       static_cast<int32_t (*)(codec::Timestamp *)>(v1::week)));

    RegisterMethod(module, "dayofweek", i32_ty, {date_ty},
                   reinterpret_cast<void *>(
                       static_cast<int32_t (*)(codec::Date *)>(v1::dayofweek)));

    RegisterMethod(module, "dayofweek", i32_ty, {date_ty},
                   reinterpret_cast<void *>(
                       static_cast<int32_t (*)(codec::Date *)>(v1::dayofweek)));

    RegisterMethod(module, "week", i32_ty, {date_ty},
                   reinterpret_cast<void *>(
                       static_cast<int32_t (*)(codec::Date *)>(v1::week)));

    RegisterMethod(module, "at", i16_ty, {list_i16_ty, i32_ty},
                   reinterpret_cast<void *>(v1::at_list<int16_t>));
    RegisterMethod(module, "at", i32_ty, {list_i32_ty, i32_ty},
                   reinterpret_cast<void *>(v1::at_list<int32_t>));
    RegisterMethod(module, "at", i64_ty, {list_i64_ty, i32_ty},
                   reinterpret_cast<void *>(v1::at_list<int64_t>));
    RegisterMethod(module, "at", float_ty, {list_float_ty, i32_ty},
                   reinterpret_cast<void *>(v1::at_list<float>));
    RegisterMethod(module, "at", double_ty, {list_double_ty, i32_ty},
                   reinterpret_cast<void *>(v1::at_list<double>));

    RegisterMethod(
        module, "at", bool_ty, {list_time_ty, i32_ty, time_ty},
        reinterpret_cast<void *>(v1::at_struct_list<codec::Timestamp>));
    RegisterMethod(module, "at", bool_ty, {list_date_ty, i32_ty, date_ty},
                   reinterpret_cast<void *>(v1::at_struct_list<codec::Date>));
    RegisterMethod(
        module, "at", bool_ty, {list_string_ty, i32_ty, string_ty},
        reinterpret_cast<void *>(v1::at_struct_list<codec::StringRef>));

    RegisterMethod(module, "sum", i16_ty, {list_i16_ty},
                   reinterpret_cast<void *>(v1::sum_list<int16_t>));
    RegisterMethod(module, "sum", i32_ty, {list_i32_ty},
                   reinterpret_cast<void *>(v1::sum_list<int32_t>));
    RegisterMethod(module, "sum", i64_ty, {list_i64_ty},
                   reinterpret_cast<void *>(v1::sum_list<int64_t>));
    RegisterMethod(module, "sum", float_ty, {list_float_ty},
                   reinterpret_cast<void *>(v1::sum_list<float>));
    RegisterMethod(module, "sum", double_ty, {list_double_ty},
                   reinterpret_cast<void *>(v1::sum_list<double>));
    RegisterMethod(
        module, "sum", bool_ty, {list_time_ty, time_ty},
        reinterpret_cast<void *>(v1::sum_struct_list<codec::Timestamp>));

    RegisterMethod(module, "max", i16_ty, {list_i16_ty},
                   reinterpret_cast<void *>(v1::max_list<int16_t>));
    RegisterMethod(module, "max", i32_ty, {list_i32_ty},
                   reinterpret_cast<void *>(v1::max_list<int32_t>));
    RegisterMethod(module, "max", i64_ty, {list_i64_ty},
                   reinterpret_cast<void *>(v1::max_list<int64_t>));
    RegisterMethod(module, "max", float_ty, {list_float_ty},
                   reinterpret_cast<void *>(v1::max_list<float>));
    RegisterMethod(module, "max", double_ty, {list_double_ty},
                   reinterpret_cast<void *>(v1::max_list<double>));
    RegisterMethod(
        module, "max", bool_ty, {list_time_ty, time_ty},
        reinterpret_cast<void *>(v1::max_struct_list<codec::Timestamp>));

    RegisterMethod(module, "max", bool_ty, {list_date_ty, date_ty},
                   reinterpret_cast<void *>(v1::max_struct_list<codec::Date>));
    RegisterMethod(
        module, "max", bool_ty, {list_string_ty, string_ty},
        reinterpret_cast<void *>(v1::max_struct_list<codec::StringRef>));

    RegisterMethod(module, "min", i16_ty, {list_i16_ty},
                   reinterpret_cast<void *>(v1::min_list<int16_t>));
    RegisterMethod(module, "min", i32_ty, {list_i32_ty},
                   reinterpret_cast<void *>(v1::min_list<int32_t>));
    RegisterMethod(module, "min", i64_ty, {list_i64_ty},
                   reinterpret_cast<void *>(v1::min_list<int64_t>));
    RegisterMethod(module, "min", float_ty, {list_float_ty},
                   reinterpret_cast<void *>(v1::min_list<float>));
    RegisterMethod(module, "min", double_ty, {list_double_ty},
                   reinterpret_cast<void *>(v1::min_list<double>));
    RegisterMethod(
        module, "min", bool_ty, {list_time_ty, time_ty},
        reinterpret_cast<void *>(v1::min_struct_list<codec::Timestamp>));

    RegisterMethod(module, "min", bool_ty, {list_date_ty, date_ty},
                   reinterpret_cast<void *>(v1::min_struct_list<codec::Date>));
    RegisterMethod(
        module, "min", bool_ty, {list_string_ty, string_ty},
        reinterpret_cast<void *>(v1::min_struct_list<codec::StringRef>));

    RegisterMethod(module, "count", i64_ty, {list_i16_ty},
                   reinterpret_cast<void *>(v1::count_list<int16_t>));
    RegisterMethod(module, "count", i64_ty, {list_i32_ty},
                   reinterpret_cast<void *>(v1::count_list<int32_t>));
    RegisterMethod(module, "count", i64_ty, {list_i64_ty},
                   reinterpret_cast<void *>(v1::count_list<int64_t>));
    RegisterMethod(module, "count", i64_ty, {list_float_ty},
                   reinterpret_cast<void *>(v1::count_list<float>));
    RegisterMethod(module, "count", i64_ty, {list_double_ty},
                   reinterpret_cast<void *>(v1::count_list<double>));
    RegisterMethod(module, "count", i64_ty, {list_time_ty},
                   reinterpret_cast<void *>(v1::count_list<codec::Timestamp>));
    RegisterMethod(module, "count", i64_ty, {list_date_ty},
                   reinterpret_cast<void *>(v1::count_list<codec::Date>));
    RegisterMethod(module, "count", i64_ty, {list_string_ty},
                   reinterpret_cast<void *>(v1::count_list<codec::StringRef>));

    {
        const std::string fn_name = "distinct_count";
        RegisterMethod(module, fn_name, i64_ty, {list_i16_ty},
                       reinterpret_cast<void *>(v1::distinct_count<int16_t>));
        RegisterMethod(module, fn_name, i64_ty, {list_i32_ty},
                       reinterpret_cast<void *>(v1::distinct_count<int32_t>));
        RegisterMethod(module, fn_name, i64_ty, {list_i64_ty},
                       reinterpret_cast<void *>(v1::distinct_count<int64_t>));
        RegisterMethod(module, fn_name, i64_ty, {list_float_ty},
                       reinterpret_cast<void *>(v1::distinct_count<float>));
        RegisterMethod(module, fn_name, i64_ty, {list_double_ty},
                       reinterpret_cast<void *>(v1::distinct_count<double>));
        RegisterMethod(
            module, fn_name, i64_ty, {list_time_ty},
            reinterpret_cast<void *>(v1::distinct_count<codec::Timestamp>));
        RegisterMethod(
            module, fn_name, i64_ty, {list_date_ty},
            reinterpret_cast<void *>(v1::distinct_count<codec::Date>));
        RegisterMethod(
            module, fn_name, i64_ty, {list_string_ty},
            reinterpret_cast<void *>(v1::distinct_count<codec::StringRef>));
    }
    RegisterMethod(module, "iterator", bool_ty, {list_i16_ty, iter_i16_ty},
                   reinterpret_cast<void *>(v1::iterator_list<int16_t>));
    RegisterMethod(module, "iterator", bool_ty, {list_i32_ty, iter_i32_ty},
                   reinterpret_cast<void *>(v1::iterator_list<int32_t>));
    RegisterMethod(module, "iterator", bool_ty, {list_i64_ty, iter_i64_ty},
                   reinterpret_cast<void *>(v1::iterator_list<int64_t>));
    RegisterMethod(module, "iterator", bool_ty, {list_float_ty, iter_float_ty},
                   reinterpret_cast<void *>(v1::iterator_list<float>));
    RegisterMethod(module, "iterator", bool_ty,
                   {list_double_ty, iter_double_ty},
                   reinterpret_cast<void *>(v1::iterator_list<double>));
    RegisterMethod(
        module, "iterator", bool_ty, {list_time_ty, iter_time_ty},
        reinterpret_cast<void *>(v1::iterator_list<codec::Timestamp>));
    RegisterMethod(module, "iterator", bool_ty, {list_date_ty, iter_date_ty},
                   reinterpret_cast<void *>(v1::iterator_list<codec::Date>));
    RegisterMethod(
        module, "iterator", bool_ty, {list_string_ty, iter_string_ty},
        reinterpret_cast<void *>(v1::iterator_list<codec::StringRef>));

    RegisterMethod(module, "next", i16_ty, {iter_i16_ty},
                   reinterpret_cast<void *>(v1::next_iterator<int16_t>));
    RegisterMethod(module, "next", i32_ty, {iter_i32_ty},
                   reinterpret_cast<void *>(v1::next_iterator<int32_t>));
    RegisterMethod(module, "next", i64_ty, {iter_i64_ty},
                   reinterpret_cast<void *>(v1::next_iterator<int64_t>));
    RegisterMethod(module, "next", float_ty, {iter_float_ty},
                   reinterpret_cast<void *>(v1::next_iterator<float>));
    RegisterMethod(module, "next", double_ty, {iter_double_ty},
                   reinterpret_cast<void *>(v1::next_iterator<double>));
    RegisterMethod(
        module, "next", bool_ty, {iter_time_ty, time_ty},
        reinterpret_cast<void *>(v1::next_struct_iterator<codec::Timestamp>));

    RegisterMethod(
        module, "next", bool_ty, {iter_date_ty, date_ty},
        reinterpret_cast<void *>(v1::next_struct_iterator<codec::Date>));
    RegisterMethod(
        module, "next", bool_ty, {iter_string_ty, string_ty},
        reinterpret_cast<void *>(v1::next_struct_iterator<codec::StringRef>));

    RegisterMethod(module, "has_next", bool_ty, {iter_i16_ty},
                   reinterpret_cast<void *>(v1::has_next<int16_t>));
    RegisterMethod(module, "has_next", bool_ty, {iter_i32_ty},
                   reinterpret_cast<void *>(v1::has_next<int32_t>));
    RegisterMethod(module, "has_next", bool_ty, {iter_i64_ty},
                   reinterpret_cast<void *>(v1::has_next<int64_t>));
    RegisterMethod(module, "has_next", bool_ty, {iter_float_ty},
                   reinterpret_cast<void *>(v1::has_next<float>));
    RegisterMethod(module, "has_next", bool_ty, {iter_double_ty},
                   reinterpret_cast<void *>(v1::has_next<double>));
    RegisterMethod(module, "has_next", bool_ty, {iter_time_ty},
                   reinterpret_cast<void *>(v1::has_next<codec::Timestamp>));
    RegisterMethod(module, "has_next", bool_ty, {iter_date_ty},
                   reinterpret_cast<void *>(v1::has_next<codec::Date>));
    RegisterMethod(module, "has_next", bool_ty, {iter_string_ty},
                   reinterpret_cast<void *>(v1::has_next<codec::StringRef>));

    RegisterMethod(module, "delete_iterator", bool_ty, {iter_i16_ty},
                   reinterpret_cast<void *>(v1::delete_iterator<int16_t>));
    RegisterMethod(module, "delete_iterator", bool_ty, {iter_i32_ty},
                   reinterpret_cast<void *>(v1::delete_iterator<int32_t>));
    RegisterMethod(module, "delete_iterator", bool_ty, {iter_i64_ty},
                   reinterpret_cast<void *>(v1::delete_iterator<int64_t>));
    RegisterMethod(module, "delete_iterator", bool_ty, {iter_float_ty},
                   reinterpret_cast<void *>(v1::delete_iterator<float>));
    RegisterMethod(module, "delete_iterator", bool_ty, {iter_double_ty},
                   reinterpret_cast<void *>(v1::delete_iterator<double>));
    RegisterMethod(
        module, "delete_iterator", bool_ty, {iter_time_ty},
        reinterpret_cast<void *>(v1::delete_iterator<codec::Timestamp>));
    RegisterMethod(module, "delete_iterator", bool_ty, {iter_date_ty},
                   reinterpret_cast<void *>(v1::delete_iterator<codec::Date>));
    RegisterMethod(
        module, "delete_iterator", bool_ty, {iter_string_ty},
        reinterpret_cast<void *>(v1::delete_iterator<codec::StringRef>));
}  // namespace udf
void RegisterUDFToModule(::llvm::Module *m) {
    base::Status status;
    RegisterNativeUDFToModule(m);
    codegen::UDFIRBuilder udf_ir_builder(&NATIVE_UDF_PTRS);
    if (!udf_ir_builder.BuildUDF(m, status)) {
        LOG(WARNING) << status.msg;
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
