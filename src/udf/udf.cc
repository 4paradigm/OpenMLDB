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
#include "boost/date_time/posix_time/posix_time.hpp"
#include "bthread/types.h"
#include "codec/list_iterator_codec.h"
#include "codec/row.h"
#include "codec/type_codec.h"
#include "codegen/fn_ir_builder.h"
#include "node/node_manager.h"
#include "node/sql_node.h"
#include "udf/default_udf_library.h"
#include "udf/literal_traits.h"

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
// TODO(chenjing): 时区统一配置
const int32_t TZ = 8;
const time_t TZ_OFFSET = TZ * 3600000;
bthread_key_t B_THREAD_LOCAL_MEM_POOL_KEY;

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
int32_t weekofyear(int64_t ts) {
    time_t time = (ts + TZ_OFFSET) / 1000;
    struct tm t;
    gmtime_r(&time, &t);
    boost::gregorian::date d = boost::gregorian::date_from_tm(t);
    return d.week_number();
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
int32_t weekofyear(codec::Timestamp *ts) { return weekofyear(ts->ts_); }
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
// Return the iso 8601 week number 1..53
int32_t weekofyear(codec::Date *date) {
    int32_t day, month, year;
    if (!codec::Date::Decode(date->date_, &year, &month, &day)) {
        return 0;
    }
    boost::gregorian::date d(year, month, day);
    return d.week_number();
}

int16_t abs_int16(int16_t x) { return static_cast<int16_t>(abs((int32_t)x)); }
int64_t abs_int64(int64_t x) { return static_cast<int64_t>(labs(x)); }

int Ceild(double x) { return static_cast<int>(ceil(x)); }
int Ceilf(float x) { return static_cast<int>(ceilf(x)); }

void date_format(codec::Timestamp *timestamp, fesql::codec::StringRef *format,
                 fesql::codec::StringRef *output) {
    if (nullptr == format) {
        return;
    }
    date_format(timestamp, format->ToString(), output);
}
void date_format(const codec::Timestamp *timestamp, const char *format,
                 char *buffer, size_t size) {
    time_t time = (timestamp->ts_ + TZ_OFFSET) / 1000;
    struct tm t;
    gmtime_r(&time, &t);
    strftime(buffer, size, format, &t);
}
void date_format(codec::Timestamp *timestamp, const std::string &format,
                 fesql::codec::StringRef *output) {
    if (nullptr == output) {
        return;
    }
    if (nullptr == timestamp) {
        output->data_ = nullptr;
        output->size_ = 0;
        return;
    }
    char buffer[80];
    date_format(timestamp, format.c_str(), buffer, 80);
    output->size_ = strlen(buffer);
    char *target =
        reinterpret_cast<char *>(ThreadLocalMemoryPoolAlloc(output->size_));
    memcpy(target, buffer, output->size_);
    output->data_ = target;
}

void date_format(codec::Date *date, fesql::codec::StringRef *format,
                 fesql::codec::StringRef *output) {
    if (nullptr == format) {
        return;
    }
    date_format(date, format->ToString(), output);
}

bool date_format(const codec::Date *date, const char *format, char *buffer,
                 size_t size) {
    int32_t day, month, year;
    if (!codec::Date::Decode(date->date_, &year, &month, &day)) {
        return false;
    }
    boost::gregorian::date g_date(year, month, day);
    tm t = boost::gregorian::to_tm(g_date);
    strftime(buffer, size, format, &t);
    return true;
}

void date_format(codec::Date *date, const std::string &format,
                 fesql::codec::StringRef *output) {
    if (nullptr == output) {
        return;
    }
    if (nullptr == date) {
        output->data_ = nullptr;
        output->size_ = 0;
        return;
    }
    char buffer[80];
    if (!date_format(date, format.c_str(), buffer, 80)) {
        output->size_ = 0;
        output->data_ = nullptr;
        return;
    }
    output->size_ = strlen(buffer);
    char *target =
        reinterpret_cast<char *>(ThreadLocalMemoryPoolAlloc(output->size_));
    memcpy(target, buffer, output->size_);
    output->data_ = target;
}

void timestamp_to_string(codec::Timestamp *v, fesql::codec::StringRef *output) {
    date_format(v, "%Y-%m-%d %H:%M:%S", output);
}

void date_to_string(codec::Date *date, fesql::codec::StringRef *output) {
    date_format(date, "%Y-%m-%d", output);
}
void sub_string(fesql::codec::StringRef *str, int32_t from,
                fesql::codec::StringRef *output) {
    if (nullptr == output) {
        return;
    }
    if (nullptr == str || str->IsNull()) {
        output->data_ = nullptr;
        output->size_ = 0;
        return;
    }
    return sub_string(str, from, str->size_, output);
}
// set output as empty string if from == 0
void sub_string(fesql::codec::StringRef *str, int32_t from, int32_t len,
                fesql::codec::StringRef *output) {
    if (nullptr == output) {
        return;
    }
    if (nullptr == str || str->IsNull()) {
        output->data_ = nullptr;
        output->size_ = 0;
        return;
    }

    if (0 == from || len < 1) {
        output->data_ = str->data_;
        output->size_ = 0;
        return;
    }

    int32_t str_size = static_cast<int32_t>(str->size_);

    // `from` is out of string range
    if (from > str_size || from < -1 * str_size) {
        output->data_ = str->data_;
        output->size_ = 0;
        return;
    }

    if (from < 0) {
        from = str_size + from;
    } else {
        from = from - 1;
    }

    len = str_size - from < len ? str_size - from : len;
    output->data_ = str->data_ + from;
    output->size_ = static_cast<uint32_t>(len);
    return;
}

template <>
uint32_t format_string<int16_t>(const int16_t &v, char *buffer, size_t size) {
    return snprintf(buffer, size, "%d", v);
}

template <>
uint32_t format_string<int32_t>(const int32_t &v, char *buffer, size_t size) {
    return snprintf(buffer, size, "%d", v);
}

template <>
uint32_t format_string<int64_t>(const int64_t &v, char *buffer, size_t size) {
    return snprintf(buffer, size, "%lld", v);
}

template <>
uint32_t format_string<float>(const float &v, char *buffer, size_t size) {
    return snprintf(buffer, size, "%f", v);
}

template <>
uint32_t format_string<double>(const double &v, char *buffer, size_t size) {
    return snprintf(buffer, size, "%f", v);
}

template <>
uint32_t format_string<codec::Date>(const codec::Date &v, char *buffer,
                                    size_t size) {
    const uint32_t len = 10;  // 1990-01-01
    if (buffer != nullptr && size >= len) {
        date_format(&v, "%Y-%m-%d", buffer, size);
    }
    return len;
}

template <>
uint32_t format_string<codec::Timestamp>(const codec::Timestamp &v,
                                         char *buffer, size_t size) {
    const uint32_t len = 19;  // "%Y-%m-%d %H:%M:%S"
    if (buffer != nullptr && size >= len) {
        date_format(&v, "%Y-%m-%d %H:%M:%S", buffer, size);
    }
    return len;
}

template <>
uint32_t format_string<std::string>(const std::string &v, char *buffer,
                                    size_t size) {
    return snprintf(buffer, size, "%s", v.c_str());
}

template <class V>
bool iterator_list(int8_t *input, int8_t *output) {
    if (nullptr == input || nullptr == output) {
        return false;
    }
    ::fesql::codec::ListRef<> *list_ref = (::fesql::codec::ListRef<> *)(input);
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

const codec::Row *next_row_iterator(int8_t *input) {
    ::fesql::codec::IteratorRef *iter_ref =
        (::fesql::codec::IteratorRef *)(input);
    ConstIterator<uint64_t, codec::Row> *iter =
        (ConstIterator<uint64_t, codec::Row> *)(iter_ref->iterator);
    auto res = &(iter->GetValue());
    iter->Next();
    return res;
}

template <class V>
void next_nullable_iterator(int8_t *input, V *v, bool *is_null) {
    ::fesql::codec::IteratorRef *iter_ref =
        (::fesql::codec::IteratorRef *)(input);
    ConstIterator<uint64_t, Nullable<V>> *iter =
        (ConstIterator<uint64_t, Nullable<V>> *)(iter_ref->iterator);
    auto nullable_value = iter->GetValue();
    iter->Next();
    *v = nullable_value.value();
    *is_null = nullable_value.is_null();
    return;
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

}  // namespace v1

thread_local base::ByteMemoryPool __THREAD_LOCAL_MEM_POOL;

int8_t *ThreadLocalMemoryPoolAlloc(int32_t request_size) {
    if (request_size < 0) {
        return nullptr;
    }
    return reinterpret_cast<int8_t *>(fesql::udf::__THREAD_LOCAL_MEM_POOL.Alloc(
        static_cast<size_t>(request_size)));
}
void ThreadLocalMemoryPoolReset() {
    fesql::udf::__THREAD_LOCAL_MEM_POOL.Reset();
}

void InitUDFSymbol(vm::FeSQLJIT *jit_ptr) {
    ::llvm::orc::MangleAndInterner mi(jit_ptr->getExecutionSession(),
                                      jit_ptr->getDataLayout());
    InitUDFSymbol(jit_ptr->getMainJITDylib(), mi);
}  // NOLINT
void InitUDFSymbol(::llvm::orc::JITDylib &jd,             // NOLINT
                   ::llvm::orc::MangleAndInterner &mi) {  // NOLINT
    fesql::vm::FeSQLJIT::AddSymbol(
        jd, mi, "fesql_memery_pool_alloc",
        reinterpret_cast<void *>(&fesql::udf::ThreadLocalMemoryPoolAlloc));
}
bool AddSymbol(::llvm::orc::JITDylib &jd,           // NOLINT
               ::llvm::orc::MangleAndInterner &mi,  // NOLINT
               const std::string &fn_name, void *fn_ptr) {
    return ::fesql::vm::FeSQLJIT::AddSymbol(jd, mi, fn_name, fn_ptr);
}

bool RegisterMethod(const std::string &fn_name, fesql::node::TypeNode *ret,
                    std::initializer_list<fesql::node::TypeNode *> args,
                    void *fn_ptr) {
    node::NodeManager nm;
    base::Status status;
    auto fn_args = nm.MakeFnListNode();
    for (auto &arg : args) {
        fn_args->AddChild(nm.MakeFnParaNode("", arg));
    }
    auto header = dynamic_cast<node::FnNodeFnHeander *>(
        nm.MakeFnHeaderNode(fn_name, fn_args, ret));
    DefaultUDFLibrary::get()->AddExternalSymbol(header->GeIRFunctionName(),
                                                fn_ptr);
    return true;
}

void RegisterNativeUDFToModule() {
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
    auto row_ty = nm.MakeTypeNode(node::kRow);

    auto list_i32_ty = nm.MakeTypeNode(node::kList, i32_ty);
    auto list_i64_ty = nm.MakeTypeNode(node::kList, i64_ty);
    auto list_i16_ty = nm.MakeTypeNode(node::kList, i16_ty);
    auto list_bool_ty = nm.MakeTypeNode(node::kList, bool_ty);
    auto list_float_ty = nm.MakeTypeNode(node::kList, float_ty);
    auto list_double_ty = nm.MakeTypeNode(node::kList, double_ty);
    auto list_time_ty = nm.MakeTypeNode(node::kList, time_ty);
    auto list_date_ty = nm.MakeTypeNode(node::kList, date_ty);
    auto list_string_ty = nm.MakeTypeNode(node::kList, string_ty);
    auto list_row_ty = nm.MakeTypeNode(node::kList, row_ty);

    auto iter_i32_ty = nm.MakeTypeNode(node::kIterator, i32_ty);
    auto iter_i64_ty = nm.MakeTypeNode(node::kIterator, i64_ty);
    auto iter_i16_ty = nm.MakeTypeNode(node::kIterator, i16_ty);
    auto iter_bool_ty = nm.MakeTypeNode(node::kIterator, bool_ty);
    auto iter_float_ty = nm.MakeTypeNode(node::kIterator, float_ty);
    auto iter_double_ty = nm.MakeTypeNode(node::kIterator, double_ty);
    auto iter_time_ty = nm.MakeTypeNode(node::kIterator, time_ty);
    auto iter_date_ty = nm.MakeTypeNode(node::kIterator, date_ty);
    auto iter_string_ty = nm.MakeTypeNode(node::kIterator, string_ty);
    auto iter_row_ty = nm.MakeTypeNode(node::kIterator, row_ty);

    RegisterMethod("iterator", bool_ty, {list_i16_ty, iter_i16_ty},
                   reinterpret_cast<void *>(v1::iterator_list<int16_t>));
    RegisterMethod("iterator", bool_ty, {list_i32_ty, iter_i32_ty},
                   reinterpret_cast<void *>(v1::iterator_list<int32_t>));
    RegisterMethod("iterator", bool_ty, {list_i64_ty, iter_i64_ty},
                   reinterpret_cast<void *>(v1::iterator_list<int64_t>));
    RegisterMethod("iterator", bool_ty, {list_bool_ty, iter_bool_ty},
                   reinterpret_cast<void *>(v1::iterator_list<bool>));
    RegisterMethod("iterator", bool_ty, {list_float_ty, iter_float_ty},
                   reinterpret_cast<void *>(v1::iterator_list<float>));
    RegisterMethod("iterator", bool_ty, {list_double_ty, iter_double_ty},
                   reinterpret_cast<void *>(v1::iterator_list<double>));
    RegisterMethod(
        "iterator", bool_ty, {list_time_ty, iter_time_ty},
        reinterpret_cast<void *>(v1::iterator_list<codec::Timestamp>));
    RegisterMethod("iterator", bool_ty, {list_date_ty, iter_date_ty},
                   reinterpret_cast<void *>(v1::iterator_list<codec::Date>));
    RegisterMethod(
        "iterator", bool_ty, {list_string_ty, iter_string_ty},
        reinterpret_cast<void *>(v1::iterator_list<codec::StringRef>));
    RegisterMethod("iterator", bool_ty, {list_row_ty, iter_row_ty},
                   reinterpret_cast<void *>(v1::iterator_list<codec::Row>));

    RegisterMethod("next", i16_ty, {iter_i16_ty},
                   reinterpret_cast<void *>(v1::next_iterator<int16_t>));
    RegisterMethod("next", i32_ty, {iter_i32_ty},
                   reinterpret_cast<void *>(v1::next_iterator<int32_t>));
    RegisterMethod("next", i64_ty, {iter_i64_ty},
                   reinterpret_cast<void *>(v1::next_iterator<int64_t>));
    RegisterMethod("next", bool_ty, {iter_bool_ty},
                   reinterpret_cast<void *>(v1::next_iterator<bool>));
    RegisterMethod("next", float_ty, {iter_float_ty},
                   reinterpret_cast<void *>(v1::next_iterator<float>));
    RegisterMethod("next", double_ty, {iter_double_ty},
                   reinterpret_cast<void *>(v1::next_iterator<double>));
    RegisterMethod(
        "next", bool_ty, {iter_time_ty, time_ty},
        reinterpret_cast<void *>(v1::next_struct_iterator<codec::Timestamp>));
    RegisterMethod(
        "next", bool_ty, {iter_date_ty, date_ty},
        reinterpret_cast<void *>(v1::next_struct_iterator<codec::Date>));
    RegisterMethod(
        "next", bool_ty, {iter_string_ty, string_ty},
        reinterpret_cast<void *>(v1::next_struct_iterator<codec::StringRef>));
    RegisterMethod("next", row_ty, {iter_row_ty},
                   reinterpret_cast<void *>(v1::next_row_iterator));

    RegisterMethod(
        "next_nullable", i16_ty, {iter_i16_ty},
        reinterpret_cast<void *>(v1::next_nullable_iterator<int16_t>));
    RegisterMethod(
        "next_nullable", i32_ty, {iter_i32_ty},
        reinterpret_cast<void *>(v1::next_nullable_iterator<int32_t>));
    RegisterMethod(
        "next_nullable", i64_ty, {iter_i64_ty},
        reinterpret_cast<void *>(v1::next_nullable_iterator<int64_t>));
    RegisterMethod("next_nullable", bool_ty, {iter_bool_ty},
                   reinterpret_cast<void *>(v1::next_nullable_iterator<bool>));
    RegisterMethod("next_nullable", float_ty, {iter_float_ty},
                   reinterpret_cast<void *>(v1::next_nullable_iterator<float>));
    RegisterMethod(
        "next_nullable", double_ty, {iter_double_ty},
        reinterpret_cast<void *>(v1::next_nullable_iterator<double>));
    RegisterMethod(
        "next_nullable", bool_ty, {iter_time_ty},
        reinterpret_cast<void *>(v1::next_nullable_iterator<codec::Timestamp>));
    RegisterMethod(
        "next_nullable", bool_ty, {iter_date_ty},
        reinterpret_cast<void *>(v1::next_nullable_iterator<codec::Date>));
    RegisterMethod(
        "next_nullable", bool_ty, {iter_string_ty},
        reinterpret_cast<void *>(v1::next_nullable_iterator<codec::StringRef>));

    RegisterMethod("has_next", bool_ty, {iter_i16_ty},
                   reinterpret_cast<void *>(v1::has_next<int16_t>));
    RegisterMethod("has_next", bool_ty, {iter_i32_ty},
                   reinterpret_cast<void *>(v1::has_next<int32_t>));
    RegisterMethod("has_next", bool_ty, {iter_i64_ty},
                   reinterpret_cast<void *>(v1::has_next<int64_t>));
    RegisterMethod("has_next", bool_ty, {iter_bool_ty},
                   reinterpret_cast<void *>(v1::has_next<bool>));
    RegisterMethod("has_next", bool_ty, {iter_float_ty},
                   reinterpret_cast<void *>(v1::has_next<float>));
    RegisterMethod("has_next", bool_ty, {iter_double_ty},
                   reinterpret_cast<void *>(v1::has_next<double>));
    RegisterMethod("has_next", bool_ty, {iter_time_ty},
                   reinterpret_cast<void *>(v1::has_next<codec::Timestamp>));
    RegisterMethod("has_next", bool_ty, {iter_date_ty},
                   reinterpret_cast<void *>(v1::has_next<codec::Date>));
    RegisterMethod("has_next", bool_ty, {iter_string_ty},
                   reinterpret_cast<void *>(v1::has_next<codec::StringRef>));
    RegisterMethod("has_next", bool_ty, {iter_row_ty},
                   reinterpret_cast<void *>(v1::has_next<codec::Row>));

    RegisterMethod("delete_iterator", bool_ty, {iter_i16_ty},
                   reinterpret_cast<void *>(v1::delete_iterator<int16_t>));
    RegisterMethod("delete_iterator", bool_ty, {iter_i32_ty},
                   reinterpret_cast<void *>(v1::delete_iterator<int32_t>));
    RegisterMethod("delete_iterator", bool_ty, {iter_i64_ty},
                   reinterpret_cast<void *>(v1::delete_iterator<int64_t>));
    RegisterMethod("delete_iterator", bool_ty, {iter_bool_ty},
                   reinterpret_cast<void *>(v1::delete_iterator<bool>));
    RegisterMethod("delete_iterator", bool_ty, {iter_float_ty},
                   reinterpret_cast<void *>(v1::delete_iterator<float>));
    RegisterMethod("delete_iterator", bool_ty, {iter_double_ty},
                   reinterpret_cast<void *>(v1::delete_iterator<double>));
    RegisterMethod(
        "delete_iterator", bool_ty, {iter_time_ty},
        reinterpret_cast<void *>(v1::delete_iterator<codec::Timestamp>));
    RegisterMethod("delete_iterator", bool_ty, {iter_date_ty},
                   reinterpret_cast<void *>(v1::delete_iterator<codec::Date>));
    RegisterMethod(
        "delete_iterator", bool_ty, {iter_string_ty},
        reinterpret_cast<void *>(v1::delete_iterator<codec::StringRef>));
    RegisterMethod("delete_iterator", bool_ty, {iter_row_ty},
                   reinterpret_cast<void *>(v1::delete_iterator<codec::Row>));
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
