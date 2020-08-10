/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * udf.h
 *
 * Author: chenjing
 * Date: 2019/11/26
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_UDF_UDF_H_
#define SRC_UDF_UDF_H_
#include <stdint.h>
#include <string>
#include <tuple>

#include "codec/list_iterator_codec.h"
#include "codec/type_codec.h"
#include "proto/fe_type.pb.h"
#include "vm/jit.h"

namespace fesql {
namespace udf {
int8_t *ThreadLocalMemoryPoolAlloc(int32_t request_size);
void ThreadLocalMemoryPoolReset();

namespace v1 {

template <class V>
struct Acos {
    using Args = std::tuple<V>;
    double operator()(V r) { return acos(r); }
};

template <class V>
struct Asin {
    using Args = std::tuple<V>;
    double operator()(V r) { return asin(r); }
};

template <class V>
struct Atan {
    using Args = std::tuple<V>;
    double operator()(V r) { return atan(r); }
};

template <class V>
struct Atan2 {
    using Args = std::tuple<V, V>;
    double operator()(V l, V r) { return atan2(l, r); }
};

template <class V>
struct Ceil {
    using Args = std::tuple<V>;
    V operator()(V r) { return static_cast<V>(ceil(r)); }
};

template <class V>
double avg_list(int8_t *input);

template <class V>
struct AtList {
    using Args = std::tuple<::fesql::codec::ListRef<V>, int32_t>;

    V operator()(::fesql::codec::ListRef<V> *list_ref, int32_t pos) {
        auto list = (codec::ListV<V> *)(list_ref->list);
        return list->At(pos);
    }
};

template <class V>
struct AtStructList {
    using Args = std::tuple<::fesql::codec::ListRef<V>, int32_t>;

    void operator()(::fesql::codec::ListRef<V> *list_ref, int32_t pos, V *v) {
        *v = AtList<V>()(list_ref, pos);
    }
};

template <class V>
struct Minimum {
    using Args = std::tuple<V, V>;

    V operator()(V l, V r) { return l < r ? l : r; }
};

template <class V>
struct Maximum {
    using Args = std::tuple<V, V>;

    V operator()(V l, V r) { return l > r ? l : r; }
};

template <class V>
struct StructMinimum {
    using Args = std::tuple<V, V>;

    void operator()(V *l, V *r, V *res) { *res = (*l < *r) ? *l : *r; }
};

template <class V>
struct StructMaximum {
    using Args = std::tuple<V, V>;

    void operator()(V *l, V *r, V *res) { *res = (*l > *r) ? *l : *r; }
};

template <class V>
bool iterator_list(int8_t *input, int8_t *output);

template <class V>
bool has_next(int8_t *input);

template <class V>
V next_iterator(int8_t *input);

template <class V>
void delete_iterator(int8_t *input);

template <class V>
bool next_struct_iterator(int8_t *input, V *v);

template <class V>
struct IncOne {
    using Args = std::tuple<V>;
    V operator()(V i) { return i + 1; }
};

int32_t month(int64_t ts);
int32_t month(fesql::codec::Timestamp *ts);

int32_t year(int64_t ts);
int32_t year(fesql::codec::Timestamp *ts);

int32_t dayofmonth(int64_t ts);
int32_t dayofmonth(fesql::codec::Timestamp *ts);

int32_t dayofweek(int64_t ts);
int32_t dayofweek(fesql::codec::Timestamp *ts);
int32_t dayofweek(fesql::codec::Date *ts);

int32_t weekofyear(int64_t ts);
int32_t weekofyear(fesql::codec::Timestamp *ts);
int32_t weekofyear(fesql::codec::Date *ts);

int16_t abs_int16(int16_t x);
int64_t abs_int64(int64_t x);

int Ceild(double x);
int Ceilf(float x);

void date_format(codec::Date *date, const std::string &format,
                 fesql::codec::StringRef *output);
void date_format(codec::Timestamp *timestamp, const std::string &format,
                 fesql::codec::StringRef *output);

void date_format(codec::Timestamp *timestamp, fesql::codec::StringRef *format,
                 fesql::codec::StringRef *output);
void date_format(codec::Date *date, fesql::codec::StringRef *format,
                 fesql::codec::StringRef *output);

void timestamp_to_string(codec::Timestamp *timestamp,
                         fesql::codec::StringRef *output);
void date_to_string(codec::Date *date, fesql::codec::StringRef *output);

void sub_string(fesql::codec::StringRef *str, int32_t pos,
                fesql::codec::StringRef *output);
void sub_string(fesql::codec::StringRef *str, int32_t pos, int32_t len,
                fesql::codec::StringRef *output);

template <class V>
struct ToString {
    using Args = std::tuple<V>;

    void operator()(V v, codec::StringRef *output) {
        std::ostringstream ss;
        ss << v;
        output->size_ = ss.str().size();
        output->data_ =
            reinterpret_cast<char *>(ThreadLocalMemoryPoolAlloc(output->size_));
        memcpy(output->data_, ss.str().data(), output->size_);
    }
};
}  // namespace v1

void InitUDFSymbol(vm::FeSQLJIT *jit_ptr);                // NOLINT
void InitUDFSymbol(::llvm::orc::JITDylib &jd,             // NOLINT
                   ::llvm::orc::MangleAndInterner &mi);   // NOLINT
void InitCLibSymbol(vm::FeSQLJIT *jit_ptr);               // NOLINT
void InitCLibSymbol(::llvm::orc::JITDylib &jd,            // NOLINT
                    ::llvm::orc::MangleAndInterner &mi);  // NOLINT
bool AddSymbol(::llvm::orc::JITDylib &jd,                 // NOLINT
               ::llvm::orc::MangleAndInterner &mi,        // NOLINT
               const std::string &fn_name, void *fn_ptr);
void RegisterNativeUDFToModule();
}  // namespace udf
}  // namespace fesql

#endif  // SRC_UDF_UDF_H_
