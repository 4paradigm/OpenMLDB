/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef HYBRIDSE_SRC_UDF_UDF_H_
#define HYBRIDSE_SRC_UDF_UDF_H_
#include <stdint.h>
#include <string>
#include <tuple>
#include "boost/lexical_cast.hpp"
#include "codec/list_iterator_codec.h"
#include "codec/type_codec.h"
#include "node/node_manager.h"
#include "proto/fe_type.pb.h"

namespace hybridse {
namespace udf {

namespace v1 {

template <class V>
struct Abs {
    using Args = std::tuple<V>;

    V operator()(V r) { return static_cast<V>(abs(r)); }
};

template <class V>
struct Abs32 {
    using Args = std::tuple<V>;

    int32_t operator()(V r) { return abs(r); }
};

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

    int64_t operator()(V r) { return static_cast<int64_t>(ceil(r)); }
};

template <class V>
struct Cos {
    using Args = std::tuple<V>;

    double operator()(V r) { return cos(r); }
};

template <class V>
struct Cot {
    using Args = std::tuple<V>;

    double operator()(V r) { return cos(r) / sin(r); }
};

template <class V>
struct Exp {
    using Args = std::tuple<V>;

    double operator()(V r) { return exp(r); }
};

template <class V>
struct Floor {
    using Args = std::tuple<V>;

    int64_t operator()(V r) { return static_cast<int64_t>(floor(r)); }
};

template <class V>
struct Pow {
    using Args = std::tuple<V, V>;

    double operator()(V l, V r) { return pow(l, r); }
};

template <class V>
struct Round {
    using Args = std::tuple<V>;

    V operator()(V r) { return static_cast<V>(round(r)); }
};

template <class V>
struct Round32 {
    using Args = std::tuple<V>;

    int32_t operator()(V r) { return static_cast<int32_t>(round(r)); }
};

template <class V>
struct Sin {
    using Args = std::tuple<V>;

    double operator()(V r) { return sin(r); }
};

template <class V>
struct Tan {
    using Args = std::tuple<V>;

    double operator()(V r) { return tan(r); }
};

template <class V>
struct Sqrt {
    using Args = std::tuple<V>;

    double operator()(V r) { return sqrt(r); }
};

template <class V>
struct Truncate {
    using Args = std::tuple<V>;

    V operator()(V r) { return static_cast<V>(trunc(r)); }
};

template <class V>
struct Truncate32 {
    using Args = std::tuple<V>;

    int32_t operator()(V r) { return static_cast<int32_t>(trunc(r)); }
};

template <class V>
double avg_list(int8_t *input);

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
void next_nullable_iterator(int8_t *input, V *v, bool *is_null);

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
int32_t month(hybridse::codec::Timestamp *ts);

int32_t year(int64_t ts);
int32_t year(hybridse::codec::Timestamp *ts);

int32_t dayofyear(int64_t ts);
int32_t dayofyear(hybridse::codec::Timestamp *ts);
int32_t dayofyear(hybridse::codec::Date *ts);

int32_t dayofmonth(int64_t ts);
int32_t dayofmonth(hybridse::codec::Timestamp *ts);

int32_t dayofweek(int64_t ts);
int32_t dayofweek(hybridse::codec::Timestamp *ts);
int32_t dayofweek(hybridse::codec::Date *ts);

int32_t weekofyear(int64_t ts);
int32_t weekofyear(hybridse::codec::Timestamp *ts);
int32_t weekofyear(hybridse::codec::Date *ts);

float Cotf(float x);

void date_format(codec::Date *date, const std::string &format,
                 hybridse::codec::StringRef *output);
void date_format(codec::Timestamp *timestamp, const std::string &format,
                 hybridse::codec::StringRef *output);

void date_format(codec::Timestamp *timestamp,
                 hybridse::codec::StringRef *format,
                 hybridse::codec::StringRef *output);
void date_format(codec::Date *date, hybridse::codec::StringRef *format,
                 hybridse::codec::StringRef *output);

void timestamp_to_string(codec::Timestamp *timestamp,
                         hybridse::codec::StringRef *output);
void timestamp_to_date(codec::Timestamp *timestamp,
                       hybridse::codec::Date *output, bool *is_null);

void date_to_string(codec::Date *date, hybridse::codec::StringRef *output);

void like(codec::StringRef *name, codec::StringRef *pattern, codec::StringRef *escape, bool *out, bool *is_null);
void like(codec::StringRef *name, codec::StringRef *pattern, bool *out, bool *is_null);
void ilike(codec::StringRef *name, codec::StringRef *pattern, codec::StringRef *escape, bool *out, bool *is_null);
void ilike(codec::StringRef *name, codec::StringRef *pattern, bool *out, bool *is_null);

void date_to_timestamp(codec::Date *date, hybridse::codec::Timestamp *output,
                       bool *is_null);
void string_to_date(codec::StringRef *str, hybridse::codec::Date *output,
                    bool *is_null);
void string_to_timestamp(codec::StringRef *str,
                         hybridse::codec::Timestamp *output, bool *is_null);
void sub_string(hybridse::codec::StringRef *str, int32_t pos,
                hybridse::codec::StringRef *output);
void sub_string(hybridse::codec::StringRef *str, int32_t pos, int32_t len,
                hybridse::codec::StringRef *output);
int32_t strcmp(hybridse::codec::StringRef *s1, hybridse::codec::StringRef *s2);
void bool_to_string(bool v, hybridse::codec::StringRef *output);
void string_to_bool(codec::StringRef *str, bool *out, bool *is_null_ptr);
void string_to_int(codec::StringRef *str, int32_t *v, bool *is_null_ptr);
void string_to_smallint(codec::StringRef *str, int16_t *v, bool *is_null_ptr);
void string_to_bigint(codec::StringRef *str, int64_t *v, bool *is_null_ptr);
void string_to_float(codec::StringRef *str, float *v, bool *is_null_ptr);
void string_to_double(codec::StringRef *str, double *v, bool *is_null_ptr);
void reverse(codec::StringRef *str, codec::StringRef *output, bool *is_null_ptr);

void ucase(codec::StringRef *str, codec::StringRef *output, bool *is_null_ptr);
/**
 * Allocate string buffer from jit runtime.
 */
char *AllocManagedStringBuf(int32_t bytes);

template <class V>
struct ToString {
    using Args = std::tuple<V>;

    void operator()(V v, codec::StringRef *output) {
        std::ostringstream ss;
        ss << v;
        output->size_ = ss.str().size();
        char *buffer = AllocManagedStringBuf(output->size_);
        memcpy(buffer, ss.str().data(), output->size_);
        output->data_ = buffer;
    }
};

template <typename V>
uint32_t format_string(const V &v, char *buffer, size_t size);

template <typename V>
uint32_t to_string_len(const V &v);

}  // namespace v1

void RegisterNativeUdfToModule(hybridse::node::NodeManager* nm);
}  // namespace udf
}  // namespace hybridse

#endif  // HYBRIDSE_SRC_UDF_UDF_H_
