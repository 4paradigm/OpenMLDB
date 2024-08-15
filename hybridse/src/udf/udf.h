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
#include <utility>

#include "absl/status/status.h"
#include "absl/strings/ascii.h"
#include "base/string_ref.h"
#include "base/type.h"
#include "udf/literal_traits.h"
#include "udf/openmldb_udf.h"

namespace hybridse {
namespace udf {
using openmldb::base::StringRef;
using openmldb::base::Date;
using openmldb::base::Timestamp;
using openmldb::base::UDFContext;

namespace v1 {

// ============================================= //
//       Object Lieftime Management
//           Helper Classes
// ============================================= //

// a meta class stores all allocation info of a 'Arrayref<T>' instance
//
// In UDF system e.g when a external udf function has a `ArrayRef<T>*` as last parameter
// for return info, the `ArrayRef<T>*` object require allocation before use, which are
// `ArrayRef<T>::raw` and `ArrayRef<T>::nullables` as two arrays.
//
// The two arrays' lifetime will managed by JitRuntime:
// - Allocate outside with new/malloc operation
// - Register with `RegisterManagedObj`
// - Free in `JitRuntime::ReleaseRunStep`
template <typename T, typename CType = typename DataTypeTrait<T>::CCallArgType>
struct ArrayMeta : public base::FeBaseObject {
    ArrayMeta(CType *r, bool *nullable) : base::FeBaseObject(), raw(r), nullables(nullable) {}
    ~ArrayMeta() override {
        delete[] raw;
        delete[] nullables;
    }

    CType *raw;
    bool *nullables;
};

// OpaqueMeta
// Mange allocated container through Jit runtime only for T is pointer.
// This is useful when allocated a `ArrayRef<T>` parameter from a external udf call
template <typename T, std::enable_if_t<std::is_pointer_v<T>, int> = 0>
struct OpaqueMeta : public base::FeBaseObject {
    explicit OpaqueMeta(T i) : inst(i) {}
    ~OpaqueMeta() override {
        delete inst;
    }

    T inst;
};

// ============================================= //
//       Object Lieftime Management
//              Interface
// ============================================= //

// register the obj to jit runtime
void RegisterManagedObj(base::FeBaseObject* obj);

/**
 * Allocate string buffer from jit runtime.
 */
char *AllocManagedStringBuf(int32_t bytes);

// alloc necessary space for ArrayRef and let Jit runtime manage its lifetime
//
// type T should be of UDF type systems: bool/intxx/float/double/StringRef/TimeStamp/Date
template <typename T>
void AllocManagedArray(ArrayRef<T>* arr, uint64_t sz) {
    assert(arr != nullptr);

    if (sz == 0) {
        return;
    }

    using CType = typename std::remove_pointer_t<decltype(ArrayRef<T>::raw)>;

    CType* raw = new CType[sz];

    // alloc space for array elements if they are pointers
    if constexpr (std::is_pointer_v<CType>) {
        for (size_t i = 0; i < sz; ++i) {
            CType inst = CCallDataTypeTrait<CType>::alloc_instance();

            RegisterManagedObj(new OpaqueMeta<CType>(inst));

            raw[i] = inst;
        }
    }

    auto nulls = new bool[sz];

    RegisterManagedObj(new ArrayMeta<T>(raw, nulls));

    arr->raw = raw;
    arr->nullables = nulls;
    arr->size = sz;
}


// ============================================= //
//       External UDF defines
// ============================================= //

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
    using Args = std::tuple<V, int32_t>;

    V operator()(V val, int32_t decimal_number) {
        if constexpr (std::is_integral_v<V>) {
            if (decimal_number >= 0) {
                return val;
            } else {
                double factor = std::pow(10, -decimal_number);
                return static_cast<V>(std::round(val / factor) * factor);
            }
        } else {
            // floats
            return static_cast<V>(std::round(val * std::pow(10, decimal_number)) / std::pow(10, decimal_number));
        }
    }
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

int64_t FarmFingerprint(absl::string_view input);
template <typename T>
struct Hash64 {
    using Args = std::tuple<T>;

    using ParamType = typename DataTypeTrait<T>::CCallArgType;

    int64_t operator()(ParamType v) {
        return FarmFingerprint(CCallDataTypeTrait<ParamType>::to_bytes_ref(&v));
    }
};

int32_t month(int64_t ts);
int32_t month(Timestamp *ts);

int32_t year(int64_t ts);
int32_t year(Timestamp *ts);

void dayofyear(int64_t ts, int32_t* out, bool* is_null);
void dayofyear(Timestamp *ts, int32_t* out, bool* is_null);
void dayofyear(Date *ts, int32_t* out, bool* is_null);

int32_t dayofmonth(int64_t ts);
int32_t dayofmonth(Timestamp *ts);

int32_t dayofweek(int64_t ts);
int32_t dayofweek(Timestamp *ts);
int32_t dayofweek(Date *ts);

int32_t weekofyear(int64_t ts);
int32_t weekofyear(Timestamp *ts);
int32_t weekofyear(Date *ts);

void last_day(int64_t ts, Date *output, bool *is_null);
void last_day(const Timestamp *ts, Date *output, bool *is_null);
void last_day(const Date *ts, Date *output, bool *is_null);

void int_to_char(int32_t, StringRef*);
int32_t char_length(StringRef *str);

float Cotf(float x);
double Degrees(double x);

void date_format(Date *date, const std::string &format,
                 StringRef *output);
void date_format(Timestamp *timestamp, const std::string &format,
                 StringRef *output);

void date_format(Timestamp *timestamp,
                 StringRef *format,
                 StringRef *output);
void date_format(Date *date, StringRef *format,
                 StringRef *output);

void timestamp_to_string(Timestamp *timestamp,
                         StringRef *output);
void timestamp_to_date(Timestamp *timestamp, Date *output, bool *is_null);

void date_to_string(Date *date, StringRef *output);

void date_diff(Date *date1, Date *date2, int32_t *diff, bool *is_null);
void date_diff(StringRef *date1, StringRef *date2, int32_t *diff, bool *is_null);
void date_diff(StringRef *date1, Date *date2, int32_t *diff, bool *is_null);
void date_diff(Date *date1, StringRef *date2, int32_t *diff, bool *is_null);

void like(StringRef *name, StringRef *pattern,
        StringRef *escape, bool *out, bool *is_null);
void like(StringRef *name, StringRef *pattern, bool *out, bool *is_null);
void ilike(StringRef *name, StringRef *pattern,
        StringRef *escape, bool *out, bool *is_null);
void ilike(StringRef *name, StringRef *pattern, bool *out, bool *is_null);

void regexp_like(StringRef *name, StringRef *pattern, StringRef *flags, bool *out, bool *is_null);
void regexp_like(StringRef *name, StringRef *pattern, bool *out, bool *is_null);

void date_to_timestamp(Date *date, Timestamp *output, bool *is_null);
void string_to_date(StringRef *str, Date *output, bool *is_null);
absl::StatusOr<absl::Time> string_to_time(absl::string_view str);

void string_to_timestamp(StringRef *str, Timestamp *output, bool *is_null);
void date_to_unix_timestamp(Date *date, int64_t *output, bool *is_null);
void string_to_unix_timestamp(StringRef *str, int64_t *output, bool *is_null);
int64_t unix_timestamp();
void sub_string(StringRef *str, int32_t pos,
                StringRef *output);
void sub_string(StringRef *str, int32_t pos, int32_t len,
                StringRef *output);
int32_t locate(StringRef *substr, StringRef* str);
int32_t locate(StringRef *substr, StringRef* str, int32_t pos);
int32_t strcmp(StringRef *s1, StringRef *s2);
void bool_to_string(bool v, StringRef *output);

// transform string into integral
// base is default to 10, 16 if string starts with '0x' or '0X', other base is not considered.
// for hex string, it's parsed unsigned, add minus('-') to the very begining for negative hex
// returns (status, int64)
struct StrToIntegral {
    std::pair<absl::Status, int64_t> operator()(absl::string_view in) {
        in = absl::StripAsciiWhitespace(in);
        if (0 == in.size()) {
            return {absl::InvalidArgumentError("empty or blank string"), {}};
        }

        // reset errno to 0 before call
        errno = 0;

        int base = 10;
        auto copy = in;
        if (copy[0] == '+' || copy[0] == '-') {
            copy.remove_prefix(1);
        }
        copy = copy.substr(0, 2);
        if (copy == "0x" || copy == "0X") {
            base = 16;
        }

        std::string copy_of_str(in);
        const char *str = copy_of_str.data();
        char *endptr = NULL;
        // always to int64
        int64_t number = std::strtoll(str, &endptr, base);

        if (str == endptr) {
            //  invalid: no digits found
            return {absl::InvalidArgumentError(absl::StrCat(in, " (no digitals found)")), {}};
        } else if (errno == ERANGE && number == LLONG_MIN) {
            return {absl::OutOfRangeError(absl::StrCat(in, " (underflow)")), number};
        } else if (errno == ERANGE && number == LLONG_MAX) {
            return {absl::OutOfRangeError(absl::StrCat(in, " (overflow)")), number};
        } else if (errno == EINVAL) {
            // not copy_of_str all c99 implementations - gcc OK
            return {absl::InvalidArgumentError(absl::StrCat(in, " (base contains unsupported value)")), {}};
        } else if (errno != 0 && number == 0) {
            // invalid:  unspecified error occurred
            return {absl::UnknownError(absl::StrCat(in, " (unspecified error: ", std::strerror(errno), ")")), {}};
        } else if (errno == 0 && str) {
            if (*endptr != 0) {
                return {
                    absl::InvalidArgumentError(absl::StrCat(in, " (digitals with extra non-space chars following)")),
                    {}};
            }
            // valid  (and represents all characters read;
            return {absl::OkStatus(), number};
        }

        return {absl::UnknownError(absl::StrCat(in, " (unspecified error)")), {}};
    }
};

void string_to_bool(StringRef *str, bool *out, bool *is_null_ptr);
void string_to_int(StringRef *str, int32_t *v, bool *is_null_ptr);
void string_to_smallint(StringRef *str, int16_t *v, bool *is_null_ptr);
void string_to_bigint(StringRef *str, int64_t *v, bool *is_null_ptr);
void string_to_float(StringRef *str, float *v, bool *is_null_ptr);
void string_to_double(StringRef *str, double *v, bool *is_null_ptr);
void reverse(StringRef *str, StringRef *output, bool *is_null_ptr);
void lcase(StringRef *str, StringRef *output, bool *is_null_ptr);
void ucase(StringRef *str, StringRef *output, bool *is_null_ptr);

/// \brief string replace, return new string that replace all occurrences of `search` with `replace`
/// any of `str`, `search`, `replace` is null, results replace string to be null;
/// if `search` is not found, return `str` unchanged;
/// if `replace` not specified or is empty string, occurrences of `search` are removed from output string
///
/// \param str Input string
/// \param search The search string
/// \param replace The replace string
/// \param output Output replaced String
/// \param is_null_ptr Output if replaced string is null
void replace(StringRef *str, StringRef *search, StringRef *replace, StringRef *output, bool *is_null_ptr);
void replace(StringRef *str, StringRef *search, StringRef *output, bool *is_null_ptr);

void init_udfcontext(UDFContext* context);
void trivial_fun();

template <class V>
struct ToString {
    using Args = std::tuple<V>;

    void operator()(V v, StringRef *output) {
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

template <class V>
struct ToHex {
    using Args = std::tuple<V>;

    void operator()(V v, StringRef *output) {
        std::ostringstream ss;
        if (std::is_same<V, float>::value || std::is_same<V, double>::value) {
            int64_t numbuf = std::llround(v);
            ss << std::hex << std::uppercase << numbuf;
        } else {
            ss << std::hex << std::uppercase << v;
        }
        std::string hexstr = ss.str();
        output->size_ = hexstr.size();
        char *buffer = AllocManagedStringBuf(output->size_);
        memcpy(buffer, hexstr.data(), output->size_);
        output->data_ = buffer;
    }
};
void hex(StringRef *str, StringRef *output);

void unhex(StringRef *str, StringRef *output, bool* is_null);

void printLog(const char* fmt);
void array_combine(codec::StringRef *del, int32_t cnt, ArrayRef<codec::StringRef> **data,
                   ArrayRef<codec::StringRef> *out);

template <typename T>
struct ArrayPadding {

    using Args = std::tuple<ArrayRef<T>, int32_t, T>;

    bool operator()(ArrayRef<T> *arr, int32_t target_size, T default_value, ArrayRef<T> *out, bool *is_null) {
        if (target_size < 0) {
            out->size = 0;
            out->raw = nullptr;
            out->nullables = nullptr;
        }
        else if (arr->size >= target_size) {
            // v1::AllocManagedArray(out, arr->size);
            out->size = arr->size;
            for (int i=0; i<arr->size; ++i) {
                out->raw[i] = arr->raw[i];
                out->nullables[i] = arr->nullables[i];
            }
        }
        else {
            // v1::AllocManagedArray(out, target_size);
            out->size = target_size;
            for (int i = 0; i < target_size; ++i) {
                if (i < arr->size) {
                    out->nullables[i] = arr->nullables[i];
                    out->raw[i] = arr->raw[i];
                } else {
                    out->nullables[i] = false;
                    out->raw[i] = default_value;
                    // out->raw[i]->data_ = default_value.data_;
                    // out->raw[i]->size_ = default_value.size_;
                }
            }
        }
};
};

}  // namespace v1

/// \brief register native udf related methods into given UdfLibrary `lib`
void RegisterNativeUdfToModule(UdfLibrary* lib);
}  // namespace udf
}  // namespace hybridse

#endif  // HYBRIDSE_SRC_UDF_UDF_H_
