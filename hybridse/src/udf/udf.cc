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

#include "udf/udf.h"
#include <stdint.h>
#include <time.h>
#include <map>
#include <set>
#include <utility>
#include "base/iterator.h"
#include "boost/date_time.hpp"
#include "boost/date_time/gregorian/parsers.hpp"
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
#include "vm/jit_runtime.h"

namespace hybridse {
namespace udf {
namespace v1 {
using hybridse::base::ConstIterator;
using hybridse::codec::IteratorRef;
using hybridse::codec::ListRef;
using hybridse::codec::ListV;
using hybridse::codec::Row;
using hybridse::codec::StringRef;
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
    try {
        boost::gregorian::date d = boost::gregorian::date_from_tm(t);
        return d.week_number();
    } catch (...) {
        return 0;
    }
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
    try {
        if (month <= 0 || month > 12) {
            return 0;
        } else if (day <= 0 || day > 31) {
            return 0;
        }
        boost::gregorian::date d(year, month, day);
        return d.day_of_week() + 1;
    } catch (...) {
        return 0;
    }
}
// Return the iso 8601 week number 1..53
int32_t weekofyear(codec::Date *date) {
    int32_t day, month, year;
    if (!codec::Date::Decode(date->date_, &year, &month, &day)) {
        return 0;
    }
    try {
        if (month <= 0 || month > 12) {
            return 0;
        } else if (day <= 0 || day > 31) {
            return 0;
        }
        boost::gregorian::date d(year, month, day);
        return d.week_number();
    } catch (...) {
        return 0;
    }
}

float Cotf(float x) { return cosf(x) / sinf(x); }

void date_format(codec::Timestamp *timestamp,
                 hybridse::codec::StringRef *format,
                 hybridse::codec::StringRef *output) {
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
                 hybridse::codec::StringRef *output) {
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
    char *target = udf::v1::AllocManagedStringBuf(output->size_);
    memcpy(target, buffer, output->size_);
    output->data_ = target;
}

void date_format(codec::Date *date, hybridse::codec::StringRef *format,
                 hybridse::codec::StringRef *output) {
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
    try {
        if (month <= 0 || month > 12) {
            return 0;
        } else if (day <= 0 || day > 31) {
            return 0;
        }
        boost::gregorian::date g_date(year, month, day);
        tm t = boost::gregorian::to_tm(g_date);
        strftime(buffer, size, format, &t);
        return true;
    } catch (...) {
        if (size > 0) {
            *buffer = '\0';
        }
        return false;
    }
}

void date_format(codec::Date *date, const std::string &format,
                 hybridse::codec::StringRef *output) {
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
    char *target = udf::v1::AllocManagedStringBuf(output->size_);
    memcpy(target, buffer, output->size_);
    output->data_ = target;
}

void timestamp_to_string(codec::Timestamp *v,
                         hybridse::codec::StringRef *output) {
    date_format(v, "%Y-%m-%d %H:%M:%S", output);
}
void bool_to_string(bool v, hybridse::codec::StringRef *output) {
    if (v) {
        char *buffer = AllocManagedStringBuf(4);
        output->size_ = 4;
        memcpy(buffer, "true", output->size_);
        output->data_ = buffer;
    } else {
        char *buffer = AllocManagedStringBuf(5);
        output->size_ = 5;
        memcpy(buffer, "false", output->size_);
        output->data_ = buffer;
    }
}

void timestamp_to_date(codec::Timestamp *timestamp,
                       hybridse::codec::Date *output, bool *is_null) {
    time_t time = (timestamp->ts_ + TZ_OFFSET) / 1000;
    struct tm t;
    if (nullptr == gmtime_r(&time, &t)) {
        *is_null = true;
        return;
    }
    *output = codec::Date(t.tm_year + 1900, t.tm_mon + 1, t.tm_mday);
    *is_null = false;
    return;
}

void date_to_string(codec::Date *date, hybridse::codec::StringRef *output) {
    date_format(date, "%Y-%m-%d", output);
}
void string_to_bool(codec::StringRef *str, bool *out, bool *is_null_ptr) {
    if (nullptr == str) {
        *out = false;
        *is_null_ptr = true;
        return;
    }
    if (0 == str->size_) {
        *out = false;
        *is_null_ptr = true;
        return;
    }

    auto temp = str->ToString();
    if ("y" == temp || "yes" == temp || "1" == temp || "t" == temp ||
        "true" == temp) {
        *out = true;
        *is_null_ptr = false;
    } else if ("n" == temp || "no" == temp || "0" == temp || "f" == temp ||
               "false" == temp) {
        *out = false;
        *is_null_ptr = false;
    } else {
        *out = false;
        *is_null_ptr = true;
    }
    return;
}
void string_to_int(codec::StringRef *str, int32_t *out, bool *is_null_ptr) {
    // init
    *out = 0;
    *is_null_ptr = true;
    if (nullptr == str) {
        return;
    }
    if (0 == str->size_) {
        return;
    }
    try {
        // string -> integer
        // std::string::size_type sz;  // alias of size_t
        // *out = std::stoi(str->ToString(), &sz);
        // if (sz < str->size_) {
        //    *out = 0;
        //    *is_null_ptr = true;
        //    return;
        //}
        std::string str_obj = str->ToString();
        const char *c_str = str_obj.c_str();
        char *end;
        *out = strtol(c_str, &end, 10);
        if (end < c_str + str->size_) {
            *out = 0;
            *is_null_ptr = true;
            return;
        }
        *is_null_ptr = false;
    } catch (...) {
        // error management
        return;
    }
    return;
}
void string_to_smallint(codec::StringRef *str, int16_t *out,
                        bool *is_null_ptr) {
    // init
    *out = 0;
    *is_null_ptr = true;
    if (nullptr == str) {
        return;
    }
    if (0 == str->size_) {
        return;
    }
    try {
        // string -> integer
        // std::string::size_type sz;  // alias of size_t
        // int i = std::stoi(str->ToString(), &sz);
        // if (sz < str->size_) {
        //    *is_null_ptr = true;
        //    return;
        // }
        std::string str_obj = str->ToString();
        const char *c_str = str_obj.c_str();
        char *end;
        int i = strtol(c_str, &end, 10);
        if (end < c_str + str->size_) {
            *is_null_ptr = true;
            return;
        }
        *out = static_cast<int16_t>(i);
        *is_null_ptr = false;
    } catch (...) {
        // error management
        return;
    }
    return;
}
void string_to_bigint(codec::StringRef *str, int64_t *out, bool *is_null_ptr) {
    // init
    *out = 0;
    *is_null_ptr = true;
    if (nullptr == str) {
        return;
    }
    if (0 == str->size_) {
        return;
    }
    try {
        // string -> integer
        // std::string::size_type sz;  // alias of size_t
        // *out = std::stol(str->ToString(), &sz);
        // if (sz < str->size_) {
        //   *out = 0;
        //    *is_null_ptr = true;
        //    return;
        // }
        std::string str_obj = str->ToString();
        const char *c_str = str_obj.c_str();
        char *end;
        *out = strtoll(c_str, &end, 0);
        if (end < c_str + str->size_) {
            *out = 0;
            *is_null_ptr = true;
            return;
        }
        *is_null_ptr = false;
    } catch (...) {
        // error management
        return;
    }
    return;
}
void string_to_float(codec::StringRef *str, float *out, bool *is_null_ptr) {
    // init
    *out = 0;
    *is_null_ptr = true;
    if (nullptr == str) {
        return;
    }
    if (0 == str->size_) {
        return;
    }
    try {
        // string -> integer
        // std::string::size_type sz;  // alias of size_t
        // *out = std::stof(str->ToString(), &sz);
        // if (sz < str->size_) {
        //    *out = 0;
        //    *is_null_ptr = true;
        //    return;
        // }
        std::string str_obj = str->ToString();
        const char *c_str = str_obj.c_str();
        char *end;
        *out = strtof(c_str, &end);
        if (end < c_str + str->size_) {
            *out = 0;
            *is_null_ptr = true;
            return;
        }
        *is_null_ptr = false;
    } catch (...) {
        // error management
        return;
    }
    return;
}
void string_to_double(codec::StringRef *str, double *out, bool *is_null_ptr) {
    // init
    *out = 0;
    *is_null_ptr = true;
    if (nullptr == str) {
        return;
    }
    if (0 == str->size_) {
        return;
    }
    try {
        // string -> integer
        // std::string::size_type sz;  // alias of size_t
        // *out = std::stod(str->ToString(), &sz);
        // if (sz < str->size_) {
        //    *out = 0;
        //    *is_null_ptr = true;
        //    return;
        // }
        std::string str_obj = str->ToString();
        const char *c_str = str_obj.c_str();
        char *end;
        *out = strtod(c_str, &end);
        if (end < c_str + str->size_) {
            *out = 0;
            *is_null_ptr = true;
            return;
        }
        *is_null_ptr = false;
    } catch (...) {
        // error management
        return;
    }
    return;
}
void string_to_date(codec::StringRef *str, hybridse::codec::Date *output,
                    bool *is_null) {
    if (19 == str->size_) {
        struct tm timeinfo;
        if (nullptr ==
            strptime(str->ToString().c_str(), "%Y-%m-%d %H:%M:%S", &timeinfo)) {
            *is_null = true;
            return;
        } else {
            if (timeinfo.tm_year < 0) {
                *is_null = true;
                return;
            }
            *output = hybridse::codec::Date(
                timeinfo.tm_year + 1900, timeinfo.tm_mon + 1, timeinfo.tm_mday);
            *is_null = false;
            return;
        }
    } else if (10 == str->size_) {
        try {
            auto g_date = boost::gregorian::from_simple_string(str->ToString());
            auto ymd = g_date.year_month_day();
            if (ymd.year < 1900) {
                *is_null = true;
                return;
            }
            *output = hybridse::codec::Date(ymd.year, ymd.month, ymd.day);
            *is_null = false;
        } catch (...) {
            *is_null = true;
            return;
        }
    } else if (8 == str->size_) {
        try {
            auto g_date =
                boost::gregorian::date_from_iso_string(str->ToString());
            auto ymd = g_date.year_month_day();
            if (ymd.year < 1900) {
                *is_null = true;
                return;
            }
            *output = hybridse::codec::Date(ymd.year, ymd.month, ymd.day);
            *is_null = false;
        } catch (...) {
            *is_null = true;
            return;
        }
    } else {
        *is_null = true;
        return;
    }
    return;
}
// cast string to timestamp with yyyy-mm-dd or YYYY-mm-dd HH:MM:SS
void string_to_timestamp(codec::StringRef *str,
                         hybridse::codec::Timestamp *output, bool *is_null) {
    if (19 == str->size_) {
        struct tm timeinfo;
        if (nullptr ==
            strptime(str->ToString().c_str(), "%Y-%m-%d %H:%M:%S", &timeinfo)) {
            *is_null = true;
            return;
        } else {
            if (timeinfo.tm_year < 0) {
                *is_null = true;
                return;
            }
            timeinfo.tm_isdst = -1;  // disable daylight saving for mktime()
            output->ts_ =
                (mktime(&timeinfo) + timeinfo.tm_gmtoff) * 1000 - TZ_OFFSET;
            *is_null = false;
        }
    } else if (10 == str->size_) {
        try {
            auto g_date = boost::gregorian::from_simple_string(str->ToString());
            tm t = boost::gregorian::to_tm(g_date);
            if (t.tm_year < 0) {
                *is_null = true;
                return;
            }
            output->ts_ = (mktime(&t) + t.tm_gmtoff) * 1000 - TZ_OFFSET;
            *is_null = false;
        } catch (...) {
            *is_null = true;
            return;
        }
    } else if (8 == str->size_) {
        try {
            auto g_date =
                boost::gregorian::date_from_iso_string(str->ToString());
            tm t = boost::gregorian::to_tm(g_date);
            if (t.tm_year < 0) {
                *is_null = true;
                return;
            }
            output->ts_ = (mktime(&t) + t.tm_gmtoff) * 1000 - TZ_OFFSET;
            *is_null = false;
        } catch (...) {
            *is_null = true;
            return;
        }
    } else {
        *is_null = true;
        return;
    }
    return;
}
void date_to_timestamp(codec::Date *date, hybridse::codec::Timestamp *output,
                       bool *is_null) {
    int32_t day, month, year;
    if (!codec::Date::Decode(date->date_, &year, &month, &day)) {
        *is_null = true;
        return;
    }
    try {
        if (month <= 0 || month > 12) {
            *is_null = true;
            return;
        } else if (day <= 0 || day > 31) {
            *is_null = true;
            return;
        }
        boost::gregorian::date g_date(year, month, day);
        tm t = boost::gregorian::to_tm(g_date);
        if (t.tm_year < 0) {
            *is_null = true;
            return;
        }
        output->ts_ = (mktime(&t) + t.tm_gmtoff) * 1000 - TZ_OFFSET;
        *is_null = false;
        return;
    } catch (...) {
        *is_null = true;
        return;
    }
}
void sub_string(hybridse::codec::StringRef *str, int32_t from,
                hybridse::codec::StringRef *output) {
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
void sub_string(hybridse::codec::StringRef *str, int32_t from, int32_t len,
                hybridse::codec::StringRef *output) {
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
int32_t strcmp(hybridse::codec::StringRef *s1, hybridse::codec::StringRef *s2) {
    if (s1 == s2) {
        return 0;
    }
    if (nullptr == s1) {
        return -1;
    }
    if (nullptr == s2) {
        return 1;
    }
    return hybridse::codec::StringRef::compare(*s1, *s2);
}

//

template <>
uint32_t to_string_len<int16_t>(const int16_t &v) {
    return std::to_string(v).size();
}

template <>
uint32_t to_string_len<int32_t>(const int32_t &v) {
    return std::to_string(v).size();
}

template <>
uint32_t to_string_len<int64_t>(const int64_t &v) {
    return std::to_string(v).size();
}

template <>
uint32_t to_string_len<float>(const float &v) {
    return std::to_string(v).size();
}

template <>
uint32_t to_string_len<double>(const double &v) {
    return std::to_string(v).size();
}

template <>
uint32_t to_string_len<codec::Date>(const codec::Date &v) {
    const uint32_t len = 10;  // 1990-01-01
    return len;
}

template <>
uint32_t to_string_len<codec::Timestamp>(const codec::Timestamp &v) {
    const uint32_t len = 19;  // "%Y-%m-%d %H:%M:%S"
    return len;
}

template <>
uint32_t to_string_len<std::string>(const std::string &v) {
    return v.size();
}

template <>
uint32_t to_string_len<codec::StringRef>(const codec::StringRef &v) {
    return v.size_;
}

////
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
    return snprintf(buffer, size, "%lld",
                    static_cast<long long int>(v));  // NOLINT
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
    if (buffer == nullptr) return len;
    if (size >= len) {
        date_format(&v, "%Y-%m-%d", buffer, size);
    }
    return len;
}

template <>
uint32_t format_string<codec::Timestamp>(const codec::Timestamp &v,
                                         char *buffer, size_t size) {
    const uint32_t len = 19;  // "%Y-%m-%d %H:%M:%S"
    if (buffer == nullptr) return len;
    if (size >= len) {
        date_format(&v, "%Y-%m-%d %H:%M:%S", buffer, size);
    }
    return len;
}

template <>
uint32_t format_string<std::string>(const std::string &v, char *buffer,
                                    size_t size) {
    if (buffer == nullptr) return v.size();
    return snprintf(buffer, size, "%s", v.c_str());
}

template <>
uint32_t format_string<codec::StringRef>(const codec::StringRef &v,
                                         char *buffer, size_t size) {
    if (buffer == nullptr) return v.size_;
    if (v.size_ < size) {
        memcpy(reinterpret_cast<void *>(buffer),
               reinterpret_cast<const void *>(v.data_), v.size_);
        return v.size_;
    } else {
        memcpy(reinterpret_cast<void *>(buffer),
               reinterpret_cast<const void *>(v.data_), size);
        return size;
    }
}

char *AllocManagedStringBuf(int32_t bytes) {
    if (bytes < 0) {
        return nullptr;
    }
    return reinterpret_cast<char *>(vm::JitRuntime::get()->AllocManaged(bytes));
}

template <class V>
bool iterator_list(int8_t *input, int8_t *output) {
    if (nullptr == input || nullptr == output) {
        return false;
    }
    ::hybridse::codec::ListRef<> *list_ref =
        (::hybridse::codec::ListRef<> *)(input);
    ::hybridse::codec::IteratorRef *iterator_ref =
        (::hybridse::codec::IteratorRef *)(output);
    ListV<V> *col = (ListV<V> *)(list_ref->list);
    auto col_iter = col->GetRawIterator();
    col_iter->SeekToFirst();
    iterator_ref->iterator = reinterpret_cast<int8_t *>(col_iter);
    return true;
}

template <class V>
bool has_next(int8_t *input) {
    if (nullptr == input) {
        return false;
    }
    ::hybridse::codec::IteratorRef *iter_ref =
        (::hybridse::codec::IteratorRef *)(input);
    ConstIterator<uint64_t, V> *iter =
        (ConstIterator<uint64_t, V> *)(iter_ref->iterator);
    return iter == nullptr ? false : iter->Valid();
}

template <class V>
V next_iterator(int8_t *input) {
    ::hybridse::codec::IteratorRef *iter_ref =
        (::hybridse::codec::IteratorRef *)(input);
    ConstIterator<uint64_t, V> *iter =
        (ConstIterator<uint64_t, V> *)(iter_ref->iterator);
    V v = iter->GetValue();
    iter->Next();
    return v;
}

const codec::Row *next_row_iterator(int8_t *input) {
    ::hybridse::codec::IteratorRef *iter_ref =
        (::hybridse::codec::IteratorRef *)(input);
    ConstIterator<uint64_t, codec::Row> *iter =
        (ConstIterator<uint64_t, codec::Row> *)(iter_ref->iterator);
    auto res = &(iter->GetValue());
    iter->Next();
    return res;
}

template <class V>
void next_nullable_iterator(int8_t *input, V *v, bool *is_null) {
    ::hybridse::codec::IteratorRef *iter_ref =
        (::hybridse::codec::IteratorRef *)(input);
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
    ::hybridse::codec::IteratorRef *iter_ref =
        (::hybridse::codec::IteratorRef *)(input);
    ConstIterator<uint64_t, V> *iter =
        (ConstIterator<uint64_t, V> *)(iter_ref->iterator);
    *v = iter->GetValue();
    iter->Next();
    return true;
}
template <class V>
void delete_iterator(int8_t *input) {
    ::hybridse::codec::IteratorRef *iter_ref =
        (::hybridse::codec::IteratorRef *)(input);
    ConstIterator<uint64_t, V> *iter =
        (ConstIterator<uint64_t, V> *)(iter_ref->iterator);
    if (iter) {
        delete iter;
    }
}

}  // namespace v1

bool RegisterMethod(const std::string &fn_name, hybridse::node::TypeNode *ret,
                    std::initializer_list<hybridse::node::TypeNode *> args,
                    void *fn_ptr) {
    node::NodeManager nm;
    base::Status status;
    auto fn_args = nm.MakeFnListNode();
    for (auto &arg : args) {
        fn_args->AddChild(nm.MakeFnParaNode("", arg));
    }
    auto header = dynamic_cast<node::FnNodeFnHeander *>(
        nm.MakeFnHeaderNode(fn_name, fn_args, ret));
    DefaultUdfLibrary::get()->AddExternalFunction(header->GeIRFunctionName(),
                                                  fn_ptr);
    return true;
}

void RegisterNativeUdfToModule() {
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

}  // namespace udf
}  // namespace hybridse
