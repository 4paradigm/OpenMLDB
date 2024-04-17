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

#include <ctime>
#include <utility>

#include "absl/strings/ascii.h"
#include "absl/strings/str_replace.h"
#include "absl/time/civil_time.h"
#include "absl/time/time.h"
#include "base/iterator.h"
#include "boost/date_time/gregorian/conversion.hpp"
#include "boost/date_time/gregorian/parsers.hpp"
#include "bthread/types.h"
#include "codec/row.h"
#include "codec/type_codec.h"
#include "farmhash.h"
#include "node/node_manager.h"
#include "node/sql_node.h"
#include "re2/re2.h"
#include "udf/literal_traits.h"
#include "udf/udf_library.h"
#include "vm/jit_runtime.h"

namespace hybridse {
namespace udf {
namespace v1 {
using hybridse::base::ConstIterator;
using hybridse::codec::IteratorRef;
using hybridse::codec::ListRef;
using hybridse::codec::ListV;
using hybridse::codec::Row;
using openmldb::base::StringRef;
using openmldb::base::Timestamp;
using openmldb::base::Date;

// strftime()-like formatting options with extensions
// ref absl::FormatTime
static constexpr char DATE_FMT_YMD_1[] = "%E4Y-%m-%d";
static constexpr char DATE_FMT_YMD_2[] = "%E4Y%m%d";
static constexpr char DATE_FMT_YMDHMS[] = "%E4Y-%m-%d %H:%M:%S";
static constexpr char DATE_FMT_RF3399_FULL[] = "%Y-%m-%d%ET%H:%M:%E*S%Ez";

// TODO(chenjing): 时区统一配置
static constexpr int32_t TZ = 8;
static const absl::TimeZone DEFAULT_TZ = absl::FixedTimeZone(TZ * 60 * 60);
static constexpr time_t TZ_OFFSET = TZ * 3600000;
static constexpr int MAX_ALLOC_SIZE = 2 * 1024 * 1024;  // 2M
bthread_key_t B_THREAD_LOCAL_MEM_POOL_KEY;

void hex(StringRef *str, StringRef *output) {
    std::ostringstream ss;
    for (uint32_t i=0; i < str->size_; i++) {
        ss << std::hex << std::uppercase << static_cast<int>(str->data_[i]);
    }
    output->size_ = ss.str().size();
    char *buffer = AllocManagedStringBuf(output->size_);
    memcpy(buffer, ss.str().data(), output->size_);
    output->data_ = buffer;
}

void unhex(StringRef *str, StringRef *output, bool* is_null) {
    char *buffer = AllocManagedStringBuf(str->size_ / 2 + str->size_ % 2);
    for (uint32_t i = 0; i < str->size_; ++i) {
        if ((str->data_[i] >= 'A' && str->data_[i] <= 'F') ||
            (str->data_[i] >= 'a' && str->data_[i] <= 'f') ||
            (str->data_[i] >= '0' && str->data_[i] <= '9')) {
            continue;
        } else {
            *is_null = true;
            break;
        }
    }
    // use lambda function to convert the char to uint8
    auto convert = [](char a) -> int {
        if (a <= 'F' && a >= 'A') { return a - 'A' + 10; }
        if (a <= 'f' && a >= 'a') { return a - 'a' + 10; }
        if (a <= '9' && a >= '0') { return a - '0'; }
        return 0;
    };

    if (!*is_null) {    // every character is valid hex character
        if (str->size_ % 2 == 0) {
            for (uint32_t i = 0; i < str->size_; i += 2) {
                buffer[i / 2] = static_cast<char>(convert(str->data_[i]) << 4 | convert(str->data_[i + 1]));
            }
        } else {
            buffer[0] = static_cast<char>(convert(str->data_[0]));
            for (uint32_t i = 1; i < str->size_; i += 2) {
                buffer[i / 2 + 1] = static_cast<char>(convert(str->data_[i]) << 4 | convert(str->data_[i + 1]));
            }
        }
        output->size_ = str->size_ / 2 + str->size_ % 2;
        output->data_ = buffer;
    }
}

void trivial_fun() {}

void dayofyear(int64_t ts, int32_t* out, bool* is_null) {
    if (ts < 0) {
        *is_null = true;
        *out = 0;
        return;
    }

    time_t time = (ts + TZ_OFFSET) / 1000;
    struct tm t;
    memset(&t, 0, sizeof(struct tm));
    gmtime_r(&time, &t);

    *out = t.tm_yday + 1;
    *is_null = false;
}
int32_t dayofmonth(int64_t ts) {
    time_t time = (ts + TZ_OFFSET) / 1000;
    struct tm t;
    memset(&t, 0, sizeof(struct tm));
    gmtime_r(&time, &t);
    return t.tm_mday;
}
int32_t dayofweek(int64_t ts) {
    time_t time = (ts + TZ_OFFSET) / 1000;
    struct tm t;
    memset(&t, 0, sizeof(struct tm));
    gmtime_r(&time, &t);
    return t.tm_wday + 1;
}
int32_t weekofyear(int64_t ts) {
    time_t time = (ts + TZ_OFFSET) / 1000;
    struct tm t;
    memset(&t, 0, sizeof(struct tm));
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
    memset(&t, 0, sizeof(struct tm));
    gmtime_r(&time, &t);
    return t.tm_mon + 1;
}
int32_t year(int64_t ts) {
    time_t time = (ts + TZ_OFFSET) / 1000;
    struct tm t;
    memset(&t, 0, sizeof(struct tm));
    gmtime_r(&time, &t);
    return t.tm_year + 1900;
}

void dayofyear(Timestamp *ts, int32_t *out, bool *is_null) { dayofyear(ts->ts_, out, is_null); }
void dayofyear(Date *date, int32_t* out, bool* is_null) {
    int32_t day, month, year;
    if (!Date::Decode(date->date_, &year, &month, &day)) {
        *out = 0;
        *is_null = true;
        return;
    }

    absl::CivilDay civil_day(year, month, day);
    if (civil_day.year() != year || civil_day.month() != month || civil_day.day() != day) {
        // CivilTime normalize it because of invalid input
        *out = 0;
        *is_null = true;
        return;
    }

    *out = absl::GetYearDay(civil_day);
    *is_null = false;
}

int32_t dayofmonth(Timestamp *ts) { return dayofmonth(ts->ts_); }
int32_t weekofyear(Timestamp *ts) { return weekofyear(ts->ts_); }
int32_t month(Timestamp *ts) { return month(ts->ts_); }
int32_t year(Timestamp *ts) { return year(ts->ts_); }
int32_t dayofweek(Timestamp *ts) { return dayofweek(ts->ts_); }
int32_t dayofweek(Date *date) {
    int32_t day, month, year;
    if (!Date::Decode(date->date_, &year, &month, &day)) {
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
int32_t weekofyear(Date *date) {
    int32_t day, month, year;
    if (!Date::Decode(date->date_, &year, &month, &day)) {
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

void last_day(int64_t ts, Date *output, bool *is_null) {
    if (ts < 0) {
        *is_null = true;
        return;
    }
    absl::CivilDay civil_day = absl::ToCivilDay(absl::FromUnixMillis(ts),
                                                absl::FixedTimeZone(TZ_OFFSET / 1000));
    absl::CivilMonth next_month = absl::CivilMonth(civil_day) + 1;
    absl::CivilDay last_day = absl::CivilDay(next_month) - 1;
    *output = Date(static_cast<int32_t>(last_day.year()), last_day.month(), last_day.day());
    *is_null = false;
}
void last_day(const Timestamp *ts, Date *output, bool *is_null) { last_day(ts->ts_, output, is_null); }
void last_day(const Date *ts, Date *output, bool *is_null) {
    int32_t year, month, day;
    if (!Date::Decode(ts->date_, &year, &month, &day)) {
        *is_null = true;
        return;
    }
    absl::CivilDay civil_day(year, month, day);
    if (civil_day.year() != year || civil_day.month() != month || civil_day.day() != day) {
        *is_null = true;
        return;
    }
    absl::CivilMonth next_month = absl::CivilMonth(civil_day) + 1;
    absl::CivilDay last_day = absl::CivilDay(next_month) - 1;
    *output = Date(static_cast<int32_t>(last_day.year()), last_day.month(), last_day.day());
    *is_null = false;
}

void int_to_char(int32_t val, StringRef* output) {
    val = val % 256;
    char v = static_cast<char>(val);
    char *buffer = AllocManagedStringBuf(1);
    output->size_ = 1;
    memcpy(buffer, &v, output->size_);
    output->data_ = buffer;
}
int32_t char_length(StringRef *str) {
    if (nullptr == str) {
        return 0;
    }
    int32_t res = str->size_;
    return res;
}

float Cotf(float x) { return cosf(x) / sinf(x); }

double Degrees(double x) { return x * (180 / M_PI); }

void date_format(Timestamp *timestamp,
                 StringRef *format,
                 StringRef *output) {
    if (nullptr == format) {
        return;
    }
    date_format(timestamp, format->ToString(), output);
}
void date_format(const Timestamp *timestamp, const char *format,
                 char *buffer, size_t size) {
    time_t time = (timestamp->ts_ + TZ_OFFSET) / 1000;
    struct tm t;
    memset(&t, 0, sizeof(struct tm));
    gmtime_r(&time, &t);
    strftime(buffer, size, format, &t);
}
void date_format(Timestamp *timestamp, const std::string &format,
                 StringRef *output) {
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

void date_format(Date *date, StringRef *format,
                 StringRef *output) {
    if (nullptr == format) {
        return;
    }
    date_format(date, format->ToString(), output);
}

bool date_format(const Date *date, const char *format, char *buffer,
                 size_t size) {
    int32_t day, month, year;
    if (!Date::Decode(date->date_, &year, &month, &day)) {
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

void date_format(Date *date, const std::string &format,
                 StringRef *output) {
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

void timestamp_to_string(Timestamp *v,
                         StringRef *output) {
    date_format(v, "%Y-%m-%d %H:%M:%S", output);
}
void bool_to_string(bool v, StringRef *output) {
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

void timestamp_to_date(Timestamp *timestamp, Date *output, bool *is_null) {
    time_t time = (timestamp->ts_ + TZ_OFFSET) / 1000;
    struct tm t;
    memset(&t, 0, sizeof(struct tm));
    if (nullptr == gmtime_r(&time, &t)) {
        *is_null = true;
        return;
    }
    *output = Date(t.tm_year + 1900, t.tm_mon + 1, t.tm_mday);
    *is_null = false;
    return;
}

void date_to_string(Date *date, StringRef *output) {
    date_format(date, "%Y-%m-%d", output);
}

/*
* SQL style glob match, use
* - percent sign (%) as zero or more characters
* - underscore (_) as exactly one.
* - backslash (\) as escape character by default
*
* rules:
* - escape
*   - exception(invalid escape character): if escape size >= 2
*   - exception(invalid escape sequence)[TODO]:
*     if <escape character> size = 1, and in pattern string, the follower character of <escape character>
*     is not <escape character>, <underscore> or <precent>
*   - empty string or null value means disable escape
*
* credit:
*  Michael Cook, https://github.com/MichaelCook/glob_match/blob/master/glob_match.cpp
*/
template <typename EQUAL>
bool like_internal(std::string_view name, std::string_view pattern, const char *escape, EQUAL &&equal) {
    auto pattern_it = pattern.cbegin();
    auto pattern_end = pattern.cend();
    auto name_it = name.cbegin();
    auto name_end = name.cend();

    while (pattern_it != pattern_end) {
        if (name_it == name_end) {
            break;
        }

        char c = *pattern_it;
        if (escape != nullptr && c == *escape) {
            // exact character match
            if (std::next(pattern_it) == pattern_end) {
                // the pattern is terminated with escape character, just return false
                return false;
            }
            c = *std::next(pattern_it);
            if (!equal(c, *name_it)) {
                return false;
            }

            std::advance(pattern_it, 2);
        } else {
            switch (c) {
                case '%': {
                    std::advance(pattern_it, 1);
                    auto sub_pattern = std::string_view(pattern_it, std::distance(pattern_it, pattern_end));
                    for (auto back = name_end; back >= name_it; std::advance(back, -1)) {
                        if (like_internal(std::string_view(back, std::distance(back, name_end)),
                                          sub_pattern, escape, std::forward<EQUAL>(equal))) {
                            return true;
                        }
                    }
                    return false;
                }
                case '_': {
                    break;
                }
                default: {
                    if (!equal(c, *name_it)) {
                        return false;
                    }
                    break;
                }
            }

            std::advance(pattern_it, 1);
        }

        std::advance(name_it, 1);
    }

    if (pattern_it != pattern_end) {
        // when pattern iterator do not reach the end
        // 1. true if there is only special character <percent>s left in pattern
        // 2. false otherwise
        for (; pattern_it != pattern_end; std::advance(pattern_it, 1)) {
            if ((escape != nullptr && *pattern_it == *escape) || *pattern_it != '%') {
                // character under pattern_it is escape character or is not <percent> character, return false
                return false;
            }
        }

        return true;
    }

    return name_it == name_end;
}

/*
* if escape is null or ref to empty string, disable escape feature
*
* nullable
* - any of (name, pattern, escape) is null, return null
*/
template <typename EQUAL>
void like_internal(StringRef *name, StringRef *pattern, StringRef *escape, EQUAL &&equal,
                   bool *out, bool *is_null) {
    if (name == nullptr || pattern == nullptr || escape == nullptr) {
        out = nullptr;
        *is_null = true;
        return;
    }
    std::string_view name_view(name->data_, name->size_);
    std::string_view pattern_view(pattern->data_, pattern->size_);

    *is_null = false;
    const char *esc = nullptr;
    if (escape->size_ > 0) {
        if (escape->size_ >= 2) {
            DLOG(ERROR) << "data exception: invalid escape character '" << escape->ToString() << "'";
            *out = false;
            return;
        }
        esc = escape->data_;
    }
    *out = like_internal(name_view, pattern_view, esc, std::forward<EQUAL>(equal));
}

void like(StringRef *name, StringRef *pattern, StringRef *escape, bool *out,
          bool *is_null) {
    like_internal(
        name, pattern, escape, [](char lhs, char rhs) { return lhs == rhs; }, out, is_null);
}

void like(StringRef* name, StringRef* pattern, bool* out, bool* is_null) {
    static StringRef default_esc(1, "\\");
    like(name, pattern, &default_esc, out, is_null);
}

void ilike(StringRef *name, StringRef *pattern, StringRef *escape, bool *out, bool *is_null) {
    like_internal(
        name, pattern, escape,
        [](char lhs, char rhs) {
            return std::tolower(static_cast<unsigned char>(lhs)) == std::tolower(static_cast<unsigned char>(rhs));
        },
        out, is_null);
}

void ilike(StringRef* name, StringRef* pattern, bool* out, bool* is_null) {
    static StringRef default_esc(1, "\\");
    ilike(name, pattern, &default_esc,  out, is_null);
}


// The options are (defaults in parentheses):
//
//   utf8             (true)  text and pattern are UTF-8; otherwise Latin-1
//   posix_syntax     (false) restrict regexps to POSIX egrep syntax
//   longest_match    (false) search for longest match, not first match
//   log_errors       (true)  log syntax and execution errors to ERROR
//   max_mem          (see below)  approx. max memory footprint of RE2
//   literal          (false) interpret string as literal, not regexp
//   never_nl         (false) never match \n, even if it is in regexp
//   dot_nl           (false) dot matches everything including new line
//   never_capture    (false) parse all parens as non-capturing
//   case_sensitive   (true)  match is case-sensitive (regexp can override
//                              with (?i) unless in posix_syntax mode)
//
// The following options are only consulted when posix_syntax == true.
// When posix_syntax == false, these features are always enabled and
// cannot be turned off; to perform multi-line matching in that case,
// begin the regexp with (?m).
//   perl_classes     (false) allow Perl's \d \s \w \D \S \W
//   word_boundary    (false) allow Perl's \b \B (word boundary and not)
//   one_line         (false) ^ and $ only match beginning and end of text
void regexp_like(StringRef *name, StringRef *pattern, StringRef *flags, bool *out, bool *is_null) {
    if (name == nullptr || pattern == nullptr || flags == nullptr) {
        out = nullptr;
        *is_null = true;
        return;
    }

    std::string_view flags_view(flags->data_, flags->size_);
    std::string_view pattern_view(pattern->data_, pattern->size_);
    std::string_view name_view(name->data_, name->size_);

    RE2::Options opts(RE2::POSIX);
    opts.set_log_errors(false);
    opts.set_one_line(true);

    for (auto &flag : flags_view) {
        switch (flag) {
            case 'c':
                opts.set_case_sensitive(true);
            break;
            case 'i':
                opts.set_case_sensitive(false);
            break;
            case 'm':
                opts.set_one_line(false);
            break;
            case 'e':
                // ignored here
            break;
            case 's':
                opts.set_dot_nl(true);
            break;
            // ignore unknown flag
        }
    }

    RE2 re(pattern_view, opts);
    if (re.error_code() != 0) {
        LOG(ERROR) << "Error parsing '" << pattern_view << "': " << re.error();
        out = nullptr;
        *is_null = true;
        return;
    }
    *is_null = false;
    *out = RE2::FullMatch(name_view, re);
}

void regexp_like(StringRef *name, StringRef *pattern, bool *out, bool *is_null) {
    StringRef flags("c");
    regexp_like(name, pattern, &flags, out, is_null);
}

void string_to_bool(StringRef *str, bool *out, bool *is_null_ptr) {
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
    boost::to_lower(temp);
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
void string_to_int(StringRef *str, int32_t *out, bool *is_null_ptr) {
    if (nullptr == str) {
        *is_null_ptr = true;
        return;
    }
    auto [s, ret] = StrToIntegral()(str->ToString());
    if (s.ok() && ret <= INT32_MAX && ret >= INT32_MIN) {
        *is_null_ptr = false;
        *out = ret;
    } else {
        *is_null_ptr = true;
    }
}
void string_to_smallint(StringRef *str, int16_t *out,
                        bool *is_null_ptr) {
    if (nullptr == str) {
        *is_null_ptr = true;
        return;
    }

    auto [s, ret] = StrToIntegral()(str->ToString());
    if (s.ok() && ret >= INT16_MIN && ret <= INT16_MAX) {
        *is_null_ptr = false;
        *out = ret;
    } else {
        *is_null_ptr = true;
    }
}

void string_to_bigint(StringRef *str, int64_t *out, bool *is_null_ptr) {
    if (nullptr == str) {
        *is_null_ptr = true;
        return;
    }

    auto [s, ret] = StrToIntegral()(str->ToString());
    if (s.ok()) {
        *is_null_ptr = false;
        *out = ret;
    } else {
        *is_null_ptr = true;
    }
}
void string_to_float(StringRef *str, float *out, bool *is_null_ptr) {
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
void string_to_double(StringRef *str, double *out, bool *is_null_ptr) {
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
void string_to_date(StringRef *str, Date *output, bool *is_null) {
    if (19 == str->size_) {
        struct tm timeinfo;
        memset(&timeinfo, 0, sizeof(struct tm));
        if (nullptr ==
            strptime(str->ToString().c_str(), "%Y-%m-%d %H:%M:%S", &timeinfo)) {
            *is_null = true;
            return;
        } else {
            if (timeinfo.tm_year < 0) {
                *is_null = true;
                return;
            }
            *output = Date(
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
            *output = Date(ymd.year, ymd.month, ymd.day);
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
            *output = Date(ymd.year, ymd.month, ymd.day);
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

absl::StatusOr<absl::Time> string_to_time(absl::string_view ref) {
    absl::string_view fmt = DATE_FMT_RF3399_FULL;
    if (19 == ref.size()) {
        fmt = DATE_FMT_YMDHMS;
    } else if (10 == ref.size()) {
        fmt = DATE_FMT_YMD_1;
    } else if (8 == ref.size()) {
        fmt = DATE_FMT_YMD_2;
    }
    absl::Time tm;
    std::string err;
    bool ret = absl::ParseTime(fmt, ref, &tm, &err);

    if (!ret) {
        return absl::InvalidArgumentError(err);
    }
    return tm;
}

void date_diff(Date *date1, Date *date2, int32_t *diff, bool *is_null) {
    if (date1 == nullptr || date2 == nullptr || date1->date_ <= 0 || date2->date_ <= 0) {
        *is_null = true;
        return;
    }
    int32_t year, month, day;
    if (!date1->Decode(date1->date_, &year, &month, &day)) {
        *is_null = true;
        return;
    }
    absl::CivilDay d1(year, month, day);
    if (!date1->Decode(date2->date_, &year, &month, &day)) {
        *is_null = true;
        return;
    }
    absl::CivilDay d2(year, month, day);
    *diff = (d1 - d2);
    *is_null = false;
}

void date_diff(StringRef *date1, StringRef *date2, int32_t *diff, bool *is_null) {
    auto t1 = string_to_time(absl::string_view(date1->data_, date1->size_));
    if (!t1.ok()) {
        *is_null = true;
        return;
    }
    auto t2 = string_to_time(absl::string_view(date2->data_, date2->size_));
    if (!t2.ok()) {
        *is_null = true;
        return;
    }

    auto d1 = absl::ToCivilDay(t1.value(), DEFAULT_TZ);
    auto d2 = absl::ToCivilDay(t2.value(), DEFAULT_TZ);

    *diff = d1 - d2;
    *is_null = false;
}

void date_diff(StringRef *date1, Date *date2, int32_t *diff, bool *is_null) {
    auto t1 = string_to_time(absl::string_view(date1->data_, date1->size_));
    if (!t1.ok()) {
        *is_null = true;
        return;
    }
    auto d1 = absl::ToCivilDay(t1.value(), DEFAULT_TZ);

    int32_t year, month, day;
    if (!Date::Decode(date2->date_, &year, &month, &day)) {
        *is_null = true;
        return;
    }
    auto d2 = absl::CivilDay(year, month, day);

    *diff = d1 - d2;
    *is_null = false;
}

void date_diff(Date *date1, StringRef *date2, int32_t *diff, bool *is_null) {
    auto t2 = string_to_time(absl::string_view(date2->data_, date2->size_));
    if (!t2.ok()) {
        *is_null = true;
        return;
    }
    auto d2 = absl::ToCivilDay(t2.value(), DEFAULT_TZ);

    int32_t year, month, day;
    if (!Date::Decode(date1->date_, &year, &month, &day)) {
        *is_null = true;
        return;
    }
    auto d1 = absl::CivilDay(year, month, day);

    *diff = d1 - d2;
    *is_null = false;
}

// cast string to timestamp with yyyy-mm-dd or YYYY-mm-dd HH:MM:SS
void string_to_timestamp(StringRef *str,
                         Timestamp *output, bool *is_null) {
    if (19 == str->size_) {
        struct tm timeinfo;
        memset(&timeinfo, 0, sizeof(struct tm));
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
void date_to_timestamp(Date *date, Timestamp *output,
                       bool *is_null) {
    int32_t day, month, year;
    if (!Date::Decode(date->date_, &year, &month, &day)) {
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

void date_to_unix_timestamp(Date *date, int64_t *output,
                       bool *is_null) {
    Timestamp ts;
    date_to_timestamp(date, &ts, is_null);
    if (*is_null) {
        return;
    }

    *output = ts.ts_ / 1000;
}

// cast string to unix_timestamp with yyyy-mm-dd or YYYY-mm-dd HH:MM:SS
void string_to_unix_timestamp(StringRef *str, int64_t *output, bool *is_null) {
    if (str == nullptr || str->IsNull() || str->size_ == 0) {
        *output = unix_timestamp();
        *is_null = false;
        return;
    }

    Timestamp ts;
    string_to_timestamp(str, &ts, is_null);
    if (*is_null) {
        return;
    }

    *output = ts.ts_ / 1000;
}

int64_t unix_timestamp() {
    std::time_t t = std::time(nullptr);
    return t;
}

void sub_string(StringRef *str, int32_t from,
                StringRef *output) {
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
void sub_string(StringRef *str, int32_t from, int32_t len,
                StringRef *output) {
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
int32_t strcmp(StringRef *s1, StringRef *s2) {
    if (s1 == s2) {
        return 0;
    }
    if (nullptr == s1) {
        return -1;
    }
    if (nullptr == s2) {
        return 1;
    }
    return StringRef::compare(*s1, *s2);
}

void ucase(StringRef *str, StringRef *output, bool *is_null_ptr) {
    if (str == nullptr || str->size_ == 0 || output == nullptr || is_null_ptr == nullptr) {
        return;
    }
    char *buffer = AllocManagedStringBuf(str->size_);
    if (buffer == nullptr) {
        *is_null_ptr = true;
        return;
    }
    for (uint32_t i = 0; i < str->size_; i++) {
        buffer[i] = absl::ascii_toupper(static_cast<unsigned char>(str->data_[i]));
    }
    output->size_ = str->size_;
    output->data_ = buffer;
    *is_null_ptr = false;
}

void replace(StringRef *str, StringRef *search, StringRef *replace, StringRef *output, bool *is_null_ptr) {
    if (str == nullptr || search == nullptr || replace == nullptr) {
        *is_null_ptr = true;
        return;
    }

    absl::string_view str_view(str->data_, str->size_);
    absl::string_view search_view(search->data_, search->size_);
    absl::string_view replace_view(replace->data_, replace->size_);
    std::string out = absl::StrReplaceAll(str_view, {{search_view, replace_view}});

    char *buf = AllocManagedStringBuf(out.size());
    if (buf == nullptr) {
        *is_null_ptr = true;
        return;
    }
    memcpy(buf, out.data(), out.size());

    output->data_ = buf;
    output->size_ = out.size();
    *is_null_ptr = false;
}

void replace(StringRef *str, StringRef *search, StringRef *output, bool *is_null_ptr) {
    StringRef rep(0, "");
    replace(str, search, &rep, output, is_null_ptr);
}

void reverse(StringRef *str, StringRef *output, bool *is_null_ptr) {
    if (str == nullptr || output == nullptr || is_null_ptr == nullptr) {
        return;
    }
    if (str->size_ == 0) {
        output->data_ = str->data_;
        output->size_ = str->size_;
        return;
    }
    char *buffer = AllocManagedStringBuf(str->size_);
    if (buffer == nullptr) {
        *is_null_ptr = true;
        return;
    }
    for (uint32_t i = 0; i < str->size_; i++) {
        buffer[i] = str->data_[str->size_ - i - 1];
    }
    output->size_ = str->size_;
    output->data_ = buffer;
    *is_null_ptr = false;
}

void lcase(StringRef *str, StringRef *output, bool *is_null_ptr) {
    if (str == nullptr || str->size_ == 0 || output == nullptr || is_null_ptr == nullptr) {
        return;
    }
    char *buffer = AllocManagedStringBuf(str->size_);
    if (buffer == nullptr) {
        *is_null_ptr = true;
        return;
    }
    for (uint32_t i = 0; i < str->size_; i++) {
        buffer[i] = absl::ascii_tolower(static_cast<unsigned char>(str->data_[i]));
    }
    output->size_ = str->size_;
    output->data_ = buffer;
    *is_null_ptr = false;
}

void init_udfcontext(UDFContext* context) {
    context->pool = vm::JitRuntime::get()->GetMemPool();
    context->ptr = nullptr;
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
uint32_t to_string_len<Date>(const Date &v) {
    const uint32_t len = 10;  // 1990-01-01
    return len;
}

template <>
uint32_t to_string_len<Timestamp>(const Timestamp &v) {
    const uint32_t len = 19;  // "%Y-%m-%d %H:%M:%S"
    return len;
}

template <>
uint32_t to_string_len<std::string>(const std::string &v) {
    return v.size();
}

template <>
uint32_t to_string_len<StringRef>(const StringRef &v) {
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
uint32_t format_string<Date>(const Date &v, char *buffer,
                                    size_t size) {
    const uint32_t len = 10;  // 1990-01-01
    if (buffer == nullptr) return len;
    if (size >= len) {
        date_format(&v, "%Y-%m-%d", buffer, size);
    }
    return len;
}

template <>
uint32_t format_string<Timestamp>(const Timestamp &v,
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
uint32_t format_string<StringRef>(const StringRef &v,
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

void RegisterManagedObj(base::FeBaseObject* obj) {
    vm::JitRuntime::get()->AddManagedObject(obj);
}

char *AllocManagedStringBuf(int32_t bytes) {
    if (bytes < 0) {
        return nullptr;
    }
    if (bytes > MAX_ALLOC_SIZE) {
        LOG(ERROR) << "alloc string buf size " << bytes << " is larger than " << MAX_ALLOC_SIZE;
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

int64_t FarmFingerprint(absl::string_view input) {
    return absl::bit_cast<int64_t>(farmhash::Fingerprint64(input));
}

void printLog(const char* fmt) {
    if (fmt) {
        fprintf(stderr, "%s\n", fmt);
    }
}

}  // namespace v1

bool RegisterMethod(UdfLibrary *lib, const std::string &fn_name, hybridse::node::TypeNode *ret,
                    std::initializer_list<hybridse::node::TypeNode *> args, void *fn_ptr) {
    node::NodeManager nm;
    base::Status status;
    auto fn_args = nm.MakeFnListNode();
    for (auto &arg : args) {
        fn_args->AddChild(nm.MakeFnParaNode("", arg));
    }
    auto header = dynamic_cast<node::FnNodeFnHeander *>(
        nm.MakeFnHeaderNode(fn_name, fn_args, ret));
    lib->AddExternalFunction(header->GeIRFunctionName(),
                                                  fn_ptr);
    return true;
}

void RegisterNativeUdfToModule(UdfLibrary* lib) {
    base::Status status;
    hybridse::node::NodeManager* nm = lib->node_manager();

    auto bool_ty = nm->MakeTypeNode(node::kBool);
    auto i32_ty = nm->MakeTypeNode(node::kInt32);
    auto i64_ty = nm->MakeTypeNode(node::kInt64);
    auto i16_ty = nm->MakeTypeNode(node::kInt16);
    auto float_ty = nm->MakeTypeNode(node::kFloat);
    auto double_ty = nm->MakeTypeNode(node::kDouble);
    auto time_ty = nm->MakeTypeNode(node::kTimestamp);
    auto date_ty = nm->MakeTypeNode(node::kDate);
    auto string_ty = nm->MakeTypeNode(node::kVarchar);
    auto row_ty = nm->MakeTypeNode(node::kRow);

    auto list_i32_ty = nm->MakeTypeNode(node::kList, i32_ty);
    auto list_i64_ty = nm->MakeTypeNode(node::kList, i64_ty);
    auto list_i16_ty = nm->MakeTypeNode(node::kList, i16_ty);
    auto list_bool_ty = nm->MakeTypeNode(node::kList, bool_ty);
    auto list_float_ty = nm->MakeTypeNode(node::kList, float_ty);
    auto list_double_ty = nm->MakeTypeNode(node::kList, double_ty);
    auto list_time_ty = nm->MakeTypeNode(node::kList, time_ty);
    auto list_date_ty = nm->MakeTypeNode(node::kList, date_ty);
    auto list_string_ty = nm->MakeTypeNode(node::kList, string_ty);
    auto list_row_ty = nm->MakeTypeNode(node::kList, row_ty);

    auto iter_i32_ty = nm->MakeTypeNode(node::kIterator, i32_ty);
    auto iter_i64_ty = nm->MakeTypeNode(node::kIterator, i64_ty);
    auto iter_i16_ty = nm->MakeTypeNode(node::kIterator, i16_ty);
    auto iter_bool_ty = nm->MakeTypeNode(node::kIterator, bool_ty);
    auto iter_float_ty = nm->MakeTypeNode(node::kIterator, float_ty);
    auto iter_double_ty = nm->MakeTypeNode(node::kIterator, double_ty);
    auto iter_time_ty = nm->MakeTypeNode(node::kIterator, time_ty);
    auto iter_date_ty = nm->MakeTypeNode(node::kIterator, date_ty);
    auto iter_string_ty = nm->MakeTypeNode(node::kIterator, string_ty);
    auto iter_row_ty = nm->MakeTypeNode(node::kIterator, row_ty);

    auto RegisterMethodInternal = [&lib](const std::string &fn_name, hybridse::node::TypeNode *ret,
                                      std::initializer_list<hybridse::node::TypeNode *> args,
                                      void *fn_ptr) { RegisterMethod(lib, fn_name, ret, args, fn_ptr); };

    RegisterMethodInternal("iterator", bool_ty, {list_i16_ty, iter_i16_ty},
                   reinterpret_cast<void *>(v1::iterator_list<int16_t>));
    RegisterMethodInternal("iterator", bool_ty, {list_i32_ty, iter_i32_ty},
                   reinterpret_cast<void *>(v1::iterator_list<int32_t>));
    RegisterMethodInternal("iterator", bool_ty, {list_i64_ty, iter_i64_ty},
                   reinterpret_cast<void *>(v1::iterator_list<int64_t>));
    RegisterMethodInternal("iterator", bool_ty, {list_bool_ty, iter_bool_ty},
                   reinterpret_cast<void *>(v1::iterator_list<bool>));
    RegisterMethodInternal("iterator", bool_ty, {list_float_ty, iter_float_ty},
                   reinterpret_cast<void *>(v1::iterator_list<float>));
    RegisterMethodInternal("iterator", bool_ty, {list_double_ty, iter_double_ty},
                   reinterpret_cast<void *>(v1::iterator_list<double>));
    RegisterMethodInternal(
        "iterator", bool_ty, {list_time_ty, iter_time_ty},
        reinterpret_cast<void *>(v1::iterator_list<Timestamp>));
    RegisterMethodInternal("iterator", bool_ty, {list_date_ty, iter_date_ty},
                   reinterpret_cast<void *>(v1::iterator_list<Date>));
    RegisterMethodInternal(
        "iterator", bool_ty, {list_string_ty, iter_string_ty},
        reinterpret_cast<void *>(v1::iterator_list<StringRef>));
    RegisterMethodInternal("iterator", bool_ty, {list_row_ty, iter_row_ty},
                   reinterpret_cast<void *>(v1::iterator_list<codec::Row>));

    RegisterMethodInternal("next", i16_ty, {iter_i16_ty},
                   reinterpret_cast<void *>(v1::next_iterator<int16_t>));
    RegisterMethodInternal("next", i32_ty, {iter_i32_ty},
                   reinterpret_cast<void *>(v1::next_iterator<int32_t>));
    RegisterMethodInternal("next", i64_ty, {iter_i64_ty},
                   reinterpret_cast<void *>(v1::next_iterator<int64_t>));
    RegisterMethodInternal("next", bool_ty, {iter_bool_ty},
                   reinterpret_cast<void *>(v1::next_iterator<bool>));
    RegisterMethodInternal("next", float_ty, {iter_float_ty},
                   reinterpret_cast<void *>(v1::next_iterator<float>));
    RegisterMethodInternal("next", double_ty, {iter_double_ty},
                   reinterpret_cast<void *>(v1::next_iterator<double>));
    RegisterMethodInternal(
        "next", bool_ty, {iter_time_ty, time_ty},
        reinterpret_cast<void *>(v1::next_struct_iterator<Timestamp>));
    RegisterMethodInternal(
        "next", bool_ty, {iter_date_ty, date_ty},
        reinterpret_cast<void *>(v1::next_struct_iterator<Date>));
    RegisterMethodInternal(
        "next", bool_ty, {iter_string_ty, string_ty},
        reinterpret_cast<void *>(v1::next_struct_iterator<StringRef>));
    RegisterMethodInternal("next", row_ty, {iter_row_ty},
                   reinterpret_cast<void *>(v1::next_row_iterator));

    RegisterMethodInternal(
        "next_nullable", i16_ty, {iter_i16_ty},
        reinterpret_cast<void *>(v1::next_nullable_iterator<int16_t>));
    RegisterMethodInternal(
        "next_nullable", i32_ty, {iter_i32_ty},
        reinterpret_cast<void *>(v1::next_nullable_iterator<int32_t>));
    RegisterMethodInternal(
        "next_nullable", i64_ty, {iter_i64_ty},
        reinterpret_cast<void *>(v1::next_nullable_iterator<int64_t>));
    RegisterMethodInternal("next_nullable", bool_ty, {iter_bool_ty},
                   reinterpret_cast<void *>(v1::next_nullable_iterator<bool>));
    RegisterMethodInternal("next_nullable", float_ty, {iter_float_ty},
                   reinterpret_cast<void *>(v1::next_nullable_iterator<float>));
    RegisterMethodInternal(
        "next_nullable", double_ty, {iter_double_ty},
        reinterpret_cast<void *>(v1::next_nullable_iterator<double>));
    RegisterMethodInternal(
        "next_nullable", bool_ty, {iter_time_ty},
        reinterpret_cast<void *>(v1::next_nullable_iterator<Timestamp>));
    RegisterMethodInternal(
        "next_nullable", bool_ty, {iter_date_ty},
        reinterpret_cast<void *>(v1::next_nullable_iterator<Date>));
    RegisterMethodInternal(
        "next_nullable", bool_ty, {iter_string_ty},
        reinterpret_cast<void *>(v1::next_nullable_iterator<StringRef>));

    RegisterMethodInternal("has_next", bool_ty, {iter_i16_ty},
                   reinterpret_cast<void *>(v1::has_next<int16_t>));
    RegisterMethodInternal("has_next", bool_ty, {iter_i32_ty},
                   reinterpret_cast<void *>(v1::has_next<int32_t>));
    RegisterMethodInternal("has_next", bool_ty, {iter_i64_ty},
                   reinterpret_cast<void *>(v1::has_next<int64_t>));
    RegisterMethodInternal("has_next", bool_ty, {iter_bool_ty},
                   reinterpret_cast<void *>(v1::has_next<bool>));
    RegisterMethodInternal("has_next", bool_ty, {iter_float_ty},
                   reinterpret_cast<void *>(v1::has_next<float>));
    RegisterMethodInternal("has_next", bool_ty, {iter_double_ty},
                   reinterpret_cast<void *>(v1::has_next<double>));
    RegisterMethodInternal("has_next", bool_ty, {iter_time_ty},
                   reinterpret_cast<void *>(v1::has_next<Timestamp>));
    RegisterMethodInternal("has_next", bool_ty, {iter_date_ty},
                   reinterpret_cast<void *>(v1::has_next<Date>));
    RegisterMethodInternal("has_next", bool_ty, {iter_string_ty},
                   reinterpret_cast<void *>(v1::has_next<StringRef>));
    RegisterMethodInternal("has_next", bool_ty, {iter_row_ty},
                   reinterpret_cast<void *>(v1::has_next<codec::Row>));

    RegisterMethodInternal("delete_iterator", bool_ty, {iter_i16_ty},
                   reinterpret_cast<void *>(v1::delete_iterator<int16_t>));
    RegisterMethodInternal("delete_iterator", bool_ty, {iter_i32_ty},
                   reinterpret_cast<void *>(v1::delete_iterator<int32_t>));
    RegisterMethodInternal("delete_iterator", bool_ty, {iter_i64_ty},
                   reinterpret_cast<void *>(v1::delete_iterator<int64_t>));
    RegisterMethodInternal("delete_iterator", bool_ty, {iter_bool_ty},
                   reinterpret_cast<void *>(v1::delete_iterator<bool>));
    RegisterMethodInternal("delete_iterator", bool_ty, {iter_float_ty},
                   reinterpret_cast<void *>(v1::delete_iterator<float>));
    RegisterMethodInternal("delete_iterator", bool_ty, {iter_double_ty},
                   reinterpret_cast<void *>(v1::delete_iterator<double>));
    RegisterMethodInternal(
        "delete_iterator", bool_ty, {iter_time_ty},
        reinterpret_cast<void *>(v1::delete_iterator<Timestamp>));
    RegisterMethodInternal("delete_iterator", bool_ty, {iter_date_ty},
                   reinterpret_cast<void *>(v1::delete_iterator<Date>));
    RegisterMethodInternal(
        "delete_iterator", bool_ty, {iter_string_ty},
        reinterpret_cast<void *>(v1::delete_iterator<StringRef>));
    RegisterMethodInternal("delete_iterator", bool_ty, {iter_row_ty},
                   reinterpret_cast<void *>(v1::delete_iterator<codec::Row>));
}

}  // namespace udf
}  // namespace hybridse
