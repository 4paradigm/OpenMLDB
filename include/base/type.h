/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef INCLUDE_BASE_TYPE_H_
#define INCLUDE_BASE_TYPE_H_

#include <stdint.h>

#include <cstddef>
#include <ostream>
#include <string>
#include <vector>

namespace openmldb {
namespace base {

struct Timestamp {
    Timestamp() : ts_(0) {}
    explicit Timestamp(int64_t ts) : ts_(ts < 0 ? 0 : ts) {}
    Timestamp& operator+=(const Timestamp& t1) {
        ts_ += t1.ts_;
        return *this;
    }
    Timestamp& operator-=(const Timestamp& t1) {
        ts_ -= t1.ts_;
        return *this;
    }
    int64_t ts_;

    friend std::ostream& operator<<(std::ostream& os, const Timestamp& ts) { return os << ts.ts_; }

    friend bool operator==(const Timestamp& a, const Timestamp& b) { return a.ts_ == b.ts_; }
};

__attribute__((unused)) static const Timestamp operator+(const Timestamp& a,
                                                         const Timestamp& b) {
    return Timestamp(a.ts_ + b.ts_);
}
__attribute__((unused)) static const Timestamp operator-(const Timestamp& a,
                                                         const Timestamp& b) {
    return Timestamp(a.ts_ - b.ts_);
}
__attribute__((unused)) static const Timestamp operator/(const Timestamp& a,
                                                         const int64_t b) {
    return Timestamp(static_cast<int64_t>(a.ts_ / b));
}
__attribute__((unused)) static bool operator>(const Timestamp& a,
                                              const Timestamp& b) {
    return a.ts_ > b.ts_;
}
__attribute__((unused)) static bool operator<(const Timestamp& a,
                                              const Timestamp& b) {
    return a.ts_ < b.ts_;
}
__attribute__((unused)) static bool operator>=(const Timestamp& a,
                                               const Timestamp& b) {
    return a.ts_ >= b.ts_;
}
__attribute__((unused)) static bool operator<=(const Timestamp& a,
                                               const Timestamp& b) {
    return a.ts_ <= b.ts_;
}
__attribute__((unused)) static bool operator!=(const Timestamp& a,
                                               const Timestamp& b) {
    return a.ts_ != b.ts_;
}

struct Date {
    Date() : date_(0) {}
    explicit Date(int32_t date) : date_(date < 0 ? 0 : date) {}
    Date(int32_t year, int32_t month, int32_t day) : date_(0) {
        if (year < 1900 || year > 9999) {
            return;
        }
        if (month < 1 || month > 12) {
            return;
        }
        if (day < 1 || day > 31) {
            return;
        }
        int32_t data = (year - 1900) << 16;
        data = data | ((month - 1) << 8);
        data = data | day;
        date_ = data;
    }
    static bool Decode(int32_t date, int32_t* year, int32_t* month,
                       int32_t* day) {
        if (date < 0) {
            return false;
        }
        *day = date & 0x0000000FF;
        date = date >> 8;
        *month = 1 + (date & 0x0000FF);
        *year = 1900 + (date >> 8);
        return true;
    }

    // | --- 16 bit -------------------------- | -- 8 bit ------------| --- 8 bit --------- |
    // | year count since 1990 (starts from 0) | month(starts from 0) | day (starts from 1) |
    int32_t date_;

    friend std::ostream& operator<<(std::ostream& os, const Date& date) {
        return os << date.date_;
    }
};

__attribute__((unused)) static bool operator>(const Date& a, const Date& b) {
    return a.date_ > b.date_;
}
__attribute__((unused)) static bool operator<(const Date& a, const Date& b) {
    return a.date_ < b.date_;
}
__attribute__((unused)) static bool operator>=(const Date& a, const Date& b) {
    return a.date_ >= b.date_;
}
__attribute__((unused)) static bool operator<=(const Date& a, const Date& b) {
    return a.date_ <= b.date_;
}
__attribute__((unused)) static bool operator==(const Date& a, const Date& b) {
    return a.date_ == b.date_;
}
__attribute__((unused)) static bool operator!=(const Date& a, const Date& b) {
    return a.date_ != b.date_;
}

}  // namespace base
}  // namespace openmldb

#endif  // INCLUDE_BASE_TYPE_H_
