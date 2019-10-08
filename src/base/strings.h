//
// strings.h
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-04-02 
// 


#ifndef RTIDB_BASE_STRINGS_H
#define RTIDB_BASE_STRINGS_H

#include <string>
#include <vector>
#include <iostream>
#include <sys/time.h>
#include <time.h>

namespace rtidb {
namespace base {

const static char LABELS[10] = {'0','1','2','3','4','5','6','7','8','9'};
const static uint32_t TIME_OFFSET[] = {1000, 60, 60, 24};
const static char* TIME_LABEL[] = { "ms", "s", "m", "h", "d"};

static inline void SplitString(const std::string& full,
                               const std::string& delim,
                               std::vector<std::string>& result) {
    result.clear();
    if (full.empty()) {
        return;
    }
    std::string tmp;
    std::string::size_type pos_begin = full.find_first_not_of(delim);
    std::string::size_type comma_pos = 0;
    while (pos_begin != std::string::npos) {
        comma_pos = full.find(delim, pos_begin);
        if (comma_pos != std::string::npos) {
            tmp = full.substr(pos_begin, comma_pos - pos_begin);
            pos_begin = comma_pos + delim.length();
        } else {
            tmp = full.substr(pos_begin);
            pos_begin = comma_pos;
        }

        if (!tmp.empty()) {
            result.push_back(tmp);
            tmp.clear();
        }
    }
}

static inline bool IsVisible(char c) {
    return (c >= 0x20 && c <= 0x7E);
}



static inline std::string FormatToString(uint32_t name, uint32_t max_shift) {
    uint32_t shift = 0;
    std::string result;
    result.resize(max_shift);
    char* rbuffer = reinterpret_cast<char*>(& (result[0]));
    for (uint32_t i = 0; i < max_shift; i++) {
        rbuffer[i] = '0';
    }
    while (shift < max_shift) {
        rbuffer[max_shift - shift - 1] = LABELS[name % 10];
        shift++;
        name /= 10;
    }
    return result;
}

static inline char ToHex(uint8_t i) {
    char j = 0;
    if (i < 10) {
        j = i + '0';
    } else {
        j = i - 10 + 'a';
    }
    return j;
}

static inline std::string DebugCharArray(const char* data, uint32_t size) {
    std::string dst;
    dst.resize(size << 2);
    uint32_t j = 0;
    for (uint32_t i = 0; i < size; i++) {
        uint8_t c = data[i];
        if (IsVisible(c)) {
            dst[j++] = c;
        } else {
            dst[j++] = '\\';
            dst[j++] = 'x';
            dst[j++] = ToHex(c >> 4);
            dst[j++] = ToHex(c & 0xF);
        }
    }
    return dst.substr(0, j);
}

static inline std::string DebugString(const std::string& src) {
    size_t src_len = src.size();
    std::string dst;
    dst.resize(src_len << 2);

    size_t j = 0;
    for (size_t i = 0; i < src_len; i++) {
        uint8_t c = src[i];
        if (IsVisible(c)) {
            dst[j++] = c;
        } else {
            dst[j++] = '\\';
            dst[j++] = 'x';
            dst[j++] = ToHex(c >> 4);
            dst[j++] = ToHex(c & 0xF);
        }
    }
    return dst.substr(0, j);
}

static inline std::string NumToString(double num) {
    char buf[32];
    snprintf(buf, sizeof(buf), "%.3f", num);
    return std::string(buf);
}

static inline std::string HumanReadableString(int64_t num) {
    static const int max_shift = 6;
    static const char* const prefix[max_shift + 1] = {"", " K", " M", " G", " T", " P", " E"};
    int shift = 0;
    double v = num;
    while ((num>>=10) > 0 && shift < max_shift) {
        v /= 1024;
        shift++;
    }
    return NumToString(v) + prefix[shift];
}

static inline std::string HumanReadableTime(uint64_t age) {
    uint64_t value = age;
    for (uint32_t i = 0; i < 4; i++) {
        uint64_t v = value / TIME_OFFSET[i];
        if (v == 0) {
            return std::to_string(value) + TIME_LABEL[i];
        }
        value = v;
    }
    return std::to_string(value) + TIME_LABEL[4];
}

static inline bool IsNumber(const std::string& str) {
    std::string::const_iterator it = str.begin();
    while (it != str.end() && std::isdigit(*it)) ++it;
    return !str.empty() && it == str.end();
}

static inline std::string GetNowTime() {
    time_t rawtime = time(0);
    tm* timeinfo = localtime(&rawtime);
    char buf[20];
    strftime(buf, 20, "%Y%m%d%H%M%S", timeinfo);
    return std::string(buf);
}

static inline int GetNowHour() {
	struct timeval tv;
    gettimeofday(&tv, NULL);
    const time_t seconds = tv.tv_sec;
    struct tm t;
    localtime_r(&seconds, &t);
 	return t.tm_hour;
}


}
}
#endif /* !STRINGS_H */
