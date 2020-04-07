//
// single_column_codec..h
// Copyright (C) 2020 4paradigm.com
// Author wangbao 
// Date 2020-04-07
//

#pragma once

#include <stdint.h>
#include <string.h>
#include <string>
#include "base/endianconv.h"

namespace rtidb {
namespace base {

/**
 *  encode part
 */
static inline std::string Append(bool data) {
    std::string result;
    result.resize(1);
    char* buffer = reinterpret_cast<char*>(&(result[0]));
    uint8_t bool_true = 1;
    uint8_t bool_false = 0;
    if (data) {
        memcpy(buffer, static_cast<const void*>(&bool_true), 1);
    } else {
        memcpy(buffer, static_cast<const void*>(&bool_false), 1);
    }
    return result;
}

static inline std::string Append(int16_t data) {
    std::string result;
    result.resize(2);
    memrev16ifbe(static_cast<void*>(&data));
    char* buffer = reinterpret_cast<char*>(&(result[0]));
    memcpy(buffer, static_cast<const void*>(&data), 2);
    return result;
}

static inline std::string Append(int32_t data) {
    std::string result;
    result.resize(4);
    memrev32ifbe(static_cast<void*>(&data));
    char* buffer = reinterpret_cast<char*>(&(result[0]));
    memcpy(buffer, static_cast<const void*>(&data), 4);
    return result;
}

static inline std::string Append(int64_t data) {
    std::string result;
    result.resize(8);
    memrev64ifbe(static_cast<void*>(&data));
    char* buffer = reinterpret_cast<char*>(&(result[0]));
    memcpy(buffer, static_cast<const void*>(&data), 8);
    return result;
}

static inline std::string Append(float data) {
    std::string result;
    result.resize(4);
    memrev32ifbe(static_cast<void*>(&data));
    char* buffer = reinterpret_cast<char*>(&(result[0]));
    memcpy(buffer, static_cast<const void*>(&data), 4);
    return result;
}

static inline std::string Append(double data) {
    std::string result;
    result.resize(8);
    memrev64ifbe(static_cast<void*>(&data));
    char* buffer = reinterpret_cast<char*>(&(result[0]));
    memcpy(buffer, static_cast<const void*>(&data), 8);
    return result;
}


/**
 *  decode part
 */
static inline bool GetBool(const std::string& value) {
    uint8_t res = 0;
    memcpy(static_cast<void*>(&res), value.c_str(), 1);
    if (res == 1) return true;
    else return false;
}

static inline int16_t GetInt16(const std::string& value) {
    int16_t res = 0;
    memcpy(static_cast<void*>(&res), value.c_str(), 2);
    memrev32ifbe(static_cast<void*>(&res));
    return res;
}

static inline int32_t GetInt32(const std::string& value) {
    int32_t res = 0;
    memcpy(static_cast<void*>(&res), value.c_str(), 4);
    memrev32ifbe(static_cast<void*>(&res));
    return res;
}

static inline int64_t GetInt64(const std::string& value) {
    int64_t res = 0;
    memcpy(static_cast<void*>(&res), value.c_str(), 8);
    memrev64ifbe(static_cast<void*>(&res));
    return res;
}

static inline float GetFloat(const std::string& value) {
    float res = 0.0;
    memcpy(static_cast<void*>(&res), value.c_str(), 4);
    memrev32ifbe(static_cast<void*>(&res));
    return res;
}

static inline double GetDouble(const std::string& value) {
    double res = 0.0;
    memcpy(static_cast<void*>(&res), value.c_str(), 8);
    memrev64ifbe(static_cast<void*>(&res));
    return res;
}

}//namespace base
}//namespace rtidb
