//
// field_codec.h
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
static inline void Convert(bool data, char* buffer) {
    uint8_t bool_true = 1;
    uint8_t bool_false = 0;
    if (data) {
        memcpy(buffer, static_cast<const void*>(&bool_true), 1);
    } else {
        memcpy(buffer, static_cast<const void*>(&bool_false), 1);
    }
}

static inline void Convert(int16_t data, char* buffer) {
    memrev16ifbe(static_cast<void*>(&data));
    memcpy(buffer, static_cast<const void*>(&data), 2);
}

static inline void Convert(int32_t data, char* buffer) {
    memrev32ifbe(static_cast<void*>(&data));
    memcpy(buffer, static_cast<const void*>(&data), 4);
}

static inline void Convert(int64_t data, char* buffer) {
    memrev64ifbe(static_cast<void*>(&data));
    memcpy(buffer, static_cast<const void*>(&data), 8);
}

static inline void Convert(float data, char* buffer) {
    memrev32ifbe(static_cast<void*>(&data));
    memcpy(buffer, static_cast<const void*>(&data), 4);
}

static inline void Convert(double data, char* buffer) {
    memrev64ifbe(static_cast<void*>(&data));
    memcpy(buffer, static_cast<const void*>(&data), 8);
}


/**
 *  decode part
 */
static inline void GetBool(const char* ch, void* res) {
    memcpy(res, ch, 1);
}

static inline void GetInt16(const char* ch, void* res) {
    memcpy(res, ch, 2);
    memrev32ifbe(static_cast<void*>(res));
}

static inline void GetInt32(const char* ch, void* res) {
    memcpy(res, ch, 4);
    memrev32ifbe(static_cast<void*>(res));
}

static inline void GetInt64(const char* ch, void* res) {
    memcpy(res, ch, 8);
    memrev64ifbe(static_cast<void*>(res));
}

static inline void GetFloat(const char* ch, void* res) {
    memcpy(res, ch, 4);
    memrev32ifbe(static_cast<void*>(res));
}

static inline void GetDouble(const char* ch, void* res) {
    memcpy(res, ch, 8);
    memrev64ifbe(static_cast<void*>(res));
}

}//namespace base
}//namespace rtidb
