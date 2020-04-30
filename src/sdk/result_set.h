/*
 * result_set.h
 * Copyright (C) 4paradigm.com 2020 wangtaize <wangtaize@4paradigm.com>
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

#ifndef SRC_SDK_RESULT_SET_H_
#define SRC_SDK_RESULT_SET_H_

#include <stdint.h>
#include "sdk/base.h"

namespace fesql {
namespace sdk {

const static std::string result_set_str_empty;

class ResultSet {
 public:
    ResultSet() {}

    virtual ~ResultSet() {}

    virtual bool Next() = 0;

    virtual bool GetString(uint32_t index, char** result, uint32_t* size) = 0;

    inline std::string GetStringUnsafe(uint32_t index) {
        if (IsNULL(index)) return result_set_str_empty;
        char* ptr = NULL;
        uint32_t size = 0;
        GetString(index, &ptr, &size);
        return std::string(ptr, size);
    }

    virtual bool GetBool(uint32_t index, bool* result) = 0;

    inline bool GetBoolUnsafe(uint32_t index) {
        if (IsNULL(index)) return false;
        bool ok = false;
        GetBool(index, &ok);
        return ok;
    }

    virtual bool GetChar(uint32_t index, char* result) = 0;

    char GetCharUnsafe(uint32_t index)  {
        if (IsNULL(index)) return 0;
        char data = 0;
        GetChar(index, &data);
        return data;
    }

    virtual bool GetInt16(uint32_t index, int16_t* result) = 0;

    virtual short GetInt16Unsafe(uint32_t index) {
        if (IsNULL(index)) return 0;
        short val = 0;
        GetInt16(index, &val);
        return val;
    }

    virtual bool GetInt32(uint32_t index, int32_t* result) = 0;

    int GetInt32Unsafe(uint32_t index) {
        if (IsNULL(index)) return 0;
        int val = 0;
        GetInt32(index, &val);
        return val;
    }

    virtual bool GetInt64(uint32_t index, int64_t* result) = 0;

    long GetInt64Unsafe(uint32_t index) {
        if (IsNULL(index)) return 0;
        long val = 0;
        GetInt64(index, &val);
        return val;
    }

    virtual bool GetFloat(uint32_t index, float* result) = 0;

    virtual float GetFloatUnsafe(uint32_t index)  {
        if (IsNULL(index)) return 0.0f;
        float val = 0.0f;
        GetFloat(index, &val);
        return val;
    }

    virtual bool GetDouble(uint32_t index, double* result) = 0;

    virtual double GetDoubleUnsafe(uint32_t index)  {
        if (IsNULL(index)) return 0;
        double val = 0;
        GetDouble(index, &val);
        return val;
    }

    virtual bool GetDate(uint32_t index, uint32_t* days) = 0;
    
    virtual int32_t GetDateUnsafe(uint32_t index) = 0;

    virtual bool GetTime(uint32_t index, int64_t* mills) = 0;
    int64_t GetTimeUnsafe(uint32_t index) {
        if (IsNULL(index)) return 0;
        int64_t mills = 0;
        GetTime(index, &mills);
        return mills;
    }

    virtual const Schema& GetSchema() = 0;

    virtual bool IsNULL(uint32_t index) = 0;

    virtual int32_t Size() = 0;
};

}  // namespace sdk
}  // namespace fesql
#endif  // SRC_SDK_RESULT_SET_H_
