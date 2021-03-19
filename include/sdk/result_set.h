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

#ifndef SRC_INCLUDE_SDK_RESULT_SET_H_
#define SRC_INCLUDE_SDK_RESULT_SET_H_

#include <stdint.h>
#include <string>
#include "sdk/base.h"

namespace hybridse {
namespace sdk {
struct Date {
    int32_t year;
    int32_t month;
    int32_t day;
};
class ResultSet {
 public:
    ResultSet() {}

    virtual ~ResultSet() {}

    virtual bool Reset() = 0;
    virtual bool Next() = 0;

    virtual bool GetString(uint32_t index, std::string* val) = 0;

    inline std::string GetStringUnsafe(int index) {
        if (IsNULL(index)) return std::string();
        std::string val;
        GetString(index, &val);
        return val;
    }
    const std::string GetAsString(uint32_t idx) {
        if (nullptr == GetSchema()) {
            return "NA";
        }
        int schema_size = GetSchema()->GetColumnCnt();
        if (0 == schema_size) {
            return "NA";
        }

        if ((int32_t)idx >= schema_size) {
            return "NA";
        }

        if (IsNULL(idx)) {
            return "NULL";
        }
        auto type = GetSchema()->GetColumnType(idx);
        switch (type) {
            case kTypeInt32: {
                return std::to_string(GetInt32Unsafe(idx));
            }
            case kTypeInt64: {
                return std::to_string(GetInt64Unsafe(idx));
            }
            case kTypeInt16: {
                return std::to_string(GetInt16Unsafe(idx));
            }
            case kTypeFloat: {
                return std::to_string(GetFloatUnsafe(idx));
            }
            case kTypeDouble: {
                return std::to_string(GetDoubleUnsafe(idx));
            }
            case kTypeBool: {
                return std::to_string(GetBoolUnsafe(idx));
            }
            case kTypeString: {
                return GetStringUnsafe(idx);
                break;
            }
            case kTypeTimestamp: {
                return std::to_string(GetTimeUnsafe(idx));
            }
            case kTypeDate: {
                int32_t year;
                int32_t month;
                int32_t day;
                if (GetDate(idx, &year, &month, &day)) {
                    char date[11];
                    snprintf(date, 11u, "%4d-%.2d-%.2d", year, month, day);
                    return std::string(date);
                } else {
                    return "NA";
                }
            }
            default: {
                break;
            }
        }

        return "NA";
    }

    inline std::string GetRowString() {
        int schema_size = GetSchema()->GetColumnCnt();
        if (schema_size == 0) {
            return "NA";
        }
        std::string row_str = "";

        for (int i = 0; i < schema_size; i++) {
            row_str.append(GetAsString(i));
            if (i != schema_size - 1) {
                row_str.append(", ");
            }
        }
        return row_str;
    }
    virtual bool GetBool(uint32_t index, bool* result) = 0;

    inline bool GetBoolUnsafe(int index) {
        if (IsNULL(index)) return false;
        bool ok = false;
        GetBool(static_cast<uint32_t>(index), &ok);
        return ok;
    }
    virtual bool GetChar(uint32_t index, char* result) = 0;
    char GetCharUnsafe(int index) {
        if (IsNULL(index)) return 0;
        char data = 0;
        GetChar(index, &data);
        return data;
    }

    virtual bool GetInt16(uint32_t index, int16_t* result) = 0;

    virtual short GetInt16Unsafe(int index) {  // NOLINT
        if (IsNULL(index)) return 0;
        short val = 0;  // NOLINT
        GetInt16(index, &val);
        return val;
    }

    virtual bool GetInt32(uint32_t index, int32_t* result) = 0;

    int GetInt32Unsafe(int index) {
        if (IsNULL(index)) return 0;
        int32_t val = 0;
        GetInt32(index, &val);
        return val;
    }

    virtual bool GetInt64(uint32_t index, int64_t* result) = 0;

    int64_t GetInt64Unsafe(int index) {
        if (IsNULL(index)) return 0;
        int64_t val = 0;
        GetInt64(index, &val);
        return val;
    }

    virtual bool GetFloat(uint32_t index, float* result) = 0;

    virtual float GetFloatUnsafe(int index) {
        if (IsNULL(index)) return 0.0f;
        float val = 0.0f;
        GetFloat(index, &val);
        return val;
    }

    virtual bool GetDouble(uint32_t index, double* result) = 0;

    virtual double GetDoubleUnsafe(int index) {
        if (IsNULL(index)) return 0;
        double val = 0;
        GetDouble(index, &val);
        return val;
    }

    virtual bool GetDate(uint32_t index, int32_t* year, int32_t* month,
                         int32_t* day) = 0;

    virtual bool GetDate(uint32_t index, int32_t* days) = 0;

    virtual Date GetStructDateUnsafe(int32_t index) {
        Date date;
        if (!GetDate(index, &date.year, &date.month, &date.day)) {
            return Date();
        }
        return date;
    }
    virtual int32_t GetDateUnsafe(uint32_t index) {
        if (IsNULL(index)) return 0;
        int32_t val = 0;
        GetDate(index, &val);
        return val;
    }

    virtual bool GetTime(uint32_t index, int64_t* mills) = 0;
    int64_t GetTimeUnsafe(int index) {
        if (IsNULL(index)) return 0;
        int64_t mills = 0;
        GetTime(index, &mills);
        return mills;
    }

    virtual const Schema* GetSchema() = 0;

    virtual bool IsNULL(int index) = 0;

    virtual int32_t Size() = 0;
};

}  // namespace sdk
}  // namespace hybridse
#endif  // SRC_INCLUDE_SDK_RESULT_SET_H_
