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

#ifndef INCLUDE_SDK_BASE_STRUCT_H_
#define INCLUDE_SDK_BASE_STRUCT_H_

#include <stdint.h>
#include <string.h>
#include <memory>
#include <string>
#include <vector>
#include "proto/fe_type.pb.h"
namespace hybridse {
namespace sdk {

struct Status {
    Status() : code(0), msg("ok") {}
    Status(int status_code, const std::string& msg_str)
        : code(status_code), msg(msg_str) {}
    int code;
    std::string msg;
};

struct GroupDef {
    std::string name;
};

struct DatabaseDef {
    std::string name;
};

struct ExecuteRequst {
    DatabaseDef database;
    std::string sql;
};
struct ExecuteResult {
    DatabaseDef database;
    std::string result;
};

enum DataType {
    kTypeBool,
    kTypeInt16,
    kTypeInt32,
    kTypeInt64,
    kTypeFloat,
    kTypeDouble,
    kTypeString,
    kTypeDate,
    kTypeTimestamp,
    kTypeUnknow
};

struct Query {
    std::string sql;
    std::string db;
    bool is_batch_mode = false;
};

class Value {
 public:
    Value() {
        type = kTypeString;
        size = 4;
        val_.vstr = nullptr;
    }
    explicit Value(int32_t v) : size(4) {
        type = kTypeInt32;
        val_.vint = v;
    }
    explicit Value(int16_t v) : size(2) {
        type = kTypeInt16;
        val_.vsmallint = v;
    }
    explicit Value(int64_t v) : size(8) {
        type = kTypeInt64;
        val_.vlong = v;
    }
    explicit Value(float v) : size(4) {
        type = kTypeFloat;
        val_.vfloat = v;
    }
    explicit Value(double v) : size(8) {
        type = kTypeDouble;
        val_.vdouble = v;
    }
    explicit Value(const char* v) {
        type = kTypeString;
        if (nullptr == v) {
            val_.vstr = 0;
            size = 4;
        } else {
            val_.vstr = strdup(v);
            size = strlen(v);
        }
    }

    ~Value() {}

    const size_t GetSize() const { return size; }

    const DataType GetDataType() const { return type; }

    const int32_t GetInt32() const {
        switch (type) {
            case kTypeInt32:
                return static_cast<int32_t>(val_.vint);
            case kTypeInt16:
                return static_cast<int32_t>(val_.vsmallint);
            case kTypeInt64:
                return static_cast<int64_t>(val_.vlong);
            case kTypeFloat:
                return static_cast<int64_t>(val_.vfloat);
            case kTypeDouble:
                return static_cast<int64_t>(val_.vdouble);
            default: {
                return 0;
            }
        }
    }

    const int16_t GetInt16() const {
        switch (type) {
            case kTypeInt32:
                return static_cast<int16_t>(val_.vint);
            case kTypeInt16:
                return static_cast<int16_t>(val_.vsmallint);
            case kTypeInt64:
                return static_cast<int16_t>(val_.vlong);
            case kTypeFloat:
                return static_cast<int16_t>(val_.vfloat);
            case kTypeDouble:
                return static_cast<int16_t>(val_.vdouble);
            default: {
                return 0;
            }
        }
    }

    const int64_t GetInt64() const {
        switch (type) {
            case kTypeInt32:
                return static_cast<int64_t>(val_.vint);
            case kTypeInt16:
                return static_cast<int64_t>(val_.vsmallint);
            case kTypeInt64:
                return static_cast<int64_t>(val_.vlong);
            case kTypeFloat:
                return static_cast<int64_t>(val_.vfloat);
            case kTypeDouble:
                return static_cast<int64_t>(val_.vdouble);
            default: {
                return 0;
            }
        }
    }

    const float GetFloat() const {
        switch (type) {
            case kTypeInt32:
                return static_cast<float>(val_.vint);
            case kTypeInt16:
                return static_cast<float>(val_.vsmallint);
            case kTypeInt64:
                return static_cast<float>(val_.vlong);
            case kTypeFloat:
                return static_cast<float>(val_.vfloat);
            case kTypeDouble:
                return static_cast<float>(val_.vdouble);
            default: {
                return 0.0;
            }
        }
    }

    const double GetDouble() const {
        switch (type) {
            case kTypeInt32:
                return static_cast<double>(val_.vint);
            case kTypeInt16:
                return static_cast<double>(val_.vsmallint);
            case kTypeInt64:
                return static_cast<double>(val_.vlong);
            case kTypeFloat:
                return static_cast<double>(val_.vfloat);
            case kTypeDouble:
                return static_cast<double>(val_.vdouble);
            default: {
                return 0.0;
            }
        }
    }

    const char* GetStr() const { return val_.vstr; }

 private:
    DataType type;
    size_t size;
    union {
        int16_t vsmallint;
        int32_t vint;
        int64_t vlong;
        const char* vstr;
        float vfloat;
        double vdouble;
    } val_;
};

struct Insert {
    std::string db;
    std::string table;
    std::string key;
    uint64_t ts;
    std::vector<std::string> columns;
    std::vector<Value> values;
};

inline DataType DataTypeFromProtoType(const type::Type& type) {
    switch (type) {
        case hybridse::type::kBool:
            return kTypeBool;
        case hybridse::type::kInt16:
            return kTypeInt16;
        case hybridse::type::kInt32:
            return kTypeInt32;
        case hybridse::type::kInt64:
            return kTypeInt64;
        case hybridse::type::kFloat:
            return kTypeFloat;
        case hybridse::type::kDouble:
            return kTypeDouble;
        case hybridse::type::kVarchar:
            return kTypeString;
        case hybridse::type::kDate:
            return kTypeDate;
        case hybridse::type::kTimestamp:
            return kTypeTimestamp;
        default:
            return kTypeUnknow;
    }
}
inline const std::string DataTypeName(const DataType& type) {
    switch (type) {
        case kTypeBool:
            return "bool";
        case kTypeInt16:
            return "int16";
        case kTypeInt32:
            return "int32";
        case kTypeInt64:
            return "int64";
        case kTypeFloat:
            return "float";
        case kTypeDouble:
            return "double";
        case kTypeString:
            return "string";
        case kTypeTimestamp:
            return "timestamp";
        default:
            return "unknownType";
    }
}

}  // namespace sdk
}  // namespace hybridse
#endif  // INCLUDE_SDK_BASE_STRUCT_H_
