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

#ifndef HYBRIDSE_INCLUDE_SDK_BASE_SCHEMA_H_
#define HYBRIDSE_INCLUDE_SDK_BASE_SCHEMA_H_

#include <stdint.h>

#include <string>

namespace hybridse {
namespace sdk {

enum DataType {
    kTypeBool = 0,
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
        case kTypeDate:
            return "date";
        default:
            return "unknownType";
    }
}

class Schema {
 public:
    Schema() : empty() {}
    virtual ~Schema() {}
    virtual int32_t GetColumnCnt() const { return 0; }
    virtual const std::string& GetColumnName(uint32_t index) const { return empty; }
    virtual const DataType GetColumnType(uint32_t index) const { return kTypeUnknow; }
    virtual const bool IsColumnNotNull(uint32_t index) const { return false; }
    virtual const bool IsConstant(uint32_t index) const { return false; }

 private:
    std::string empty;
};
}  // namespace sdk
}  // namespace hybridse
#endif  // HYBRIDSE_INCLUDE_SDK_BASE_SCHEMA_H_
