/*
 * tablet_sdk.h
 * Copyright (C) 4paradigm.com 2019 wangtaize <wangtaize@4paradigm.com>
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

#ifndef SRC_SDK_TABLET_SDK_H_
#define SRC_SDK_TABLET_SDK_H_

#include "sdk/base_struct.h"
#include <stdint-gcc.h>
#include <string.h>
#include <memory>
#include <string>
#include <vector>

namespace fesql {
namespace sdk {
enum DataType {
    kTypeBool,
    kTypeInt16,
    kTypeInt32,
    kTypeInt64,
    kTypeFloat,
    kTypeDouble,
    kTypeString,
    kTypeDate,
    kTypeTimestamp
};

struct Query {
    std::string sql;
    std::string db;
};

class Value {
 public:
    explicit Value() {
        type = kTypeString;
        size = 0;
        val_.vstr = nullptr;
    }
    explicit Value(int32_t v) {
        type = kTypeInt32;
        size = sizeof(int32_t);
        val_.vint = v;
    }
    explicit Value(int16_t v) {
        type = kTypeInt16;
        size = sizeof(int16_t);
        val_.vsmallint = v;
    }
    explicit Value(int64_t v) {
        type = kTypeInt64;
        size = sizeof(int64_t);
        val_.vlong = v;
    }
    explicit Value(float v) {
        type = kTypeFloat;
        size = sizeof(float);
        val_.vfloat = v;
    }
    explicit Value(double v) {
        type = kTypeDouble;
        size = sizeof(double);
        val_.vdouble = v;
    }
    explicit Value(const char* v) {
        type = kTypeString;
        size = strlen(v);
        val_.vstr = strdup(v);
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
            default:{
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
            default:{
                return 0;
            }
        }
    }

    const int64_t GetInt64() const {
        switch (type) {
            case kTypeInt32:
                return static_cast<int64_t >(val_.vint);
            case kTypeInt16:
                return static_cast<int64_t>(val_.vsmallint);
            case kTypeInt64:
                return static_cast<int64_t>(val_.vlong);
            case kTypeFloat:
                return static_cast<int64_t>(val_.vfloat);
            case kTypeDouble:
                return static_cast<int64_t>(val_.vdouble);
            default:{
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
            default:{
                return 0.0;
            }
        }
    }

    const double GetDouble() const {  switch (type) {
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
            default:{
                return 0.0;
            }
        } }

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
};  // namespace sdk
struct Insert {
    std::string db;
    std::string table;
    std::string key;
    uint64_t ts;
    std::vector<std::string> columns;
    std::vector<Value> values;
};

class ResultSetIterator {
 public:
    ResultSetIterator() {}
    virtual ~ResultSetIterator() {}

    virtual bool HasNext() = 0;

    virtual void Next() = 0;
    virtual bool GetInt16(uint32_t idx, int16_t* val) = 0;
    virtual bool GetInt32(uint32_t idx, int32_t* val) = 0;
    virtual bool GetInt64(uint32_t idx, int64_t* val) = 0;
    virtual bool GetFloat(uint32_t idx, float* val) = 0;
    virtual bool GetDouble(uint32_t idx, double* val) = 0;
};

class ResultSet {
 public:
    ResultSet() {}
    virtual ~ResultSet() {}
    virtual const uint32_t GetColumnCnt() const = 0;
    virtual const std::string& GetColumnName(uint32_t i) const = 0;
    virtual const DataType GetColumnType(uint32_t i) const = 0;
    virtual const uint32_t GetRowCnt() const = 0;
    virtual std::unique_ptr<ResultSetIterator> Iterator() = 0;
};

class TabletSdk {
 public:
    TabletSdk() = default;
    virtual ~TabletSdk() {}
    virtual void SyncInsert(const Insert& insert, sdk::Status& status) = 0;
    virtual void SyncInsert(const std::string& db, const std::string& sql,
                            sdk::Status& status) = 0;
    virtual std::unique_ptr<ResultSet> SyncQuery(const Query& query, sdk::Status& status) = 0;
};

// create a new dbms sdk with a endpoint
// failed return NULL
std::unique_ptr<TabletSdk> CreateTabletSdk(const std::string& endpoint);

}  // namespace sdk
}  // namespace fesql
#endif  // SRC_SDK_TABLET_SDK_H
