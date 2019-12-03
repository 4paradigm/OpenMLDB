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

#include <string>
#include <vector>
#include <memory>
#include "sdk/base_struct.h"

namespace fesql {
namespace sdk {

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
    virtual void SyncInsert(const Insert& insert, sdk::Status& status) = 0;  // NOLINT
    virtual void SyncInsert(const std::string& db, const std::string& sql,
                            sdk::Status& status) = 0;  // NOLINT
    virtual std::unique_ptr<ResultSet> SyncQuery(const Query& query,
                                                 sdk::Status& status) = 0;  // NOLINT
};

// create a new dbms sdk with a endpoint
// failed return NULL
std::unique_ptr<TabletSdk> CreateTabletSdk(const std::string& endpoint);

}  // namespace sdk
}  // namespace fesql
#endif  // SRC_SDK_TABLET_SDK_H_
