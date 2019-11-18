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
#include <memory>

namespace fesql {
namespace sdk {

struct Query {
    std::string sql;
    std::string db;
};

class ResultSet {

 public:
    ResultSet() {}
    virtual ~ResultSet() {}
    virtual const uint32_t GetColumnCnt() const = 0;
    virtual const std::string& GetColumnName(uint32_t i) const = 0;
    virtual const uint32_t GetRowCnt() const = 0;
};

class TabletSdk {

 public:
    TabletSdk() = default;
    virtual ~TabletSdk() {}
   // virtual void SyncPut() = 0;
    virtual std::unique_ptr<ResultSet> SyncQuery(const Query& query) = 0;
};

// create a new dbms sdk with a endpoint
// failed return NULL
std::unique_ptr<TabletSdk> CreateTabletSdk(const std::string &endpoint);

}  // namespace sdk
}  // namespace fesql
#endif  // SRC_SDK_TABLET_SDK_H 
