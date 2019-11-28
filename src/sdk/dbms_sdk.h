/*
 * dbms_sdk.h
 * Copyright (C) 4paradigm.com 2019 wangtaize <wangtaize@4paradigm.com>
 * Licensed under the Apache License, Version 2.0 (the "License")
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

#ifndef SRC_SDK_DBMS_SDK_H_
#define SRC_SDK_DBMS_SDK_H_
#include <proto/type.pb.h>
#include <string>
#include <vector>
#include "sdk/base_struct.h"

namespace fesql {
namespace sdk {

class Schema {
 public:
    Schema() {}
    virtual ~Schema() {};
    virtual const uint32_t GetColumnCnt() const = 0;
    virtual const std::string& GetColumnName(uint32_t i) const = 0;
    virtual const DataType GetColumnType(uint32_t i) const = 0;
    virtual const bool IsColumnNotNull(uint32_t i) const = 0;
};

class DBMSSdk {
 public:
    virtual ~DBMSSdk() {}
    virtual void CreateGroup(
        const GroupDef &group,
        sdk::Status &status) = 0;  // NOLINT (runtime/references)
    virtual void CreateDatabase(
        const DatabaseDef &database,
        sdk::Status &status) = 0;  // NOLINT (runtime/references)
    virtual bool IsExistDatabase(
        const DatabaseDef &database,
        sdk::Status &status) = 0;  // NOLINT (runtime/references)
    virtual std::unique_ptr<Schema> GetSchema(
        const DatabaseDef &database, const std::string &name,
        sdk::Status &status) = 0;  // NOLINT (runtime/references)
    virtual void GetTables(
        const DatabaseDef &database,
        std::vector<std::string> &names,  // NOLINT (runtime/references)
        sdk::Status &status) = 0;         // NOLINT (runtime/references)
    virtual void GetDatabases(
        std::vector<std::string> &names,  // NOLINT (runtime/references)
        sdk::Status &status) = 0;         // NOLINT (runtime/references)
    virtual void ExecuteScript(
        const ExecuteRequst &request,
        ExecuteResult &result,     // NOLINT (runtime/references)
        sdk::Status &status) = 0;  // NOLINT (runtime/references)
};

// create a new dbms sdk with a endpoint
// failed return NULL
DBMSSdk *CreateDBMSSdk(const std::string &endpoint);

}  // namespace sdk
}  // namespace fesql
#endif  // SRC_SDK_DBMS_SDK_H_
