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

#include <string>
#include <vector>
#include <memory>
#include "sdk/base.h"
#include "sdk/result_set.h"
#include "sdk/base_impl.h"

namespace fesql {
namespace sdk {

class DBMSSdk {
 public:
    virtual ~DBMSSdk() {}

    virtual void CreateDatabase(
        const std::string& catalog,
        sdk::Status *status) = 0;

    virtual std::unique_ptr<TableSet> GetTables(
        const std::string& catalog,
        sdk::Status *status) = 0;

    virtual std::vector<std::string> GetDatabases(
        sdk::Status *status) = 0;

    virtual std::unique_ptr<ResultSet>  ExecuteQuery(const std::string& catalog,
            const std::string& sql,
            sdk::Status *status) = 0; 
};

// create a new dbms sdk with a endpoint
// failed return NULL
DBMSSdk *CreateDBMSSdk(const std::string &endpoint);

}  // namespace sdk
}  // namespace fesql
#endif  // SRC_SDK_DBMS_SDK_H_
