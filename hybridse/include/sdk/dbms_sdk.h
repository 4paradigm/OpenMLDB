/*
 * Copyright 2021 4Paradigm
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

#ifndef HYBRIDSE_INCLUDE_SDK_DBMS_SDK_H_
#define HYBRIDSE_INCLUDE_SDK_DBMS_SDK_H_

#include <memory>
#include <string>
#include <vector>
#include "sdk/base.h"
#include "sdk/base_impl.h"
#include "sdk/request_row.h"
#include "sdk/result_set.h"
#include "sdk/tablet_sdk.h"

namespace hybridse {
namespace sdk {

static const Schema EMPTY;

// TODO(wangtaize) replace shared_ptr with unique_ptr

class DBMSSdk {
 public:
    DBMSSdk() {}
    virtual ~DBMSSdk() {}
    virtual void CreateDatabase(const std::string &catalog,
                                sdk::Status *status) {}

    virtual std::shared_ptr<TableSet> GetTables(const std::string &catalog,
                                                sdk::Status *status) {
        return std::shared_ptr<TableSet>();
    }

    virtual std::vector<std::string> GetDatabases(sdk::Status *status) {
        return std::vector<std::string>();
    }

    virtual std::shared_ptr<ResultSet> ExecuteQuery(const std::string &catalog,
                                                    const std::string &sql,
                                                    sdk::Status *status) {
        return std::shared_ptr<ResultSet>();
    }

    virtual std::shared_ptr<ResultSet> ExecuteQuery(
        const std::string &catalog, const std::string &sql,
        const std::shared_ptr<RequestRow> &row, sdk::Status *status) {
        return std::shared_ptr<ResultSet>();
    }

    virtual const Schema &GetInputSchema(const std::string &catalog,
                                         const std::string &sql,
                                         sdk::Status *status) {
        return EMPTY;
    }

    virtual std::shared_ptr<RequestRow> GetRequestRow(
        const std::string &catalog, const std::string &sql,
        sdk::Status *status) {
        return std::shared_ptr<RequestRow>();
    }

    virtual std::shared_ptr<ExplainInfo> Explain(const std::string &catalog,
                                                 const std::string &sql,
                                                 sdk::Status *status) {
        return std::shared_ptr<ExplainInfo>();
    }
};

// create a new dbms sdk with a endpoint
// failed return NULL
std::shared_ptr<DBMSSdk> CreateDBMSSdk(const std::string &endpoint);

}  // namespace sdk
}  // namespace hybridse
#endif  // HYBRIDSE_INCLUDE_SDK_DBMS_SDK_H_
