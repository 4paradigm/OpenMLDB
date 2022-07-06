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

#ifndef HYBRIDSE_INCLUDE_SDK_TABLET_SDK_H_
#define HYBRIDSE_INCLUDE_SDK_TABLET_SDK_H_

#include <memory>
#include <string>
#include <vector>
#include "sdk/base.h"
#include "sdk/result_set.h"

namespace hybridse {
namespace sdk {

class ExplainInfo {
 public:
    ExplainInfo() {}
    virtual ~ExplainInfo() {}
    virtual const Schema& GetInputSchema() = 0;
    virtual const Schema& GetOutputSchema() = 0;
    virtual const std::string& GetLogicalPlan() = 0;
    virtual const std::string& GetPhysicalPlan() = 0;
    virtual const std::string& GetIR() = 0;
};

class TabletSdk {
 public:
    TabletSdk() = default;
    virtual ~TabletSdk() {}

    virtual void Insert(const std::string& db, const std::string& sql,
                        sdk::Status* status) = 0;

    virtual std::shared_ptr<ResultSet> Query(const std::string& db,
                                             const std::string& sql,
                                             sdk::Status* status) = 0;
    virtual std::shared_ptr<ResultSet> Query(const std::string& db,
                                             const std::string& sql,
                                             const std::string& row,
                                             sdk::Status* status) = 0;
    virtual std::shared_ptr<ExplainInfo> Explain(const std::string& db,
                                                 const std::string& sql,
                                                 sdk::Status* status) = 0;
};

// create a new tablet sdk with a endpoint
// failed return NULL
std::shared_ptr<TabletSdk> CreateTabletSdk(const std::string& endpoint);

}  // namespace sdk
}  // namespace hybridse
#endif  // HYBRIDSE_INCLUDE_SDK_TABLET_SDK_H_
