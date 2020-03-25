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

#include <memory>
#include <string>
#include <vector>
#include "sdk/base.h"
#include "sdk/result_set.h"

namespace fesql {
namespace sdk {

class TabletSdk {
 public:
    TabletSdk() = default;
    virtual ~TabletSdk() {}
    virtual void Insert(const std::string& db, const std::string& sql,
                        sdk::Status* status) = 0;
    virtual std::shared_ptr<ResultSet> Query(
        const std::string& db, 
        const std::string& sql,
        sdk::Status* status) = 0;
};

// create a new tablet sdk with a endpoint
// failed return NULL
std::shared_ptr<TabletSdk> CreateTabletSdk(const std::string& endpoint);

}  // namespace sdk
}  // namespace fesql
#endif  // SRC_SDK_TABLET_SDK_H_

