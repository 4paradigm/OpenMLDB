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

#ifndef EXAMPLES_TOYDB_SRC_TABLET_TABLET_INTERNAL_SDK_H_
#define EXAMPLES_TOYDB_SRC_TABLET_TABLET_INTERNAL_SDK_H_

#include <string>
#include "brpc/channel.h"
#include "proto/fe_tablet.pb.h"

namespace hybridse {
namespace tablet {

class TabletInternalSDK {
 public:
    explicit TabletInternalSDK(const std::string& endpoint);
    ~TabletInternalSDK();
    bool Init();
    void CreateTable(CreateTableRequest* request,
                     common::Status& status);  // NOLINT

 private:
    std::string endpoint_;
    ::brpc::Channel* channel_;
};

}  // namespace tablet
}  // namespace hybridse
#endif  // EXAMPLES_TOYDB_SRC_TABLET_TABLET_INTERNAL_SDK_H_
