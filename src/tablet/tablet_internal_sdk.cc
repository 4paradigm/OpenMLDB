/*
 * tablet_internal_sdk.cc
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

#include "tablet/tablet_internal_sdk.h"
#include "glog/logging.h"

namespace fesql {
namespace tablet {

TabletInternalSDK::TabletInternalSDK(const std::string& endpoint)
    : endpoint_(endpoint), channel_(NULL) {}

TabletInternalSDK::~TabletInternalSDK() {
    delete channel_;
    channel_ = NULL;
}

bool TabletInternalSDK::Init() {
    channel_ = new ::brpc::Channel();
    brpc::ChannelOptions options;
    int ret = channel_->Init(endpoint_.c_str(), &options);
    if (ret != 0) {
        LOG(WARNING) << "fail to init tablet sdk with ret " << ret;
        return false;
    }
    LOG(INFO) << "init tablet sdk with endpoint " << endpoint_ << " done";
    return true;
}

void TabletInternalSDK::CreateTable(CreateTableRequest* request,
                                    common::Status& status) {
    ::fesql::tablet::TabletServer_Stub stub(channel_);
    ::fesql::tablet::CreateTableResponse response;
    brpc::Controller cntl;
    stub.CreateTable(&cntl, request, &response, NULL);
    if (cntl.Failed()) {
        status.set_code(common::kConnError);
        status.set_msg("connection error");
    } else {
        status.CopyFrom(response.status());
    }
}

}  // namespace tablet
}  // namespace fesql
