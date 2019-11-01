/*
 * dbms_sdk.cc
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

#include "sdk/dbms_sdk.h"

#include "proto/dbms.pb.h"
#include "brpc/channel.h"

namespace fesql {
namespace sdk {

class DBMSSdkImpl : public DBMSSdk {

public:
    DBMSSdkImpl(const std::string& endpoint);
    ~DBMSSdkImpl();
    bool Init();
    void CreateGroup(const GroupDef& group, Status& status);
private:
    ::brpc::Channel* channel_;
    std::string endpoint_;
};

DBMSSdkImpl::DBMSSdkImpl(const std::string& endpoint):channel_(NULL),
endpoint_(endpoint){}
DBMSSdkImpl::~DBMSSdkImpl() {
    delete channel_;
}

bool DBMSSdkImpl::Init() {
    channel_ = new ::brpc::Channel();
    brpc::ChannelOptions options;
    int ret = channel_->Init(endpoint_.c_str(), &options);
    if (ret != 0) {
        return false;
    }
    return true;
}

void DBMSSdkImpl::CreateGroup(const GroupDef& group,
        Status& status) {

    ::fesql::dbms::DBMSServer_Stub stub(channel_);
    ::fesql::dbms::AddGroupRequest request;
    request.set_name(group.name);
    ::fesql::dbms::AddGroupResponse response;
    brpc::Controller cntl;
    stub.AddGroup(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        status.code = -1;
        status.msg = "fail to call remote";
    }else {
        status.code = response.status().code();
        status.msg = response.status().msg();
    }
}

DBMSSdk* CreateDBMSSdk(const std::string& endpoint) {

    DBMSSdkImpl* sdk_impl = new DBMSSdkImpl(endpoint);
    if (sdk_impl->Init()) {
        return sdk_impl;
    }

    return NULL;
}

} // namespace sdk
} // namespace fesql



