/*
 * dbms_server_impl.cc
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

#include "dbms/dbms_server_impl.h"
#include "brpc/server.h"
#include "absl/time.h"

namespace fesql {
namespace dbms {

DBMSServerImpl::DBMSServerImpl() {}
DBMSServerImpl::~DBMSServerImpl () {}

void DBMSServerImpl::AddGroup(RpcController* ctr,
            const AddGroupRequest* request,
            AddGroupResponse* response,
            Closure* done) {

    brpc::ClosureGuard done_guard(done);
    if (request->name().isEmpty()) {
        ::fesql::common::Status* status = response->mutable_status();
        status->set_code(::fesql::common::kOK);
        status->set_msg("group name is empty");
        LOG(WARNING) << "create group failed for name is empty";
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    Groups::iterator it = groups_.find(request->name());
    if (it != groups_.end()) {
        ::fesql::common::Status* status = response->mutable_status();
        status->set_code(::fesql::common::kNameExists);
        status->set_msg("group name exists ");
        LOG(WARNING) << "create group failed for name existing";
        return;
    }
    ::fesql::type::Groups& group = groups_[request->name()];
    group.set_name(request->name());

}



} // namespace of dbms
} // namespace of fesql


