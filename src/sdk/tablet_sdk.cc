/*
 * tablet_sdk.cc
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

#include "sdk/tablet_sdk.h"

#include "brpc/channel.h"
#include "proto/tablet.pb.h"

namespace fesql {
namespace sdk {

class TabletSdkImpl;
class ResultSetImpl;

class ResultSetImpl : public ResultSet {
 public:
    ResultSetImpl() : response_() {}
    ~ResultSetImpl() {}

    const uint32_t GetColumnCnt() const { return response_.schema_size(); }
    const std::string& GetColumnName(uint32_t i) const {
        // TODO check i out of index
        return response_.schema(i).name();
    }
    const uint32_t GetRowCnt() const { return response_.result_set_size(); }

 private:
    friend TabletSdkImpl;
    tablet::QueryResponse response_;
};

class TabletSdkImpl : public TabletSdk {
 public:
    TabletSdkImpl() {}
    TabletSdkImpl(const std::string& endpoint)
        : endpoint_(endpoint), channel_(NULL) {}
    ~TabletSdkImpl() { delete channel_; }

    bool Init();

    std::unique_ptr<ResultSet> SyncQuery(const Query& query);

 private:
    std::string endpoint_;
    brpc::Channel* channel_;
};

bool TabletSdkImpl::Init() {
    channel_ = new ::brpc::Channel();
    brpc::ChannelOptions options;
    int ret = channel_->Init(endpoint_.c_str(), &options);
    if (ret != 0) {
        return false;
    }
    return true;
}

std::unique_ptr<ResultSet> TabletSdkImpl::SyncQuery(const Query& query) {
    ::fesql::tablet::TabletServer_Stub stub(channel_);
    ::fesql::tablet::QueryRequest request;
    request.set_sql(query.sql);
    request.set_db(query.db);
    brpc::Controller cntl;
    ResultSetImpl* rs = new ResultSetImpl();
    stub.Query(&cntl, &request, &rs->response_, NULL);
    if (cntl.Failed()) {
        delete rs;
        return std::unique_ptr<ResultSet>();
    }
    return std::move(std::unique_ptr<ResultSet>(rs));
}

std::unique_ptr<TabletSdk> CreateTabletSdk(const std::string& endpoint) {
    TabletSdkImpl* sdk = new TabletSdkImpl(endpoint);
    bool ok = sdk->Init();
    if (!ok) {
        delete sdk;
        return std::unique_ptr<TabletSdk>();
    }
    return std::move(std::unique_ptr<TabletSdk>(sdk));
}

}  // namespace sdk
}  // namespace fesql
