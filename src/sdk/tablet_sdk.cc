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
#include "storage/codec.h"
#include "glog/logging.h"

namespace fesql {
namespace sdk {

class TabletSdkImpl;
class ResultSetImpl;
class ResultSetIteratorImpl;

class ResultSetIteratorImpl : public ResultSetIterator {

 public:

    ResultSetIteratorImpl(tablet::QueryResponse* response);

    ~ResultSetIteratorImpl();

    bool HasNext();
    
    void Next();

    bool GetInt16(uint32_t idx, int16_t* val);
    bool GetInt32(uint32_t idx, int32_t* val);
    bool GetInt64(uint32_t idx, int64_t* val);
    bool GetFloat(uint32_t idx, float* val);
    bool GetDouble(uint32_t idx, double* val);

 private:
    std::vector<uint32_t> offsets_;
    uint32_t idx_;
    tablet::QueryResponse* response_;
    std::unique_ptr<storage::RowView> row_view_;
};

ResultSetIteratorImpl::ResultSetIteratorImpl(tablet::QueryResponse* response):
    offsets_(), idx_(0), response_(response), row_view_() {
    uint32_t offset = 2;
    for (int32_t i = 0; i < response_->schema_size(); i++) {
        offsets_.push_back(offset);
        const ::fesql::type::ColumnDef& column = response_->schema(i);
        switch (column.type()) {
            case ::fesql::type::kInt16:
                {
                    offset += 2;
                    break;
                }
            case ::fesql::type::kInt32:
            case ::fesql::type::kFloat:
                {
                    offset += 4;
                    break;
                }
            case ::fesql::type::kInt64:
            case ::fesql::type::kDouble:
                {
                    offset += 8;
                    break;
                }
            default:
                {
                    LOG(WARNING) << ::fesql::type::Type_Name(column.type())  << " is not supported";
                    break;
                }
        }
    }
}


ResultSetIteratorImpl::~ResultSetIteratorImpl() {}

bool ResultSetIteratorImpl::HasNext() {
    if ((int32_t)idx_ < response_->result_set_size()) return true;
    return false;
}

void ResultSetIteratorImpl::Next() {
    const int8_t* row = reinterpret_cast<const int8_t*>(response_->result_set(idx_).c_str());
    uint32_t size = response_->result_set(idx_).size();
    row_view_ = std::move(
            std::unique_ptr<storage::RowView>(
                new storage::RowView(&(response_->schema()),
                    row, &offsets_, size)));
    idx_ += 1;
}

bool ResultSetIteratorImpl::GetInt16(uint32_t idx, int16_t* val) {
    if (!row_view_) {
        return false;
    }
    return row_view_->GetInt16(idx, val);
}

bool ResultSetIteratorImpl::GetInt32(uint32_t idx, int32_t* val) {
    if (!row_view_) {
        return false;
    }
    return row_view_->GetInt32(idx, val);
}

bool ResultSetIteratorImpl::GetInt64(uint32_t idx, int64_t* val) {
    if (!row_view_) {
        return false;
    }
    return row_view_->GetInt64(idx, val);
}

bool ResultSetIteratorImpl::GetFloat(uint32_t idx, float* val) {
    if (!row_view_) {
        return false;
    }
    return row_view_->GetFloat(idx, val);
}

bool ResultSetIteratorImpl::GetDouble(uint32_t idx, double* val) {
    if (!row_view_) {
        return false;
    }
    return row_view_->GetDouble(idx, val);
}

class ResultSetImpl : public ResultSet {
 public:
    ResultSetImpl() : response_() {}
    ~ResultSetImpl() {}

    const uint32_t GetColumnCnt() const { return response_.schema_size(); }
    const std::string& GetColumnName(uint32_t i) const {
        // TODO check i out of index
        return response_.schema(i).name();
    }

    const uint32_t GetRowCnt() const { 
        return response_.result_set_size(); 
    }

    std::unique_ptr<ResultSetIterator> Iterator() {
        return std::move(std::unique_ptr<ResultSetIteratorImpl>(new ResultSetIteratorImpl(&response_)));
    }

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

    bool SyncInsert(const Insert& insert);

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

bool TabletSdkImpl::SyncInsert(const Insert& insert) {
    ::fesql::tablet::TabletServer_Stub stub(channel_);
    ::fesql::tablet::InsertRequest req;
    req.set_db(insert.db);
    req.set_table(insert.table);
    req.set_row(insert.row);
    req.set_ts(insert.ts);
    req.set_key(insert.key);
    ::fesql::tablet::InsertResponse response;
    brpc::Controller cntl;
    stub.Insert(&cntl, &req, &response, NULL);
    if (cntl.Failed() 
            || response.status().code() != ::fesql::common::kOk) {
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
    stub.Query(&cntl, &request, &(rs->response_), NULL);
    if (cntl.Failed() 
            || rs->response_.status().code() != common::kOk) {
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
