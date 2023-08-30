/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SRC_SDK_RESULT_SET_SQL_H_
#define SRC_SDK_RESULT_SET_SQL_H_

#include <memory>
#include <string>
#include <vector>

#include "brpc/controller.h"
#include "butil/iobuf.h"
#include "schema/index_util.h"
#include "proto/tablet.pb.h"
#include "sdk/base_impl.h"
#include "sdk/codec_sdk.h"
#include "sdk/result_set.h"
#include "sdk/result_set_base.h"

namespace openmldb {
namespace sdk {

class ResultSetSQL : public ::hybridse::sdk::ResultSet {
 public:
    ResultSetSQL(const ::hybridse::vm::Schema& schema, uint32_t record_cnt, uint32_t buf_size,
                 const std::shared_ptr<brpc::Controller>& cntl);

    ResultSetSQL(const ::hybridse::vm::Schema& schema, uint32_t record_cnt,
                 const std::shared_ptr<butil::IOBuf>& io_buf);

    ~ResultSetSQL();

    static std::shared_ptr<::hybridse::sdk::ResultSet> MakeResultSet(
        const std::shared_ptr<::openmldb::api::QueryResponse>& response, const std::shared_ptr<brpc::Controller>& cntl,
        ::hybridse::sdk::Status* status);

    static std::shared_ptr<::hybridse::sdk::ResultSet> MakeResultSet(
        const std::shared_ptr<::openmldb::api::ScanResponse>& response,
        const ::google::protobuf::RepeatedField<uint32_t>& projection, const std::shared_ptr<brpc::Controller>& cntl,
        std::shared_ptr<::hybridse::vm::TableHandler> table_handler, ::hybridse::sdk::Status* status);

    static std::shared_ptr<::hybridse::sdk::ResultSet> MakeResultSet(
        const std::vector<std::string>& fields, const std::vector<std::vector<std::string>>& records,
        ::hybridse::sdk::Status* status);

    static std::shared_ptr<::hybridse::sdk::ResultSet> MakeResultSet(
        const ::openmldb::schema::PBSchema& schema, const std::vector<std::vector<std::string>>& records,
        ::hybridse::sdk::Status* status);

    bool Init();

    bool Reset() override { return result_set_base_->Reset(); }

    bool Next() override { return result_set_base_->Next(); }

    bool IsNULL(int index) override { return result_set_base_->IsNULL(index); }

    bool GetString(uint32_t index, std::string* str) override { return result_set_base_->GetString(index, str); }

    bool GetBool(uint32_t index, bool* result) override { return result_set_base_->GetBool(index, result); }

    bool GetChar(uint32_t index, char* result) override { return result_set_base_->GetChar(index, result); }

    bool GetInt16(uint32_t index, int16_t* result) override { return result_set_base_->GetInt16(index, result); }

    bool GetInt32(uint32_t index, int32_t* result) override { return result_set_base_->GetInt32(index, result); }

    bool GetInt64(uint32_t index, int64_t* result) override { return result_set_base_->GetInt64(index, result); }

    bool GetFloat(uint32_t index, float* result) override { return result_set_base_->GetFloat(index, result); }

    bool GetDouble(uint32_t index, double* result) override { return result_set_base_->GetDouble(index, result); }

    bool GetDate(uint32_t index, int32_t* date) override { return result_set_base_->GetDate(index, date); }

    bool GetDate(uint32_t index, int32_t* year, int32_t* month, int32_t* day) override {
        return result_set_base_->GetDate(index, year, month, day);
    }

    bool GetTime(uint32_t index, int64_t* mills) override { return result_set_base_->GetTime(index, mills); }

    const ::hybridse::sdk::Schema* GetSchema() override { return result_set_base_->GetSchema(); }

    int32_t Size() override { return result_set_base_->Size(); }

    void CopyTo(void* buf) override { return result_set_base_->CopyTo(buf); }

    int32_t GetDataLength() override { return result_set_base_->GetDataLength(); }

 private:
    ::hybridse::vm::Schema schema_;
    uint32_t record_cnt_;
    uint32_t buf_size_;
    std::shared_ptr<brpc::Controller> cntl_;
    ResultSetBase* result_set_base_;
    std::shared_ptr<butil::IOBuf> io_buf_;
};

class MultipleResultSetSQL : public ::hybridse::sdk::ResultSet {
 public:
    explicit MultipleResultSetSQL(const std::vector<std::shared_ptr<ResultSetSQL>>& result_set_list,
            const int limit_cnt = 0)
        : result_set_list_(result_set_list), result_set_idx_(0), limit_cnt_(limit_cnt), result_idx_(0) {}
    ~MultipleResultSetSQL() {}

    static std::shared_ptr<::hybridse::sdk::ResultSet> MakeResultSet(
        const std::vector<std::shared_ptr<ResultSetSQL>>& result_set_list,
        const int limit_cnt, ::hybridse::sdk::Status* status) {
        auto rs = std::make_shared<openmldb::sdk::MultipleResultSetSQL>(result_set_list, limit_cnt);
        if (!rs->Init()) {
            status->code = -1;
            status->msg = "request error, MultipleResultSetSQL init failed";
            return std::shared_ptr<ResultSet>();
        }
        return rs;
    }
    bool Init() {
        if (result_set_list_.empty()) {
            return false;
        }
        result_set_idx_ = 0;
        result_idx_ = 0;
        result_set_base_ = result_set_list_[0];
        return true;
    }

    bool Reset() override {
        // Fail to reset if result set is empty
        if (result_set_list_.empty()) {
            return false;
        }
        for (size_t i = 0; i < result_set_list_.size(); i++) {
            if (!result_set_list_[i]->Reset()) {
                return false;
            }
        }
        result_set_idx_ = 0;
        result_idx_ = 0;
        result_set_base_ = result_set_list_[0];
        return true;
    }

    bool Next() override {
        if (limit_cnt_ > 0 && result_idx_ >= limit_cnt_) {
            return false;
        }
        if (result_set_base_->Next()) {
            result_idx_++;
            return true;
        } else {
            result_set_idx_++;
            while (result_set_idx_ < result_set_list_.size()) {
                result_set_base_ = result_set_list_[result_set_idx_];
                if (result_set_base_->Next()) {
                    result_idx_++;
                    return true;
                } else {
                    result_set_idx_++;
                }
            }
            return false;
        }
        return false;
    }

    bool IsNULL(int index) override { return result_set_base_->IsNULL(index); }

    bool GetString(uint32_t index, std::string* str) override { return result_set_base_->GetString(index, str); }

    bool GetBool(uint32_t index, bool* result) override { return result_set_base_->GetBool(index, result); }

    bool GetChar(uint32_t index, char* result) override { return result_set_base_->GetChar(index, result); }

    bool GetInt16(uint32_t index, int16_t* result) override { return result_set_base_->GetInt16(index, result); }

    bool GetInt32(uint32_t index, int32_t* result) override { return result_set_base_->GetInt32(index, result); }

    bool GetInt64(uint32_t index, int64_t* result) override { return result_set_base_->GetInt64(index, result); }

    bool GetFloat(uint32_t index, float* result) override { return result_set_base_->GetFloat(index, result); }

    bool GetDouble(uint32_t index, double* result) override { return result_set_base_->GetDouble(index, result); }

    bool GetDate(uint32_t index, int32_t* date) override { return result_set_base_->GetDate(index, date); }

    bool GetDate(uint32_t index, int32_t* year, int32_t* month, int32_t* day) override {
        return result_set_base_->GetDate(index, year, month, day);
    }

    bool GetTime(uint32_t index, int64_t* mills) override { return result_set_base_->GetTime(index, mills); }

    const ::hybridse::sdk::Schema* GetSchema() override { return result_set_base_->GetSchema(); }

    int32_t Size() override {
        int total_size = 0;
        for (size_t i = 0 ; i < result_set_list_.size(); i++) {
            total_size += result_set_list_[i]->Size();
            if (limit_cnt_ > 0 && total_size > static_cast<int32_t>(limit_cnt_)) {
                return static_cast<int32_t>(limit_cnt_);
            }
        }
        return total_size;
    }

    // TODO: dl239
    int32_t GetDataLength() override { return 0; }
    void CopyTo(void* buf) override { }

 private:
    std::vector<std::shared_ptr<ResultSetSQL>> result_set_list_;
    uint32_t result_set_idx_;
    uint32_t limit_cnt_;
    uint32_t result_idx_;
    std::shared_ptr<ResultSetSQL> result_set_base_;
};

class ReadableResultSetSQL : public ::hybridse::sdk::ResultSet {
 public:
    explicit ReadableResultSetSQL(const std::shared_ptr<::hybridse::sdk::ResultSet>& rs) : rs_(rs) {}

    ~ReadableResultSetSQL() {}

    bool Reset() override { return rs_->Reset(); }

    bool Next() override { return rs_->Next(); }

    bool IsNULL(int index) override { return rs_->IsNULL(index); }

    bool GetString(uint32_t index, std::string* str) override { return rs_->GetString(index, str); }

    bool GetBool(uint32_t index, bool* result) override { return rs_->GetBool(index, result); }

    bool GetChar(uint32_t index, char* result) override { return rs_->GetChar(index, result); }

    bool GetInt16(uint32_t index, int16_t* result) override { return rs_->GetInt16(index, result); }

    bool GetInt32(uint32_t index, int32_t* result) override { return rs_->GetInt32(index, result); }

    bool GetInt64(uint32_t index, int64_t* result) override { return rs_->GetInt64(index, result); }

    bool GetFloat(uint32_t index, float* result) override { return rs_->GetFloat(index, result); }

    bool GetDouble(uint32_t index, double* result) override { return rs_->GetDouble(index, result); }

    bool GetDate(uint32_t index, int32_t* date) override { return rs_->GetDate(index, date); }

    bool GetDate(uint32_t index, int32_t* year, int32_t* month, int32_t* day) override {
        return rs_->GetDate(index, year, month, day);
    }

    bool GetTime(uint32_t index, int64_t* mills) override { return rs_->GetTime(index, mills); }

    const ::hybridse::sdk::Schema* GetSchema() override { return rs_->GetSchema(); }

    int32_t Size() override { return rs_->Size(); }

    const bool GetAsString(uint32_t idx, std::string& val) override;

    int32_t GetDataLength() override { return rs_->GetDataLength(); }
    void CopyTo(void* buf) override { rs_->CopyTo(buf); }

 private:
    std::shared_ptr<::hybridse::sdk::ResultSet> rs_;
};

}  // namespace sdk
}  // namespace openmldb
#endif  // SRC_SDK_RESULT_SET_SQL_H_
