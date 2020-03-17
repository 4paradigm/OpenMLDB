/*
 * result_set_impl.cc
 * Copyright (C) 4paradigm.com 2020 wangtaize <wangtaize@4paradigm.com>
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

#include "sdk/result_set_impl.h"

#include "glog/logging.h"

namespace fesql {
namespace sdk {

ResultSetImpl::ResultSetImpl(std::unique_ptr<tablet::QueryResponse> response):response_(std::move(response)), schema_(), index_(-1),
    size_(response_->result_set().size()) {}

ResultSetImpl::~ResultSetImpl() {}

bool ResultSetImpl::Init() {
    schema_ = response_->schema();
    std::unique_ptr<codec::RowView> row_view(new codec::RowView(schema_));
    row_view_ = std::move(row_view);
    return true;
}

bool ResultSetImpl::Next() {
    index_++;
    if (index_ < size_){
        row_view_->Reset(reinterpret_cast<const int8_t*>(response_->result_set(index_).c_str()));
        return true;
    } 
    return false;
}

bool ResultSetImpl::GetString(uint32_t index, char** result, uint32_t* size) {
    if (result == NULL || size == NULL) {
        LOG(WARNING) << "input ptr is null pointer";
        return false;
    }
    int32_t ret = row_view_->GetString(index, result, size);
    if (ret == 0) return true;
    return false;
}

bool ResultSetImpl::GetBool(uint32_t index, bool* val) {
    if (val == NULL) {
        LOG(WARNING) << "input ptr is null pointer";
        return false;
    }
    int32_t ret = row_view_->GetBool(index, val);
    return ret == 0;
}

bool ResultSetImpl::GetChar(uint32_t index, char* result) {
    return false;
}

bool ResultSetImpl::GetInt16(uint32_t index, int16_t* result) {
    if (result == NULL) {
        LOG(WARNING) << "input ptr is null pointer";
        return false;
    }
    int32_t ret = row_view_->GetInt16(index, result);
    return ret == 0;
}


bool ResultSetImpl::GetInt32(uint32_t index, int32_t* result) {
    if (result == NULL) {
        LOG(WARNING) << "input ptr is null pointer";
        return false;
    }
    int32_t ret = row_view_->GetInt32(index, result);
    return ret == 0;
}

bool ResultSetImpl::GetInt64(uint32_t index, int64_t* result) {
    if (result == NULL) {
        LOG(WARNING) << "input ptr is null pointer";
        return false;
    }
    int32_t ret = row_view_->GetInt64(index, result);
    return ret == 0;
}

bool ResultSetImpl::GetFloat(uint32_t index, float* result) {
    if (result == NULL) {
        LOG(WARNING) << "input ptr is null pointer";
        return false;
    }
    int32_t ret = row_view_->GetFloat(index, result);
    return ret == 0;
}

bool ResultSetImpl::GetDouble(uint32_t index, double* result) {

    if (result == NULL) {
        LOG(WARNING) << "input ptr is null pointer";
        return false;
    }
    int32_t ret = row_view_->GetDouble(index, result);
    return ret == 0;
}

bool ResultSetImpl::GetDate(uint32_t index, uint32_t* days) {
    if (days == NULL) {
        LOG(WARNING) << "input ptr is null pointer";
        return false;
    }
    return false;
}

bool ResultSetImpl::GetTime(uint32_t index, int64_t *mills) {
    if (mills == NULL) {
        LOG(WARNING) << "input ptr is null pointer";
        return false;
    }
    int32_t ret = row_view_->GetTimestamp(index, mills);
    return ret == 0;
}

}  // namespace of sdk
}  // namespace of fesql



