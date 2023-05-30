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

#include "sdk/result_set_base.h"

#include <utility>

namespace openmldb {
namespace sdk {

ResultSetBase::ResultSetBase(const butil::IOBuf* buf, uint32_t count, uint32_t buf_size,
                             std::unique_ptr<::hybridse::sdk::RowIOBufView> row_view,
                             const ::hybridse::vm::Schema& schema)
    : io_buf_(buf),
      count_(count),
      buf_size_(buf_size),
      row_view_(std::move(row_view)),
      schema_(),
      position_(0),
      index_(-1) {
    schema_.SetSchema(schema);
}

ResultSetBase::~ResultSetBase() {}

bool ResultSetBase::Reset() {
    index_ = -1;
    position_ = 0;
    return true;
}

bool ResultSetBase::Next() {
    index_++;
    if (index_ < static_cast<int32_t>(count_) && position_ < buf_size_) {
        // get row size
        uint32_t row_size = 0;
        io_buf_->copy_to(reinterpret_cast<void*>(&row_size), 4, position_ + 2);
        DLOG(INFO) << "row size " << row_size << " position " << position_ << " byte size " << buf_size_;
        butil::IOBuf tmp;
        io_buf_->append_to(&tmp, row_size, position_);
        position_ += row_size;
        bool ok = row_view_->Reset(tmp);
        if (!ok) {
            LOG(WARNING) << "reset row buf failed";
            return false;
        }
        return true;
    }
    return false;
}

bool ResultSetBase::IsNULL(int index) { return row_view_->IsNULL(index); }

bool ResultSetBase::GetString(uint32_t index, std::string* str) {
    if (str == NULL) {
        LOG(WARNING) << "input ptr is null pointer";
        return false;
    }
    butil::IOBuf tmp;
    int32_t ret = row_view_->GetString(index, &tmp);
    if (ret == 0) {
        DLOG(INFO) << "get str size " << tmp.size();
        tmp.append_to(str, tmp.size(), 0);
        return true;
    }
    DLOG(INFO) << "fail to get string with ret " << ret;
    return false;
}

bool ResultSetBase::GetBool(uint32_t index, bool* val) {
    if (val == NULL) {
        LOG(WARNING) << "input ptr is null pointer";
        return false;
    }
    int32_t ret = row_view_->GetBool(index, val);
    return ret == 0;
}

bool ResultSetBase::GetChar(uint32_t index, char* result) { return false; }

bool ResultSetBase::GetInt16(uint32_t index, int16_t* result) {
    if (result == NULL) {
        LOG(WARNING) << "input ptr is null pointer";
        return false;
    }
    int32_t ret = row_view_->GetInt16(index, result);
    return ret == 0;
}

bool ResultSetBase::GetInt32(uint32_t index, int32_t* result) {
    if (result == NULL) {
        LOG(WARNING) << "input ptr is null pointer";
        return false;
    }
    int32_t ret = row_view_->GetInt32(index, result);
    return ret == 0;
}

bool ResultSetBase::GetInt64(uint32_t index, int64_t* result) {
    if (result == NULL) {
        LOG(WARNING) << "input ptr is null pointer";
        return false;
    }
    int32_t ret = row_view_->GetInt64(index, result);
    return ret == 0;
}

bool ResultSetBase::GetFloat(uint32_t index, float* result) {
    if (result == NULL) {
        LOG(WARNING) << "input ptr is null pointer";
        return false;
    }
    int32_t ret = row_view_->GetFloat(index, result);
    return ret == 0;
}

bool ResultSetBase::GetDouble(uint32_t index, double* result) {
    if (result == NULL) {
        LOG(WARNING) << "input ptr is null pointer";
        return false;
    }
    int32_t ret = row_view_->GetDouble(index, result);
    return ret == 0;
}

bool ResultSetBase::GetDate(uint32_t index, int32_t* date) {
    if (date == NULL) {
        LOG(WARNING) << "input ptr is null pointer";
        return false;
    }
    int32_t ret = row_view_->GetDate(index, date);
    return ret == 0;
}

bool ResultSetBase::GetDate(uint32_t index, int32_t* year, int32_t* month, int32_t* day) {
    if (day == NULL) {
        LOG(WARNING) << "input ptr is null pointer";
        return false;
    }
    return 0 == row_view_->GetDate(index, year, month, day);
}

bool ResultSetBase::GetTime(uint32_t index, int64_t* mills) {
    if (mills == NULL) {
        LOG(WARNING) << "input ptr is null pointer";
        return false;
    }
    int32_t ret = row_view_->GetTimestamp(index, mills);
    return ret == 0;
}

}  // namespace sdk
}  // namespace openmldb
