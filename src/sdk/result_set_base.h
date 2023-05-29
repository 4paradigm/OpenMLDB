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

#ifndef SRC_SDK_RESULT_SET_BASE_H_
#define SRC_SDK_RESULT_SET_BASE_H_
#include <memory>
#include <string>

#include "butil/iobuf.h"
#include "sdk/base_impl.h"
#include "sdk/codec_sdk.h"

namespace openmldb {
namespace sdk {

class ResultSetBase {
 public:
    ResultSetBase(const butil::IOBuf* buf, uint32_t count, uint32_t buf_size,
                  std::unique_ptr<::hybridse::sdk::RowIOBufView> row_view, const ::hybridse::vm::Schema& schema);
    ~ResultSetBase();

    bool Reset();

    bool Next();

    bool IsNULL(int index);

    bool GetString(uint32_t index, std::string* str);

    bool GetBool(uint32_t index, bool* result);

    bool GetChar(uint32_t index, char* result);

    bool GetInt16(uint32_t index, int16_t* result);

    bool GetInt32(uint32_t index, int32_t* result);

    bool GetInt64(uint32_t index, int64_t* result);

    bool GetFloat(uint32_t index, float* result);

    bool GetDouble(uint32_t index, double* result);

    bool GetDate(uint32_t index, int32_t* date);

    bool GetDate(uint32_t index, int32_t* year, int32_t* month, int32_t* day);

    bool GetTime(uint32_t index, int64_t* mills);

    inline const ::hybridse::sdk::Schema* GetSchema() { return &schema_; }

    inline int32_t Size() { return count_; }

    const butil::IOBuf& GetRecordValue() const { return cur_record_; }

 private:
    const butil::IOBuf* io_buf_;
    butil::IOBuf cur_record_;
    uint32_t count_;
    uint32_t buf_size_;
    std::unique_ptr<::hybridse::sdk::RowIOBufView> row_view_;
    ::hybridse::sdk::SchemaImpl schema_;
    uint32_t position_;
    int32_t index_;
};

}  // namespace sdk
}  // namespace openmldb

#endif  // SRC_SDK_RESULT_SET_BASE_H_
