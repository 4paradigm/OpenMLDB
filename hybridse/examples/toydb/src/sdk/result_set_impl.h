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

#ifndef HYBRIDSE_EXAMPLES_TOYDB_SRC_SDK_RESULT_SET_IMPL_H_
#define HYBRIDSE_EXAMPLES_TOYDB_SRC_SDK_RESULT_SET_IMPL_H_

#include <memory>
#include <string>
#include "brpc/controller.h"
#include "butil/iobuf.h"
#include "codec/fe_row_codec.h"
#include "proto/fe_tablet.pb.h"
#include "sdk/base_impl.h"
#include "sdk/codec_sdk.h"
#include "sdk/result_set.h"

namespace hybridse {
namespace sdk {

class ResultSetImpl : public ResultSet {
 public:
    ResultSetImpl(std::unique_ptr<tablet::QueryResponse> response,
                  std::unique_ptr<brpc::Controller> cntl);

    ~ResultSetImpl();

    bool Init();

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

    bool GetDate(uint32_t index, int32_t* days);

    bool GetDate(uint32_t index, int32_t* year, int32_t* month, int32_t* day);

    bool GetTime(uint32_t index, int64_t* mills);

    inline const Schema* GetSchema() { return &schema_; }

    inline int32_t Size() { return response_->count(); }

    void CopyTo(void* buf) override {
        cntl_->response_attachment().copy_to(buf);
    }

    int32_t GetDataLength() override {
        return cntl_->response_attachment().size();
    }

 private:
    inline uint32_t GetRecordSize() { return response_->count(); }

 private:
    std::unique_ptr<tablet::QueryResponse> response_;
    int32_t index_;
    int32_t byte_size_;
    uint32_t position_;
    std::unique_ptr<sdk::RowIOBufView> row_view_;
    vm::Schema internal_schema_;
    SchemaImpl schema_;
    std::unique_ptr<brpc::Controller> cntl_;
};

}  // namespace sdk
}  // namespace hybridse
#endif  // HYBRIDSE_EXAMPLES_TOYDB_SRC_SDK_RESULT_SET_IMPL_H_
