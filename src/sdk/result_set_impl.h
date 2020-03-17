/*
 * result_set_impl.h
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

#ifndef SRC_SDK_RESULT_SET_IMPL_H_
#define SRC_SDK_RESULT_SET_IMPL_H_

#include "sdk/result_set.h"
#include "proto/tablet.pb.h"
#include "codec/row_codec.h"

namespace fesql {
namespace sdk {

class ResultSetImpl : public ResultSet {
 public:
    ResultSetImpl(std::unique_ptr<tablet::QueryResponse> response);
    ~ResultSetImpl();

    bool Init();
    bool Next();

    bool Close();

    bool GetString(uint32_t index, char** result, uint32_t* size);

    bool GetBool(uint32_t index, bool* result);

    bool GetChar(uint32_t index, char* result);

    bool GetInt16(uint32_t index, int16_t* result);

    bool GetInt32(uint32_t index, int32_t* result);

    bool GetInt64(uint32_t index, int64_t* result);

    bool GetFloat(uint32_t index, float* result);

    bool GetDouble(uint32_t index, double* result);

    bool GetDate(uint32_t index, uint32_t* days);

    bool GetTime(uint32_t index, int64_t* mills);

 private:
    std::unique_ptr<tablet::QueryResponse> response_;
    vm::Schema schema_;
    int32_t index_;
    int32_t size_;
    std::unique_ptr<codec::RowView> row_view_;
};

}  // namespace sdk
}  // namespace fesql
#endif  // SRC_SDK_RESULT_SET_IMPL_H_
