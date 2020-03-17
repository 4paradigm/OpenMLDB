/*
 * result_set.h
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

#ifndef SRC_SDK_RESULT_SET_H_
#define SRC_SDK_RESULT_SET_H_

#include <stdint.h>

namespace fesql {
namespace sdk {

class ResultSet {

 public:
    ResultSet() {}

    virtual ~ResultSet() {}

    virtual bool Next() = 0;

    virtual bool Close() = 0;

    virtual bool GetString(uint32_t index, char** result, uint32_t* size) = 0;

    virtual bool GetBool(uint32_t index, bool* result) = 0;

    virtual bool GetChar(uint32_t index, char* result) = 0;

    virtual bool GetInt16(uint32_t index, int16_t* result) = 0;

    virtual bool GetInt32(uint32_t index, int32_t* result) = 0;

    virtual bool GetInt64(uint32_t index, int64_t* result) = 0;

    virtual bool GetFloat(uint32_t index, float* result) = 0;

    virtual bool GetDouble(uint32_t index, double* result) = 0;

    virtual bool GetDate(uint32_t index, uint32_t* days) = 0;

    virtual bool GetTime(uint32_t index, int64_t* mills) = 0;

};

}  // namespace sdk
}  // namespace fesql
#endif  // SRC_SDK_RESULT_SET_H_
