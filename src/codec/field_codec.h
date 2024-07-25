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

#pragma once

#include <stdint.h>
#include <string.h>

#include <algorithm>
#include <string>
#include <vector>

#include "base/endianconv.h"
#include "base/strings.h"
#include "boost/lexical_cast.hpp"
#include "proto/type.pb.h"
#include "sdk/base.h"

namespace openmldb {
namespace codec {

template <typename T>
static bool AppendColumnValue(const std::string& v, hybridse::sdk::DataType type, bool is_not_null,
                       const std::string& null_value, T row) {
    // check if null, empty string will cast fail and throw bad_lexical_cast
    if (v.empty() || v == null_value) {
        if (is_not_null) {
            return false;
        }
        return row->AppendNULL();
    }
    try {
        switch (type) {
            case hybridse::sdk::kTypeBool: {
                bool ok = false;
                std::string b_val = v;
                std::transform(b_val.begin(), b_val.end(), b_val.begin(), ::tolower);
                if (b_val == "true") {
                    ok = row->AppendBool(true);
                } else if (b_val == "false") {
                    ok = row->AppendBool(false);
                }
                return ok;
            }
            case hybridse::sdk::kTypeInt16: {
                return row->AppendInt16(boost::lexical_cast<int16_t>(v));
            }
            case hybridse::sdk::kTypeInt32: {
                return row->AppendInt32(boost::lexical_cast<int32_t>(v));
            }
            case hybridse::sdk::kTypeInt64: {
                return row->AppendInt64(boost::lexical_cast<int64_t>(v));
            }
            case hybridse::sdk::kTypeFloat: {
                return row->AppendFloat(boost::lexical_cast<float>(v));
            }
            case hybridse::sdk::kTypeDouble: {
                return row->AppendDouble(boost::lexical_cast<double>(v));
            }
            case hybridse::sdk::kTypeString: {
                return row->AppendString(v);
            }
            case hybridse::sdk::kTypeDate: {
                std::vector<std::string> parts;
                ::openmldb::base::SplitString(v, "-", parts);
                if (parts.size() != 3) {
                    return false;
                }
                auto year = boost::lexical_cast<int32_t>(parts[0]);
                auto mon = boost::lexical_cast<int32_t>(parts[1]);
                auto day = boost::lexical_cast<int32_t>(parts[2]);
                return row->AppendDate(year, mon, day);
            }
            case hybridse::sdk::kTypeTimestamp: {
                return row->AppendTimestamp(boost::lexical_cast<int64_t>(v));
            }
            default: {
                return false;
            }
        }
    } catch (std::exception const& e) {
        return false;
    }
}

}  // namespace codec
}  // namespace openmldb
