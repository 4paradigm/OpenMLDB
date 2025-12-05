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

#ifndef SRC_BASE_INDEX_UTIL_H_
#define SRC_BASE_INDEX_UTIL_H_

#include <map>

#include "codec/codec.h"

namespace openmldb {
namespace base {

// don't declare func in table header cuz swig sdk

// <pkey_col_name, (idx_in_row, type)>, error if empty
std::map<std::string, std::pair<uint32_t, type::DataType>> MakePkeysHint(const codec::Schema& schema,
                                                                         const common::ColumnKey& cidx_ck);

// error if empty
std::string MakeDeleteSQL(const std::string& db, const std::string& name, const common::ColumnKey& cidx_ck,
                          const int8_t* values, uint64_t ts, const codec::RowView& row_view,
                          const std::map<std::string, std::pair<uint32_t, type::DataType>>& col_idx);

// error if empty
std::string ExtractPkeys(const common::ColumnKey& cidx_ck, const int8_t* values, const codec::RowView& row_view,
                         const std::map<std::string, std::pair<uint32_t, type::DataType>>& col_idx);

}  // namespace base
}  // namespace openmldb

#endif  // SRC_BASE_INDEX_UTIL_H_
