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

#include <algorithm>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "boost/algorithm/string.hpp"
#include "boost/container/deque.hpp"
#include "storage/segment.h"

namespace openmldb {
namespace codec {

using ::openmldb::storage::DataBlock;

class RowCodec {
 public:
    static int32_t CalStrLength(const std::map<std::string, std::string>& str_map, const Schema& schema);

    static int32_t CalStrLength(const std::vector<std::string>& input_value, const Schema& schema);

    static ::openmldb::base::Status EncodeRow(const std::vector<std::string> input_value, const Schema& schema,
                                                 uint32_t version,
                                                 std::string& row);  // NOLINT

    static ::openmldb::base::Status EncodeRow(const std::map<std::string, std::string>& str_map,
                                                 const Schema& schema, int32_t version,
                                                 std::string& row);  // NOLINT

    static bool DecodeRow(const Schema& schema, const ::openmldb::base::Slice& value,
                          std::vector<std::string>& value_vec);  // NOLINT

    static bool DecodeRow(const Schema& schema, const int8_t* data, int32_t size, bool replace_empty_str, int start,
                          int len, std::vector<std::string>& values);  // NOLINT

    static bool DecodeRow(const Schema& schema,
                          openmldb::codec::RowView& rv,           // NOLINT
                          std::vector<std::string>& value_vec);  // NOLINT

    static bool DecodeRow(const Schema& schema,
                          openmldb::codec::RowView& rv,  // NOLINT
                          bool replace_empty_str, int start, int length, std::vector<std::string>* value_vec);

    static bool DecodeRow(const openmldb::codec::RowView& rv, const int8_t* data,
            const std::vector<uint32_t>& cols, std::vector<std::string>* value_vec);
};

bool DecodeRows(const std::string& data, uint32_t count, const Schema& schema,
        std::vector<std::vector<std::string>>* row_vec);

void Encode(uint64_t time, const char* data, const size_t size, char* buffer, uint32_t offset);

void Encode(uint64_t time, const DataBlock* data, char* buffer, uint32_t offset);

void Encode(const char* data, const size_t size, char* buffer, uint32_t offset);

void Encode(const DataBlock* data, char* buffer, uint32_t offset);

int32_t EncodeRows(const std::vector<::openmldb::base::Slice>& rows, uint32_t total_block_size,
                                 std::string* body);

int32_t EncodeRows(const boost::container::deque<std::pair<uint64_t, ::openmldb::base::Slice>>& rows,
                                 uint32_t total_block_size, std::string* pairs);
// encode pk, ts and value
void EncodeFull(const std::string& pk, uint64_t time, const char* data, const size_t size, char* buffer,
                              uint32_t offset);

void EncodeFull(const std::string& pk, uint64_t time, const DataBlock* data, char* buffer,
                              uint32_t offset);

void Decode(const std::string* str, std::vector<std::pair<uint64_t, std::string*>>& pairs);  // NOLINT

void DecodeFull(const std::string* str,
                         std::map<std::string, std::vector<std::pair<uint64_t, std::string*>>>& value_map); // NOLINT

}  // namespace codec
}  // namespace openmldb
