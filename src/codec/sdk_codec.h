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

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "codec/schema_codec.h"
#include "proto/common.pb.h"
#include "proto/tablet.pb.h"

namespace openmldb {
namespace codec {

using Index = google::protobuf::RepeatedPtrField<::openmldb::common::ColumnKey>;
using Dimension = std::vector<std::pair<std::string, uint32_t>>;
using Schema = google::protobuf::RepeatedPtrField<openmldb::common::ColumnDesc>;
using VerSchema = google::protobuf::RepeatedPtrField<openmldb::common::VersionPair>;

class SDKCodec {
 public:
    explicit SDKCodec(const ::openmldb::nameserver::TableInfo& table_info);

    explicit SDKCodec(const ::openmldb::api::TableMeta& table_info);

    int EncodeDimension(const std::map<std::string, std::string>& raw_data, uint32_t pid_num,
                        std::map<uint32_t, Dimension>* dimensions);

    int EncodeDimension(const std::vector<std::string>& raw_data, uint32_t pid_num,
                        std::map<uint32_t, Dimension>* dimensions);



    int EncodeRow(const std::vector<std::string>& raw_data, std::string* row);

    int DecodeRow(const std::string& row, std::vector<std::string>* value);

    int CombinePartitionKey(const std::vector<std::string>& raw_data, std::string* key);

    inline bool HasTSCol() const { return !ts_idx_.empty(); }

    std::vector<std::string> GetColNames();

 private:
    void ParseColumnDesc(const Schema& column_desc);
    void ParseAddedColumnDesc(const Schema& column_desc);
    void ParseSchemaVer(const VerSchema& ver_schema, const Schema& add_schema);
    void ParseTsCol();

 private:
    Schema schema_;
    Index index_;
    std::map<std::string, uint32_t> schema_idx_map_;
    std::vector<uint32_t> ts_idx_;
    std::vector<uint32_t> partition_col_idx_;
    uint32_t base_schema_size_;
    std::map<int32_t, std::shared_ptr<Schema>> version_schema_;
    int32_t last_ver_;
};

}  // namespace codec
}  // namespace openmldb
