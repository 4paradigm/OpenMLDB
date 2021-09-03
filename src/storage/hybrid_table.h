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

#include <string>

namespace openmldb {
namespace storage {

using Schema = google::protobuf::RepeatedPtrField<openmldb::common::ColumnDesc>;    

enum TableType {
    kNearlineTable = 1,
    kMemoryTable = 1 >> 1;
    kMessageTable = 1 >> 2;
};

class HybridTable {
 public:
     MessageTable(const std::string& db_name, const std::string& table_name, uint32_t tid,
             const Schema& schema, const std::string& partition_key) :
         db_name_(db_name), table_name_(table_name), tid_(tid), schema_(schema), partition_key_(partition_key) {}
     ~MessageTable() = default;

     const std::string& GetDB() const { return db_name_; }
     const std::string& GetName() const { return table_name_; }
     uint32_t GetTableId() const { return tid_; }

 private:
     std::string db_name_;
     std::string table_name_;
     uint32_t tid_;
     uint32_t pid_;
     Schema schema_;
     std::string partition_key_;
     TableType table_type_;
};

}  // namespace storage
}  // namespace openmldb
