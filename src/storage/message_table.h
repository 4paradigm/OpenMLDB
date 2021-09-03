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

class MessageTable {
 public:
     MessageTable(const std::string& db_name, const std::string& table_name, uint32_t tid, uint32_t pid) :
         db_name_(db_name), table_name_(table_name), tid_(tid), pid_(pid) {}
     ~MessageTable() = default;

     const std::string& GetDB() const { return db_name_; }
     const std::string& GetName() const { return table_name_; }
     uint32_t GetTableId() const { return tid_; }
     uint32_t GetPartitionId() const { return pid_; }

     bool Put() {
         return true;
     }

 private:
     std::string db_name_;
     std::string table_name_;
     uint32_t tid_;
     uint32_t pid_;
};

}  // namespace storage
}  // namespace openmldb
