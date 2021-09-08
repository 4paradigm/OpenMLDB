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

#include "storage/message_table.h"
#include <gflags/gflags.h>
#include <map>
#include "base/glog_wapper.h"
#include "base/slice.h"

DECLARE_string(message_root_path);

namespace openmldb {
namespace storage {

bool MessageTable::Init() {
    std::string table_db_path = FLAGS_message_root_path + "/" + std::to_string(tid_) + "_" +
        std::to_string(pid_) + "_msg";
    replicator_ = std::make_shared<::openmldb::replica::LogReplicator>(tid_, pid_, table_db_path,
            std::map<std::string, std::string>(), ::openmldb::replica::ReplicatorRole::kLeaderNode);
    if (!replicator_->Init()) {
        return false;
    }
    return true;
}

bool MessageTable::AddMessage(const ::butil::IOBuf& message) {
    return replicator_->AppendMessage(message);
}

bool MessageTable::AddConsumer(const std::string& endpoint) {
    return true;
}

}  // namespace storage
}  // namespace openmldb
