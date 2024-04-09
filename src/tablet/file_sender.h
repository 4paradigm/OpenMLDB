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

#include <brpc/channel.h>
#include <brpc/controller.h>

#include <string>

#include "proto/tablet.pb.h"
#include "auth/brpc_authenticator.h"

namespace openmldb {
namespace tablet {

class FileSender {
 public:
    FileSender(uint32_t tid, uint32_t pid, common::StorageMode storage_mode, const std::string& endpoint);
    ~FileSender();
    bool Init();
    int SendFile(const std::string& file_name, const std::string& dir_name, const std::string& full_path);
    int SendFile(const std::string& file_name, const std::string& full_path);
    int SendFileInternal(const std::string& file_name, const std::string& dir_name, const std::string& full_path,
                         uint64_t file_size);
    int SendDir(const std::string& dir_name, const std::string& full_path);
    int WriteData(const std::string& file_name, const std::string& dir_name, const char* buffer, size_t len,
                  uint64_t block_id);
    int CheckFile(const std::string& file_name, const std::string& dir_name, uint64_t file_size);

 private:
    uint32_t tid_;
    uint32_t pid_;
    common::StorageMode storage_mode_;
    std::string endpoint_;
    uint32_t cur_try_time_;
    uint32_t max_try_time_;
    uint64_t limit_time_;
    brpc::Channel* channel_;
    ::openmldb::api::TabletServer_Stub* stub_;
    openmldb::authn::BRPCAuthenticator client_authenticator_;
};

}  // namespace tablet
}  // namespace openmldb
