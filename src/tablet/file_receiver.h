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
namespace tablet {

class FileReceiver {
 public:
    FileReceiver(const std::string& file_name, const std::string& dir_name,
                 const std::string& path);
    ~FileReceiver();
    FileReceiver(const FileReceiver&) = delete;
    FileReceiver& operator=(const FileReceiver&) = delete;
    bool Init();
    int WriteData(const std::string& data, uint64_t block_id);
    void SaveFile();
    uint64_t GetBlockId();

 private:
    std::string file_name_;
    std::string dir_name_;
    std::string path_;
    uint64_t size_;
    uint64_t block_id_;
    FILE* file_;
};

}  // namespace tablet
}  // namespace openmldb
