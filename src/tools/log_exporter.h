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

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.


#ifndef SRC_TOOLS_LOG_EXPORTER_H_
#define SRC_TOOLS_LOG_EXPORTER_H_

#include <vector>
#include <string>

#include "codec/codec.h"

using ::openmldb::codec::Schema;
using ::openmldb::codec::RowView;

namespace openmldb {
namespace tools {

class LogExporter {
 public:
    explicit LogExporter(const std::string& file_path, std::ofstream& table_cout) : table_dir_path_(file_path),
                                                                                 table_cout_(table_cout),
                                                                                 offset_(0) {}

    ~LogExporter() {}

    void ExportTable();

    void ReadManifest();

    void SetSchema(const Schema& schema) { schema_ = schema; }

    Schema GetSchema() const { return schema_; }

    std::string GetSnapshotPath() { return snapshot_path_; }

    int GetOffset() const { return offset_; }

 private:
    std::string table_dir_path_;
    std::ofstream& table_cout_;
    uint64_t offset_;
    std::string snapshot_path_;
    Schema schema_;

    uint64_t GetLogStartOffset(std::string&);

    void ReadLog(const std::string&);

    void ReadSnapshot();

    void WriteToFile(RowView&);
};

}  // namespace tools
}  // namespace openmldb

#endif  // SRC_TOOLS_LOG_EXPORTER_H_
