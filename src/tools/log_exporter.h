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

#include <string>
#include <vector>
#include "codec/codec.h"

namespace openmldb {
namespace tools {

using ::openmldb::codec::Schema;
using ::openmldb::codec::RowView;
class Exporter {
 public:
    explicit Exporter(std::string file_path) : table_dir_path(file_path) {}

    ~Exporter() {}

    void ExportTable();

    void ReadManifest();

    void SetSchema(Schema schema_) { schema = schema_; }

    Schema GetSchema() { return schema; }

    std::string GetSnapshotPath() { return snapshot_path; }

    int GetOffset() { return offset; }

 private:
    uint64_t GetLogStartOffset(std::string&);
    void ReadLog(std::ofstream&, std::string&);
    void ReadSnapshot(std::ofstream&);
    void WriteToFile(std::ofstream&, RowView&);
    std::string table_dir_path;
    uint64_t offset;
    std::string snapshot_path;
    Schema schema;
};

}  // namespace tools
}  // namespace openmldb

#endif  // SRC_TOOLS_LOG_EXPORTER_H_
