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


#ifndef SRC_TOOLS_TABLEMETA_READER_H_
#define SRC_TOOLS_TABLEMETA_READER_H_

#include <filesystem>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "proto/common.pb.h"
#include "sdk/db_sdk.h"

constexpr int TYPE_FILE = 0;
constexpr int TYPE_DIRECTORY = 1;
constexpr int MAX_COMMAND_LEN = 300;

using ::openmldb::sdk::ClusterOptions;
using Schema = ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnDesc>;

namespace openmldb {
namespace tools {

class TablemetaReader {
 public:
    TablemetaReader(const std::string &db_name,
                    const std::string &table_name,
                    std::unordered_map<std::string, std::string> tablet_map) :
                    db_name_(db_name),
                    table_name_(table_name),
                    tablet_map_(tablet_map) {
        std::filesystem::create_directory("./tmp");
        tmp_path_ = "./tmp";
    }

    virtual ~TablemetaReader() {}

    virtual bool IsClusterMode() const = 0;

    bool ReadTableMeta(std::string&);

    virtual void SetTableinfoPtr() = 0;

    std::shared_ptr<openmldb::nameserver::TableInfo> GetTableinfoPtr() const { return tableinfo_ptr_; }

    std::filesystem::path GetTmpPath() const { return tmp_path_; }

    Schema GetSchema() const { return schema_; }

 protected:
    std::string ReadDBRootPath(const std::string& deploy_dir, const std::string& host, const std::string& mode = "");

    void CopyFromRemote(const std::string& host, const std::string& source, const std::string& dest, int type);

    std::string db_name_;
    std::string table_name_;
    std::unordered_map<std::string, std::string> tablet_map_;
    Schema schema_;
    uint32_t tid_;
    std::filesystem::path tmp_path_;
    std::shared_ptr<openmldb::nameserver::TableInfo> tableinfo_ptr_;
};

class ClusterTablemetaReader : public TablemetaReader {
 public:
    ClusterTablemetaReader(const std::string &db_name, const std::string &table_name,
                       std::unordered_map<std::string, std::string> tablet_map, const ClusterOptions& options) :
                       TablemetaReader(db_name, table_name, tablet_map), options_(options) {}

    void SetTableinfoPtr() override;

    bool IsClusterMode() const override { return true; }

 private:
    ClusterOptions options_;
};


class StandaloneTablemetaReader : public TablemetaReader {
 public:
    StandaloneTablemetaReader(const std::string &db_name, const std::string &table_name,
                          std::unordered_map<std::string, std::string> tablet_map, const std::string &host, int port) :
                          TablemetaReader(db_name, table_name, tablet_map), host_(host), port_(port) {}

    void SetTableinfoPtr() override;

    bool IsClusterMode() const override { return false; }

 private:
    std::string host_;
    uint32_t port_;
};

}  // namespace tools
}  // namespace openmldb

#endif  // SRC_TOOLS_TABLEMETA_READER_H_
