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

#include <string>
#include <vector>
#include <unordered_map>
#include <filesystem>

#include "sdk/sql_cluster_router.h"
#include "sdk/db_sdk.h"
#include "proto/common.pb.h"

using ::openmldb::sdk::ClusterOptions;
using Schema = ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnDesc>;

namespace openmldb {
namespace tools {

class TablemetaReader {
 public:
    TablemetaReader(const std::string &db_name, const std::string &table_name) : db_name_(db_name), table_name_(table_name),
            tmp_path_(std::filesystem::temp_directory_path() / std::filesystem::current_path()) {}

    virtual ~TablemetaReader() {}

    virtual bool IsClusterMode() const = 0;

    void ReadConfigYaml(const std::string &);

    virtual void ReadTableMeta() = 0;

    std::filesystem::path GetTmpPath() { return tmp_path_; }

    Schema getSchema() { return schema_; }

 protected:
    virtual std::string ReadDBRootPath(const std::string&) = 0;

    std::string db_name_;
    std::string table_name_;
    std::unordered_map<std::string, std::string> ns_map_;
    std::unordered_map<std::string, std::string> tablet_map_;
    Schema schema_;
    uint32_t tid_;
    std::filesystem::path tmp_path_; // = std::filesystem::temp_directory_path() / std::filesystem::current_path();
};

class ClusterTablemetaReader : public TablemetaReader {
 public:
    ClusterTablemetaReader(const std::string &db_name, const std::string &table_name, const ClusterOptions& options) :
            TablemetaReader(db_name, table_name), options_(options) {}

    bool IsClusterMode() const override { return true; }

    void ReadTableMeta() override;

private:
    std::string ReadDBRootPath(const std::string&) override;

    ClusterOptions options_;
};


class StandaloneTablemetaReader : public TablemetaReader {
 public:
    StandaloneTablemetaReader(const std::string &db_name, const std::string &table_name, const std::string &host, int port) :
            TablemetaReader(db_name, table_name), host_(host), port_(port) {}

    bool IsClusterMode() const override { return false; }

    void ReadTableMeta() override;

 private:
    std::string ReadDBRootPath(const std::string&) override;

    std::string host_;
    uint32_t port_;
};

}  // namespace tools
}  // namespace openmldb

#endif  // SRC_TOOLS_TABLEMETA_READER_H_
