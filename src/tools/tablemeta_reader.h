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

#include "sdk/sql_cluster_router.h"
#include "sdk/db_sdk.h"
#include "proto/common.pb.h"

using ::openmldb::sdk::ClusterOptions;
using Schema = ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnDesc>;

namespace openmldb {
namespace tools {

class TablemetaReader {
 public:
    // TablemetaReader(std::string db_name, std::string table_name) : db_name_(db_name), table_name_(table_name) {}

    virtual ~TablemetaReader() { }

    virtual bool IsClusterMode() const = 0;

    void SetDBName(std::string db_name) { db_name_ = db_name; }

    std::string GetDBName() { return  db_name_; }

    void SetTableName(std::string table_name) { table_name_ = table_name; }

    std::string GetTableName() { return  table_name_; }

    virtual void ReadTableMeta(const std::string&, const std::string&) = 0;

    virtual void ReadDBRootPath(const std::string&) = 0;

 protected:
    std::string db_root_path_;
    std::string db_name_;
    std::string table_name_;
    Schema schema_;
    uint32_t tid_;
};

class ClusterTableMetaReader : public TablemetaReader {
 public:
    explicit ClusterTableMetaReader(const ClusterOptions& options) : options_(options) {}

    bool IsClusterMode() const override { return true; }

    void ReadTableMeta(const std::string&, const std::string&) override;

    void ReadDBRootPath(const std::string&) override;

 private:
    ClusterOptions options_;
};


class StandaloneTablemetaReader : public TablemetaReader {
 public:
    StandaloneTablemetaReader(std::string host, int port) : host_(host), port_(port) {}

    bool IsClusterMode() const override { return false; }

    void ReadTableMeta(const std::string&, const std::string&) override;

    void ReadDBRootPath(const std::string&) override;

 private:
    std::string host_;
    uint32_t port_;
};

}  // namespace tools
}  // namespace openmldb

#endif  // SRC_TOOLS_TABLEMETA_READER_H_
