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

#include "tools/tablemeta_reader.h"

#include <cstdlib>
#include <fstream>
#include <sstream>
#include <iostream>
#include <string>

#include "sdk/sql_cluster_router.h"
#include "nameserver/system_table.h"
#include "proto/name_server.pb.h"
#include "codec/schema_codec.h"
#include "yaml-cpp/yaml.h"

using ::openmldb::sdk::SQLClusterRouter;
using ::openmldb::sdk::DBSDK;
using ::openmldb::codec::SchemaCodec;
using TablePartitions = ::google::protobuf::RepeatedPtrField<::openmldb::nameserver::TablePartition>;

namespace openmldb {
namespace tools {

void TablemetaReader::ReadConfigYaml(const std::string &yaml_path) {
    // Reads nameserver's and tablet's endpoints and corresponding paths
    YAML::Node config = YAML::LoadFile(yaml_path);
    for (YAML::const_iterator iter = config["nameserver"].begin(); iter != config["nameserver"].end(); ++iter) {
        std::string endpoint = (*iter)["endpoint"].as<std::string>();
        if (ns_map_.find(endpoint) == ns_map_.end()) {
            printf("Error. Exists duplicate endpoints.\n");
        } else {
            ns_map_[endpoint] = (*iter)["path"].as<std::string>();
        }
    }
    for (YAML::const_iterator iter = config["tablet"].begin(); iter != config["tablet"].end(); ++iter)
    {
        std::string endpoint = (*iter)["tablet"].as<std::string>();
        if (tablet_map_.find(endpoint) == tablet_map_.end()) {
            printf("Error. Exists duplicate endpoints.\n");
        } else {
            tablet_map_[endpoint] = (*iter)["path"].as<std::string>();
        }
    }
}

void StandaloneTablemetaReader::ReadTableMeta() {
    ::openmldb::sdk::DBSDK *sdk = new ::openmldb::sdk::StandAloneSDK(host_, port_);
    sdk->Init();
    SQLClusterRouter *sr = new SQLClusterRouter(sdk);
    sr->Init();
    ::openmldb::nameserver::TableInfo tableInfo = sr->GetTableInfo(db_name_, table_name_);

    tid_ = tableInfo.tid();
    schema_ = tableInfo.column_desc();
    //std::filesystem::path tmp_path = std::filesystem::temp_directory_path() / std::filesystem::current_path();
    for (const auto& tablePartition : tableInfo.table_partition()) {
        uint32_t pid = tablePartition.pid();
        for (const auto& partitionMeta : tablePartition.partition_meta()) {
            std::string path;
            if (partitionMeta.is_leader()) {
                std::string endpoint = partitionMeta.endpoint();
                // go to the corresponding machine to find the deployment directory through the endpoint
                if (ns_map_.find(endpoint) == ns_map_.end()) {
                    path = ns_map_[endpoint];
                } else if (tablet_map_.find(endpoint) == tablet_map_.end()) {
                    path = tablet_map_[endpoint];
                } else {
                    printf("Error. Cannot find endpoint.\n");
                    return;
                }
                break;
            }
            std::string db_root_path = ReadDBRootPath(path);
            std::string data_path = db_root_path + "/" + std::to_string(tid_) + "_" + std::to_string(pid);
            std::filesystem::copy(data_path, tmp_path_);
        }
    }
}

std::string StandaloneTablemetaReader::ReadDBRootPath(const std::string& deploy_dir) {
    printf("--------start ReadDBRootPath--------\n");
    std::string tablet_path = deploy_dir + "/conf/standalone_tablet.flags";
    std::ifstream infile(tablet_path);
    std::string line;
    std::string db_root_path;
    std::string db_root = "--db_root_path";
    while (std::getline(infile, line)) {
        if (line.find(db_root) != std::string::npos) {
            db_root_path = line.substr(line.find("=") + 1);
            printf("The database root path is: %s\n", db_root_path.c_str());
            break;
        }
    }
    printf("--------end ReadDBRootPath--------\n");
    return db_root_path;
}

void ClusterTablemetaReader::ReadTableMeta() {
    ::openmldb::sdk::DBSDK *sdk = new ::openmldb::sdk::ClusterSDK(options_);
    sdk->Init();
    SQLClusterRouter *sr = new SQLClusterRouter(sdk);
    sr->Init();
    ::openmldb::nameserver::TableInfo tableInfo = sr->GetTableInfo(db_name_, table_name_);

    tid_ = tableInfo.tid();
    schema_ = tableInfo.column_desc();

    for (const auto& tablePartition : tableInfo.table_partition()) {
        uint32_t pid = tablePartition.pid();
        for (const auto& partitionMeta : tablePartition.partition_meta()) {
            std::string path;
            if (partitionMeta.is_leader()) {
                std::string endpoint = partitionMeta.endpoint();
                // go to the corresponding machine to find the deployment directory through the endpoint
                if (ns_map_.find(endpoint) == ns_map_.end()) {
                    path = ns_map_[endpoint];
                } else if (tablet_map_.find(endpoint) == tablet_map_.end()) {
                    path = tablet_map_[endpoint];
                } else {
                    printf("Error. Cannot find endpoint.\n");
                    return;
                }
                break;
            }
            std::string db_root_path = ReadDBRootPath(path);
            std::string data_path = db_root_path + "/" + std::to_string(tid_) + "_" + std::to_string(pid);
            std::filesystem::copy(data_path, tmp_path_);
        }
    }
}

std::string ClusterTablemetaReader::ReadDBRootPath(const std::string& deploy_dir) {
    printf("--------start ReadDBRootPath--------\n");
    std::string tablet_path = deploy_dir + "/conf/tablet.flags";
    std::ifstream infile(tablet_path);
    std::string line;
    std::string db_root_path;
    std::string db_root = "--db_root_path";
    while (std::getline(infile, line)) {
        if (line.find(db_root) != std::string::npos) {
            db_root_path = line.substr(line.find("=") + 1);
            printf("The database root path is: %s\n", db_root_path.c_str());
            break;
        }
    }
    printf("--------end ReadDBRootPath--------\n");
    return db_root_path;
}

}  // namespace tools
}  // namespace openmldb

