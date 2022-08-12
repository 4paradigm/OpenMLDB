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

#include <stdlib.h>

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

#include "codec/schema_codec.h"
#include "nameserver/system_table.h"
#include "proto/name_server.pb.h"

using ::openmldb::sdk::DBSDK;
using ::openmldb::codec::SchemaCodec;
using TablePartitions = ::google::protobuf::RepeatedPtrField<::openmldb::nameserver::TablePartition>;

namespace openmldb {
namespace tools {

void TablemetaReader::copyFromRemote(const std::string& host, const std::string& source,
                                     const std::string& dest, int type) {
    char exec[200];
    if (type == TYPE_FILE) {
        snprintf(exec, sizeof(exec), "scp %s:%s %s", host.c_str(), source.c_str(), dest.c_str());
    } else {
        snprintf(exec, sizeof(exec), "scp -r %s:%s %s", host.c_str(), source.c_str(), dest.c_str());
    }
    printf("SCP Command: %s, ", exec);
    if (system(exec) == 0) {
        printf("copied successfully.\n");
    } else {
        printf("not copied successfully.\n");
    }
}

bool StandaloneTablemetaReader::ReadTableMeta() {
    printf("--------begin ReadTableMeta--------\n");
    ::openmldb::sdk::StandAloneSDK sdk(host_, port_);
    sdk.Init();
    auto tableInfo_ptr = sdk.GetTableInfo(db_name_, table_name_);
    tid_ = tableInfo_ptr->tid();
    // check whether table exists
    if (tid_ == 0) {
        printf("Table not found. Exit\n");
        return false;
    }
    schema_ = tableInfo_ptr->column_desc();

    for (const auto& tablePartition : tableInfo_ptr->table_partition()) {
        uint32_t pid = tablePartition.pid();
        for (const auto& partitionMeta : tablePartition.partition_meta()) {
            std::string path;
            if (partitionMeta.is_leader()) {
                std::string endpoint = partitionMeta.endpoint();
                // go to the corresponding machine to find the deployment directory through the endpoint
                if (tablet_map_.find(endpoint) != tablet_map_.end()) {
                    path = tablet_map_[endpoint];
                    std::string db_root_path = ReadDBRootPath(path, host_);

                    std::string data_path = db_root_path + "/" + std::to_string(tid_) + "_" + std::to_string(pid);
                    copyFromRemote(host_, data_path, tmp_path_.string(), TYPE_DIRECTORY);
                } else {
                    printf("Error. Cannot find endpoint.\n");
                }
                break;
            }
        }
    }
    printf("---------end ReadTableMeta---------\n");
    return true;
}

std::string StandaloneTablemetaReader::ReadDBRootPath(const std::string& deploy_dir, const std::string & host) {
    printf("--------start ReadDBRootPath--------\n");
    std::string tablet_path = deploy_dir + "/conf/standalone_tablet.flags";
    copyFromRemote(host, tablet_path, tmp_path_.string(), TYPE_FILE);

    std::string tablet_local_path =  tmp_path_.string() + "/standalone_tablet.flags";
    std::ifstream infile(tablet_local_path);
    std::string line;
    std::string db_root_path;
    std::string db_root = "--db_root_path";
    while (std::getline(infile, line)) {
        if (line.find(db_root) != std::string::npos && line[0] != '#') {
            db_root_path = line.substr(line.find("=") + 1);
            if (db_root_path[0] != '/')
                db_root_path = deploy_dir + db_root_path.substr(1);
            break;
        }
    }
    printf("---------end ReadDBRootPath---------\n");
    return db_root_path;
}

bool ClusterTablemetaReader::ReadTableMeta() {
    printf("--------begin ReadTableMeta--------\n");
    ::openmldb::sdk::ClusterSDK sdk(options_);
    sdk.Init();
    auto tableInfo_ptr = sdk.GetTableInfo(db_name_, table_name_);

    tid_ = tableInfo_ptr->tid();
    // check whether table exists
    if (tid_ == 0) {
        printf("Table not found. Exit\n");
        return false;
    }
    schema_ = tableInfo_ptr->column_desc();

    for (const auto& tablePartition : tableInfo_ptr->table_partition()) {
        uint32_t pid = tablePartition.pid();
        for (const auto& partitionMeta : tablePartition.partition_meta()) {
            std::string path, endpoint;
            if (partitionMeta.is_leader()) {
                endpoint = partitionMeta.endpoint();
                // go to the corresponding machine to find the deployment directory through the endpoint
                if (tablet_map_.find(endpoint) != tablet_map_.end()) {
                    path = tablet_map_[endpoint];
                    std::string host = endpoint.substr(0, endpoint.find(":"));
                    std::string db_root_path = ReadDBRootPath(path, host);

                    std::string data_path = db_root_path + "/" + std::to_string(tid_) + "_" + std::to_string(pid);
                    copyFromRemote(host, data_path, tmp_path_.string(), TYPE_DIRECTORY);
                } else {
                    printf("Error. Cannot find endpoint.\n");
                }
                break;
            }
        }
    }
    printf("---------end ReadTableMeta---------\n");
    return true;
}

std::string ClusterTablemetaReader::ReadDBRootPath(const std::string& deploy_dir, const std::string& host) {
    printf("--------start ReadDBRootPath--------\n");
    std::string tablet_path = deploy_dir + "/conf/tablet.flags";
    copyFromRemote(host, tablet_path, tmp_path_.string(), TYPE_FILE);

    std::string tablet_local_path =  tmp_path_.string() + "/tablet.flags";
    std::ifstream infile(tablet_local_path);
    std::string line;
    std::string db_root_path;
    std::string db_root = "--db_root_path";
    while (std::getline(infile, line)) {
        if (line.find(db_root) != std::string::npos && line[0] != '#') {
            db_root_path = line.substr(line.find("=") + 1);
            if (db_root_path[0] != '/')
                db_root_path = deploy_dir + db_root_path.substr(1);
            break;
        }
    }
    printf("---------end ReadDBRootPath---------\n");
    return db_root_path;
}

}  // namespace tools
}  // namespace openmldb
