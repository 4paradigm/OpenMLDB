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

#include "base/glog_wrapper.h"
#include "codec/schema_codec.h"
#include "proto/name_server.pb.h"

DEFINE_string(user_name, "", "the user name of deploy machine");

using ::openmldb::codec::SchemaCodec;

namespace openmldb {
namespace tools {

void TablemetaReader::CopyFromRemote(const std::string& host, const std::string& source,
                                     const std::string& dest, int type) {
    char exec[MAX_COMMAND_LEN];
    if (type == TYPE_FILE) {
        if (FLAGS_user_name.empty()) {
            snprintf(exec, sizeof(exec), "scp %s:%s %s", host.c_str(), source.c_str(), dest.c_str());
        } else {
            snprintf(exec, sizeof(exec), "scp %s@%s:%s %s", FLAGS_user_name.c_str(),
                                                        host.c_str(), source.c_str(), dest.c_str());
        }
    } else {
        if (FLAGS_user_name.empty()) {
            snprintf(exec, sizeof(exec), "scp -r %s:%s %s", host.c_str(), source.c_str(), dest.c_str());
        } else {
            snprintf(exec, sizeof(exec), "scp -r %s@%s:%s %s", FLAGS_user_name.c_str(),
                                                        host.c_str(), source.c_str(), dest.c_str());
        }
    }
    if (system(exec) == 0) {
        PDLOG(INFO, "SCP command: %s successfully executed.", exec);
    } else {
        PDLOG(WARNING, "SCP command: %s not successfully executed.", exec);
    }
}

bool TablemetaReader::ReadTableMeta(const std::string& mode) {
    tid_ = tableinfo_ptr_->tid();
    schema_ = tableinfo_ptr_->column_desc();

    for (const auto& table_partition : tableinfo_ptr_->table_partition()) {
        uint32_t pid = table_partition.pid();
        for (const auto& partition_meta : table_partition.partition_meta()) {
            std::string path;
            if (partition_meta.is_leader()) {
                std::string endpoint = partition_meta.endpoint();
                // go to the corresponding machine to find the deployment directory through the endpoint
                if (tablet_map_.find(endpoint) != tablet_map_.end()) {
                    path = tablet_map_[endpoint];
                    std::string host = endpoint.substr(0, endpoint.find(":"));
                    std::string db_root_path = ReadDBRootPath(path, host, mode);

                    std::string data_path = db_root_path + "/" + std::to_string(tid_) + "_" + std::to_string(pid);
                    CopyFromRemote(host, data_path, tmp_path_.string(), TYPE_DIRECTORY);
                } else {
                    PDLOG(ERROR, "Cannot find endpoint %s", endpoint);
                    return false;
                }
                break;
            }
        }
    }
    return true;
}

std::string TablemetaReader::ReadDBRootPath(const std::string& deploy_dir, const std::string& host,
                                            const std::string& mode) {
    std::string tablet_path;
    std::string tablet_local_path;
    if (mode == "standalone") {
        tablet_path = deploy_dir + "/conf/standalone_tablet.flags";
        CopyFromRemote(host, tablet_path, tmp_path_.string(), TYPE_FILE);
        tablet_local_path =  tmp_path_.string() + "/standalone_tablet.flags";
    } else {
        tablet_path = deploy_dir + "/conf/tablet.flags";
        CopyFromRemote(host, tablet_path, tmp_path_.string(), TYPE_FILE);
        tablet_local_path =  tmp_path_.string() + "/tablet.flags";
    }
    std::ifstream infile(tablet_local_path);
    std::string line;
    std::string db_root_path;
    std::string db_root = "--db_root_path";
    while (std::getline(infile, line)) {
        if (line.find(db_root) != std::string::npos && line[0] != '#') {
            db_root_path = line.substr(line.find("=") + 1);
            if (db_root_path[0] != '/') {
                if (deploy_dir.back() != '/') {
                    db_root_path = deploy_dir + "/" + db_root_path;
                } else {
                    db_root_path = deploy_dir + db_root_path;
                }
            }
            break;
        }
    }
    return db_root_path;
}

void StandaloneTablemetaReader::SetTableinfoPtr() {
    ::openmldb::sdk::StandAloneSDK standalone_sdk(options_);
    standalone_sdk.Init();
    tableinfo_ptr_ = standalone_sdk.GetTableInfo(db_name_, table_name_);
}

void ClusterTablemetaReader::SetTableinfoPtr() {
    ::openmldb::sdk::ClusterSDK cluster_sdk(options_);
    cluster_sdk.Init();
    tableinfo_ptr_ = cluster_sdk.GetTableInfo(db_name_, table_name_);
}

}  // namespace tools
}  // namespace openmldb
