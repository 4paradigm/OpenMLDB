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

using ::openmldb::sdk::SQLClusterRouter;
using ::openmldb::sdk::DBSDK;
using ::openmldb::codec::SchemaCodec;
using TablePartitions = ::google::protobuf::RepeatedPtrField<::openmldb::nameserver::TablePartition>;

namespace openmldb {
namespace tools {

void StandaloneTablemetaReader::ReadTableMeta(const std::string& db, const std::string& table) {
    DBSDK *sdk = new StandAloneSDK(host_, port_);
    ASSERT_TRUE(sdk->Init());
    SQLClusterRouter *sr = new SQLClusterRouter(sdk);
    sr->Init();
    ::openmldb::nameserver::TableInfo tableInfo = sr->GetTableInfo(db, table);

    tid_ = tableInfo.tid();
    schema_ = tableInfo.column_desc();

    for (const auto& tablePartition : tableInfo.table_partition()) {
        uint32 pid = tablePartition.pid();
        std::string ip;
        uint32_t port;
        for (const auto& partitionMeta : tablePartition.partition_meta()) {
            if (partitionMeta.is_leader()) {
                std::string endpoint = partitionMeta.endpoint();
                size_t pos = endpoint.find(":");
                ip = endpoint.substr(0, pos);
                port = std::atoi(endpoint.substr(po s+1, endpoint.size()-1));
                // TODO(xiaopanz) : go to the corresponding machine to find the deployment directory through the port

                break;
            }

        }
    }
}

void StandaloneTablemetaReader::ReadDBRootPath(const std::string& deploy_dir) {
    printf("--------start ReadDBRootPath--------\n");
    std::string tablet_path = deploy_dir + "/conf/standalone_tablet.flags";
    std::ifstream infile(tablet_path);
    std::string line;
    std::string db_root = "--db_root_path";
    while (std::getline(infile, line))
    {
        if (line.find(db_root) != std::string::npos) {
            db_root_path_ = line.substr(x.find("=") + 1);
            printf("The database root path is: %s\n", db_root_path_.c_str());
            break;
        }
    }
    printf("--------end ReadDBRootPath--------\n");
}

void ClusterTablemetaReader::ReadTableMeta(const std::string& db, const std::string& table) {
    DBSDK *sdk = new ClusterSDK(options_);
    ASSERT_TRUE(sdk->Init());
    SQLClusterRouter *sr = new SQLClusterRouter(sdk);
    sr->Init();
    ::openmldb::nameserver::TableInfo tableInfo = sr->GetTableInfo(db, table);

    tid_ = tableInfo.tid();
    schema_ = tableInfo.column_desc();

    for (const auto& tablePartition : tableInfo.table_partition()) {
        uint32 pid = tablePartition.pid();
        std::string ip;
        uint32_t port;
        for (const auto& partitionMeta : tablePartition.partition_meta()) {
            if (partitionMeta.is_leader()) {
                std::string endpoint = partitionMeta.endpoint();
                size_t pos = endpoint.find(":");
                ip = endpoint.substr(0, pos);
                port = std::atoi(endpoint.substr(po s+1, endpoint.size()-1));
                // TODO(xiaopanz) : go to the corresponding machine to find the deployment directory through the port

                break;
            }

        }
    }
}

void ClusterTablemetaReader::ReadDBRootPath(const std::string& deploy_dir) {
    printf("--------start ReadDBRootPath--------\n");
    std::string tablet_path = deploy_dir + "/conf/tablet.flags";
    std::ifstream infile(tablet_path);
    std::string line;
    std::string db_root = "--db_root_path";
    while (std::getline(infile, line))
    {
        if (line.find(db_root) != std::string::npos) {
            db_root_path_ = line.substr(x.find("=") + 1);
            printf("The database root path is: %s\n", db_root_path_.c_str());
            break;
        }
    }
    printf("--------end ReadDBRootPath--------\n");
}

}  // namespace tools
}  // namespace openmldb

