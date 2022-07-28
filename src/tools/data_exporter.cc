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

#include <gflags/gflags.h>
#include <dirent.h>

#include <iostream>
#include <filesystem>
#include <vector>
#include <string>
#include <unordered_map>

#include "sdk/db_sdk.h"
#include "tools/tablemeta_reader.h"
#include "tools/log_exporter.h"
#include "proto/common.pb.h"
#include "yaml-cpp/yaml.h"
#include "version.h"  // NOLINT

DECLARE_string(host);
DECLARE_int32(port);
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DEFINE_string(db_name, "", "database name.");
DEFINE_string(table_name, "", "table name. ");

using Schema = ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnDesc>;
const std::string OPENMLDB_VERSION = std::to_string(OPENMLDB_VERSION_MAJOR) + "." +  // NOLINT
                                     std::to_string(OPENMLDB_VERSION_MINOR) + "." +
                                     std::to_string(OPENMLDB_VERSION_BUG) + "." + OPENMLDB_COMMIT_ID;

std::string ReadConfigYaml(const std::string &yaml_path, std::unordered_map<std::string, std::string> &tablet_map) {
    printf("--------begin ReadConfigYaml--------\n");
    // Reads tablet's endpoints and corresponding paths
    YAML::Node config = YAML::LoadFile(yaml_path);
    for (YAML::const_iterator iter = config["tablet"].begin(); iter != config["tablet"].end(); ++iter) {
        std::string endpoint = (*iter)["endpoint"].as<std::string>();
        if (tablet_map.find(endpoint) != tablet_map.end()) {
            printf("Error. Exists duplicate endpoints.\n");
        } else {
            tablet_map[endpoint] = (*iter)["path"].as<std::string>();
            printf("Tablet's endpoint: %s, path: %s\n", endpoint.c_str(), tablet_map[endpoint].c_str());
        }
    }
    printf("--------end ReadConfigYaml--------\n");
    return config["mode"].as<std::string>();
}

int main(int argc, char* argv[]) {
    ::google::SetVersionString(OPENMLDB_VERSION);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    if (FLAGS_db_name.empty() || FLAGS_table_name.empty()) {
        std::cout << "Error. db_name or table_name not set." << std::endl;
        return -1;
    }
    std::unordered_map<std::string, std::string> tablet_map;
    std::string mode = ReadConfigYaml("./conf.yaml", tablet_map);
    Schema table_schema;
    std::string tmp_path;

    if (mode == "standalone") {
        std::cout << "Data Exporter start in stand-alone mode." << std::endl;
        if (FLAGS_host.empty() || FLAGS_port == 0) {
            std::cout << "Error. host or port not set." << std::endl;
            return -1;
        }
        ::openmldb::tools::StandaloneTablemetaReader standalone_tablemeta_reader =
                ::openmldb::tools::StandaloneTablemetaReader(FLAGS_db_name, FLAGS_table_name,
                                                             tablet_map, FLAGS_host, FLAGS_port);
        standalone_tablemeta_reader.ReadTableMeta();
        table_schema = standalone_tablemeta_reader.getSchema();
        tmp_path = standalone_tablemeta_reader.GetTmpPath().string();
    } else {
        std::cout << "Data Exporter start in cluster mode." << std::endl;
        if (FLAGS_zk_cluster.empty() || FLAGS_zk_root_path.empty()) {
            std::cout << "Error. zk_cluster or zk_path not set." << std::endl;
            return -1;
        }
        ::openmldb::sdk::ClusterOptions cluster_options;
        cluster_options.zk_cluster = FLAGS_zk_cluster;
        cluster_options.zk_path = FLAGS_zk_root_path;
        ::openmldb::tools::ClusterTablemetaReader cluster_tablemeta_reader =
                ::openmldb::tools::ClusterTablemetaReader(FLAGS_db_name, FLAGS_table_name,
                                                          tablet_map, cluster_options);
        cluster_tablemeta_reader.ReadTableMeta();
        table_schema = cluster_tablemeta_reader.getSchema();
        tmp_path = cluster_tablemeta_reader.GetTmpPath().string();
    }
    std::vector<std::string> file_path;
    struct dirent *ptr;
    DIR *dir;
    dir = opendir(tmp_path.c_str());
    while ((ptr = readdir(dir)) != NULL) {
        if (ptr->d_name[0] == '.' || !isdigit(ptr->d_name[0]))
            continue;
        std::string table = tmp_path + "/" + ptr->d_name;
        file_path.emplace_back(table);
    }
    for (auto table : file_path) {
        printf("Opening table path: %s\n", table.c_str());
        ::openmldb::tools::Exporter exporter = ::openmldb::tools::Exporter(table);
        exporter.SetSchema(table_schema);
        exporter.ReadManifest();
        printf("--------start ExportData--------\n");
        exporter.ExportTable();
        printf("--------end ExportData--------\n");
    }
    std::filesystem::path remove_path = tmp_path;
    std::filesystem::remove_all(remove_path);
    return 0;
}
