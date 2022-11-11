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

#include <dirent.h>
#include <gflags/gflags.h>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

#include "base/glog_wrapper.h"
#include "proto/common.pb.h"
#include "sdk/db_sdk.h"
#include "tools/tablemeta_reader.h"
#include "tools/log_exporter.h"
#include "yaml-cpp/yaml.h"

DEFINE_string(db_name, "", "database name");
DEFINE_string(table_name, "", "table name");
DEFINE_string(config_path, "", "the path of the config file");
DECLARE_string(user_name);
DECLARE_string(delimiter);

using Schema = ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnDesc>;

std::string ReadConfigYaml(const std::string& yaml_path, std::unordered_map<std::string, std::string>* tablet_map) {\
    // Reads tablet's endpoints and corresponding paths
    YAML::Node config = YAML::LoadFile(yaml_path);
    for (YAML::const_iterator iter = config["tablet"].begin(); iter != config["tablet"].end(); ++iter) {
        std::string endpoint = (*iter)["endpoint"].as<std::string>();
        if (tablet_map->find(endpoint) != tablet_map->end()) {
            PDLOG(WARNING, "Exists duplicate endpoints %s.", endpoint.c_str());
        } else {
            (*tablet_map)[endpoint] = (*iter)["path"].as<std::string>();
            PDLOG(INFO, "Tablet's endpoint: %s, path: %s.", endpoint.c_str(), (*tablet_map)[endpoint].c_str());
        }
    }
    return config["mode"].as<std::string>();
}

void ReadHostAndPortFromYaml(const std::string& yaml_path, std::string* host, int* port) {
    YAML::Node config = YAML::LoadFile(yaml_path);
    YAML::const_iterator iter = config["nameserver"].begin();
    std::string endpoint = (*iter)["endpoint"].as<std::string>();
    *host = endpoint.substr(0, endpoint.find(":"));
    *port = stoi(endpoint.substr(endpoint.find(":") + 1));
}

void ReadZKFromYaml(const std::string& yaml_path, std::string* zk_cluster, std::string* zk_root_path) {
    YAML::Node config = YAML::LoadFile(yaml_path);
    *zk_cluster = config["zookeeper"]["zk_cluster"].as<std::string>();
    *zk_root_path = config["zookeeper"]["zk_root_path"].as<std::string>();
}

int main(int argc, char* argv[]) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    if (FLAGS_db_name.empty() || FLAGS_table_name.empty()) {
        PDLOG(ERROR, "db_name or table_name not set");
        return -1;
    }
    if (FLAGS_config_path.empty()) {
        PDLOG(ERROR, "config_path not set");
        return -1;
    }
    std::unordered_map<std::string, std::string> tablet_map;
    std::string mode = ReadConfigYaml(FLAGS_config_path, &tablet_map);
    Schema table_schema;
    std::string tmp_path;
    ::openmldb::tools::TablemetaReader *tablemeta_reader;
    if (mode == "standalone") {
        std::string host;
        int port;
        ReadHostAndPortFromYaml(FLAGS_config_path, &host, &port);
        tablemeta_reader = new ::openmldb::tools::StandaloneTablemetaReader(FLAGS_db_name, FLAGS_table_name,
                                                                tablet_map, host, port);
    } else {
        std::string zk_cluster, zk_root_path;
        ReadZKFromYaml(FLAGS_config_path, &zk_cluster, &zk_root_path);
        ::openmldb::sdk::ClusterOptions cluster_options;
        cluster_options.zk_cluster = zk_cluster;
        cluster_options.zk_path = zk_root_path;
        tablemeta_reader = new ::openmldb::tools::ClusterTablemetaReader(FLAGS_db_name, FLAGS_table_name,
                                                            tablet_map, cluster_options);
    }

    tablemeta_reader->SetTableinfoPtr();
    if (tablemeta_reader->GetTableinfoPtr() == nullptr) {
        return -1;
    }
    if (tablemeta_reader->ReadTableMeta() == false) {
        return -1;
    }
    table_schema = tablemeta_reader->GetSchema();
    tmp_path = tablemeta_reader->GetTmpPath().string();
    delete tablemeta_reader;

    std::unordered_map<int, std::vector<std::string>> filepath_map;
    struct dirent *ptr;
    DIR *dir;
    dir = opendir(tmp_path.c_str());

    while ((ptr = readdir(dir)) != NULL) {
        if (ptr->d_name[0] == '.' || !isdigit(ptr->d_name[0]))
            continue;
        std::string table_name = ptr->d_name;
        std::string table_path = tmp_path + "/" + table_name;

        int table_num = std::stoi(table_name.substr(0, table_name.find("_")));
        if  (filepath_map.find(table_num) == filepath_map.end()) {
            filepath_map[table_num] = std::vector<std::string>();
        }
        filepath_map[table_num].emplace_back(table_path);
    }
    for (const auto& filepath_pair : filepath_map) {
        PDLOG(INFO, "Starting export table %d.", filepath_pair.first);
        std::ofstream table_cout(std::to_string(filepath_pair.first) + "_result.csv");
        for (int i = 0; i < table_schema.size(); ++i) {
            table_cout << table_schema.Get(i).name();
            if (i < table_schema.size() - 1) {
                table_cout << FLAGS_delimiter;
            } else {
                table_cout << "\n";
            }
        }
        for (const auto& table : filepath_pair.second) {
            ::openmldb::tools::LogExporter exporter = ::openmldb::tools::LogExporter(table, table_cout);
            exporter.SetSchema(table_schema);
            exporter.ReadManifest();
            exporter.ExportTable();
        }
    }
    std::filesystem::path remove_path = tmp_path;
    std::filesystem::remove_all(remove_path);
    return 0;
}
