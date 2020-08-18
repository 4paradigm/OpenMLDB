/*
 * gen_udf_doc.cc
 * Copyright (C) 4paradigm.com 2019 wangtaize <wangtaize@4paradigm.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <fstream>
#include <iostream>
#include <string>
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "yaml-cpp/yaml.h"

#include "udf/default_udf_library.h"
#include "udf/udf_registry.h"

DEFINE_string(output_dir, ".", "Output directory path");
DEFINE_string(output_file, "udf_defs.yaml", "Output yaml filename");

namespace fesql {
namespace cmd {

int ExportUDFInfo(const std::string& dir, const std::string& filename) {
    auto library = udf::DefaultUDFLibrary::get();
    auto& registries = library->GetAllRegistries();
    YAML::Emitter yaml_out;

    yaml_out << YAML::BeginMap;
    for (auto& pair : registries) {
        std::string name = pair.first;
        auto signature_table = pair.second->GetTable();

        yaml_out << YAML::Key << name;
        yaml_out << YAML::Value;
        yaml_out << YAML::BeginSeq;

        for (auto& regitem : signature_table) {
            auto registry = regitem.second.first;
            auto arg_names = regitem.second.second;
            yaml_out << YAML::BeginMap;
            yaml_out << YAML::Key << "arg_types";
            yaml_out << YAML::Value;
            yaml_out << YAML::BeginSeq;
            for (auto n : arg_names) {
                yaml_out << n;
            }
            yaml_out << YAML::EndSeq;
            yaml_out << YAML::Key << "doc";
            yaml_out << YAML::Value << registry->doc();
            yaml_out << YAML::EndMap;
        }

        yaml_out << YAML::EndSeq;
    }
    yaml_out << YAML::EndMap;

    // write c style header for udfs
    LOG(INFO) << "Export udf info to " << dir << "/" << filename;
    std::fstream header_file(dir + "/" + filename, std::fstream::out);
    if (header_file.fail()) {
        LOG(WARNING) << "Create doxygen header file failed";
        return -1;
    }
    header_file << yaml_out.c_str() << "\n";
    header_file.close();
    return 0;
}

}  // namespace cmd
}  // namespace fesql

int main(int argc, char* argv[]) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    return fesql::cmd::ExportUDFInfo(FLAGS_output_dir, FLAGS_output_file);
    return 0;
}
