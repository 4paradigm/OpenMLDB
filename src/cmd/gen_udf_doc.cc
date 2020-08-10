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

#include "udf/default_udf_library.h"
#include "udf/udf_registry.h"

DEFINE_string(output_dir, ".", "Output directory path");

namespace fesql {
namespace cmd {

int OutputuUDFDoc(const std::string& dir) {
    udf::DefaultUDFLibrary library;
    auto& registries = library.GetAllRegistries();

    // write c style header for udfs
    std::fstream header_file(dir + "/udf_header.h", std::fstream::out);
    if (header_file.fail()) {
        LOG(WARNING) << "Create doxygen header file failed";
        return -1;
    }
    for (auto& pair : registries) {
        std::string name = pair.first;
        auto& comp = pair.second;
        for (auto& reg_item : comp->GetSubRegistries()) {
            header_file << "/*!\n";
            header_file << reg_item->doc();
            header_file << "\n*/\n";
            header_file << "void " << name << "();\n\n";
        }
    }
    header_file.close();
    return 0;
}

}  // namespace cmd
}  // namespace fesql

int main(int argc, char* argv[]) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    return fesql::cmd::OutputuUDFDoc(FLAGS_output_dir);
    return 0;
}
